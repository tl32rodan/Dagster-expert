"""Tier-1 / Tier-2 data-version contract — folder hashing.

Each step writes a folder. The folder's ``data_version`` is the
sha256 hash of a deterministic file manifest (relative path + size +
mtime). This intentionally does NOT read file content — keeps the
hash O(n) on stat() for very large folders.

Use ``digest_folder_contents`` for content-sensitive cases (e.g. small
config files); use ``digest_folder_manifest`` for big leaf trees.

Contract details: see ``CONTRACT.md``.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class FolderSummary:
    data_version: str
    file_count: int
    total_bytes: int
    latest_mtime: int     # epoch seconds (int, deterministic)
    manifest: list[tuple[str, int, int]]   # (rel_path, size, mtime_int)


def _iter_files(root: Path):
    for f in sorted(root.rglob("*")):
        if f.is_file():
            yield f


def digest_folder_manifest(root: Path) -> FolderSummary:
    """Hash by (rel_path, size, mtime). Fast; OK when content changes
    always coincide with stat changes (typical for step outputs).
    """
    h = hashlib.sha256()
    manifest: list[tuple[str, int, int]] = []
    total_bytes = 0
    latest_mtime = 0

    for f in _iter_files(root):
        rel = f.relative_to(root).as_posix()
        size = f.stat().st_size
        mtime = int(f.stat().st_mtime)
        total_bytes += size
        latest_mtime = max(latest_mtime, mtime)
        manifest.append((rel, size, mtime))
        h.update(rel.encode())
        h.update(str(size).encode())
        h.update(str(mtime).encode())
        h.update(b"\x00")

    return FolderSummary(
        data_version=h.hexdigest()[:32],
        file_count=len(manifest),
        total_bytes=total_bytes,
        latest_mtime=latest_mtime,
        manifest=manifest,
    )


def digest_folder_contents(root: Path) -> FolderSummary:
    """Like ``digest_folder_manifest`` but reads file bytes too. Use for
    small / config-like folders where stat changes might not catch all
    edits.
    """
    h = hashlib.sha256()
    manifest: list[tuple[str, int, int]] = []
    total_bytes = 0
    latest_mtime = 0

    for f in _iter_files(root):
        rel = f.relative_to(root).as_posix()
        size = f.stat().st_size
        mtime = int(f.stat().st_mtime)
        total_bytes += size
        latest_mtime = max(latest_mtime, mtime)
        manifest.append((rel, size, mtime))
        h.update(rel.encode())
        h.update(b"\x00")
        h.update(f.read_bytes())
        h.update(b"\x00")

    return FolderSummary(
        data_version=h.hexdigest()[:32],
        file_count=len(manifest),
        total_bytes=total_bytes,
        latest_mtime=latest_mtime,
        manifest=manifest,
    )


def write_meta(folder: Path, summary: FolderSummary) -> Path:
    """Write the ``.dagster_meta.json`` file the contract requires.

    Tier 1's asset body reads this back to fill MaterializeResult metadata
    and data_version.
    """
    meta_path = folder / ".dagster_meta.json"
    meta_path.write_text(
        json.dumps(
            {
                "data_version": summary.data_version,
                "file_count": summary.file_count,
                "total_bytes": summary.total_bytes,
                "latest_mtime": summary.latest_mtime,
            },
            indent=2,
        )
    )
    return meta_path


def read_meta(folder: Path) -> dict:
    return json.loads((folder / ".dagster_meta.json").read_text())
