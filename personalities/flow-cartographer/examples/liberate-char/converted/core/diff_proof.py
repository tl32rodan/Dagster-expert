"""The correctness contract: prove the Dagster outputs differ from the
reference outputs ONLY in embedded paths.

`liberate` writes two path-bearing header lines into each .lib
(`/* source_scr: ... */` and `/* inputs: ... */`). Everything else — the
library body (char_digest / delay_ps) and the whole .ldb — derives from
input *content*, so it must be byte-identical between the reference run
and the Dagster run. We drop the two header lines, then compare.
"""
from pathlib import Path

_PATH_HEADER_PREFIXES = ("/* source_scr:", "/* inputs:")


def _normalize_lib(text: str) -> str:
    return "\n".join(
        line for line in text.splitlines()
        if not line.startswith(_PATH_HEADER_PREFIXES)
    )


def outputs_equivalent(ref_dir, dag_dir):
    """Return (ok: bool, diffs: list[str]). `ok` means every .lib body and
    every .ldb in ref_dir matches dag_dir, ignoring path-bearing headers."""
    ref, dag = Path(ref_dir), Path(dag_dir)
    diffs = []

    ref_libs = sorted(p.name for p in ref.glob("*.lib"))
    dag_libs = sorted(p.name for p in dag.glob("*.lib"))
    if ref_libs != dag_libs:
        diffs.append(f".lib set differs: {set(ref_libs) ^ set(dag_libs)}")
    for name in ref_libs:
        if name not in dag_libs:
            continue
        if _normalize_lib((ref / name).read_text()) != _normalize_lib((dag / name).read_text()):
            diffs.append(f"{name}: body differs (more than just paths)")

    ref_ldbs = sorted(p.name for p in ref.glob("*.ldb"))
    dag_ldbs = sorted(p.name for p in dag.glob("*.ldb"))
    if ref_ldbs != dag_ldbs:
        diffs.append(f".ldb set differs: {set(ref_ldbs) ^ set(dag_ldbs)}")
    for name in ref_ldbs:
        if name not in dag_ldbs:
            continue
        if (ref / name).read_text() != (dag / name).read_text():
            diffs.append(f"{name}: ldb differs (should be path-free)")

    return (len(diffs) == 0, diffs)
