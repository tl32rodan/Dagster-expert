# Tier-1 / Tier-2 contract

The Dagster (Tier-1) layer **never** reads the contents of step output
files. It only:

1. invokes the step runner (perl / python script),
2. computes a folder digest after the script returns,
3. reports the digest as the asset's ``data_version``.

Any leaf-level structure — per-PVT, per-cell, per-block — lives entirely
in the step script (Tier-2). The contract below is what every step
script must honor so the digest is meaningful.

## Step script invocation

The runner calls the script with:

```
<runner_bin> <demo>/scripts/<runner_dir>/<step>.<ext> \
    --library  <library_name> \
    --branch   <branch_name> \
    --step     <step_name> \
    --out      <abs_path_to_output_folder>
```

* ``runner_bin``    — ``perl`` or ``python3`` (configurable on the
                       resource).
* ``runner_dir``    — ``perl`` or ``python``.
* ``ext``           — ``pl`` or ``py``.
* ``--out``         — directory; the runner creates it before invocation.

## Step script contract

The script MUST:

* Exit 0 on success, non-zero on failure.
* Write only into ``--out`` and its subdirectories.
* Make the output deterministic given identical inputs (same digest on
  retry).
* Treat ``stdout``/``stderr`` as logs; Dagster captures them.

The script MAY:

* Drop a ``.dagster_meta.json`` of its own. The runner overwrites it
  with the folder digest before returning, so do not rely on its
  contents.
* Fan out internally (per-PVT loop, per-cell loop, LSF array submit).

## Folder digest

Computed by ``pipelines/folder_digest.py``.

* Default (manifest-only): ``sha256( (rel_path, size, int(mtime)) ... )``.
  O(n) on stat(), no file reads.
* Optional (content): ``digest_folder_contents`` — adds bytes hash.

The digest is written into ``<out>/.dagster_meta.json``:

```json
{
  "data_version": "abc123...",
  "file_count": 2,
  "total_bytes": 4096,
  "latest_mtime": 1715512345
}
```

Tier 1's asset body reads this file to produce ``MaterializeResult``.

### Who computes the digest

| Runner mode | Who computes | Why |
|---|---|---|
| **Local subprocess** (default in scale-lib) | Dagster asset body, post-run | Local FS stat is fast; no NFS round-trip; simpler. |
| **LSF / remote** | **Node-side wrapper, before exit** | NFS stat over 50k+ files is 30s–2min per materialization. The LSF wrapper writes ``<out>/.dagster_meta.json`` as its last action; Dagster only reads that JSON. ~1s end-to-end. |

The contract is the same in both cases — the file at
``<out>/.dagster_meta.json`` is the authoritative version. The
runner mode just changes who computed it.

For the LSF path, ``pipelines/runners.py`` is the swap point; the
wrapper script (e.g. ``learn/13-lsf-integration/scripts/python/lsf_submit.py``)
should call ``digest_folder_manifest()`` itself on the compute
node before bsub exits. Dagster's asset body becomes:

```python
@asset(...)
def my_asset(context) -> MaterializeResult:
    out_dir = ...
    subprocess.run(["bsub", "-K", "-J", ..., "lsf_submit.py", ...], check=True)
    meta = json.loads((out_dir / ".dagster_meta.json").read_text())
    return MaterializeResult(
        data_version=DataVersion(meta["data_version"]),
        metadata={k: v for k, v in meta.items() if k != "data_version"},
    )
```

Trust boundary: this requires Dagster to trust the LSF node
didn't lie about the digest. Same trust as trusting the node ran
the script at all. Acceptable.

## Source change events

Two source assets observe input files and propagate staleness:

* ``pvt_manifest``  — hashes ``config/pvt_manifest.yaml``.
* ``cell_list``     — hashes ``config/cells.json``.

These are NOT current upstreams of any step asset in the demo (steps
are pure functions of branch + script binary). Wire them by adding a
DepRule that emits an edge to one of these source AssetKeys; the
factory will then plumb the source into the asset's ``deps=``.

## AP compatibility shim (optional)

Two directions, both opt-in:

* **Tier 1 → AP**: after a step succeeds, ``touch`` the AP marker file
  (``<out>/.ap_done``). Add a one-line script hook in
  ``scripts/<runner>/<step>``.
* **AP → Tier 1**: register a sensor that watches AP's
  ``${AP_ROOT}/<lib>/<branch>/<step>/.ap_done`` and emits a
  ``RunRequest``. Not implemented in the demo; see ``lessons/15-sensors``.

## LSF swap point

The runner currently invokes ``subprocess.run([perl|python3, script,
...])``. To swap to LSF:

```python
cmd = ["bsub", "-K", "-J", f"{step}-{branch}",
       "-o", f"{out}/lsf.out", "-e", f"{out}/lsf.err",
       perl_bin, str(script), ...]
subprocess.run(cmd, check=True)
```

``bsub -K`` blocks until the job completes; the rest of the pipeline
stays identical. The script then has the LSF-array sub-fan-out (per-PVT,
per-cell). No Tier-1 change.
