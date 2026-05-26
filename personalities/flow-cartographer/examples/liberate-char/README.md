# liberate-char — a worked flow→Dagster conversion (reference for the agent to mimic)

A complete, **runnable, air-gapped** example a less-capable agent can copy
instead of designing from scratch. It shows, end to end:

1. **`flow-src/`** — the characterization flow *before* conversion: real-shaped
   files & folders, **hardcoded paths**, runs standalone, produces `.lib`/`.ldb`.
2. **`converted/`** — the same flow *after* conversion to Dagster 1.13.3:
   sources generated from one config, a 2D `pvt × cell` asset graph, bsub-via-Pipes
   execution, a sensor + auto-materialize — with a **proof the products differ
   only in embedded paths**.
3. **`DO_AND_DONT.md`** — the operating rules the agent must follow (so it stops
   improvising). **`CONVERSION.md`** — the filled charter.

> 繁中：這是一份「轉換前 → 轉換後」的完整可跑範例，給內部那個比較弱的 agent 照著臨摹。
> 重點：來源全部由 config 產生（不複製、不讀原路徑）、pvt×cell 雙軸 partition、bsub 走 Pipes、
> 並且用測試證明「產物只有路徑變」。設計守則在 `DO_AND_DONT.md`。

## The shape

Inputs vary along two axes — **PVT** (`tt_25 / ff_125 / ss_m40`) and **cell**
(`INV / BUF / NAND2`), sections 2–7. One `run.scr` ties it together and runs
`liberate` → 9 `.lib` + 9 `.ldb`.

```
flow-src/  (hardcoded paths)              converted/  (config-driven Dagster)
  templates/template_<pvt>.tcl      ──▶     config/liberate.yaml  (one source of truth)
  sections/<pvt>/section{2..7}.tcl  ──▶     pipelines/generators.py  (reproduce each file)
  modelcard/model_<pvt>.tcl         ──▶     pipelines/spec/partitions.py  (pvt, cell, pvt×cell singletons)
  netlist/<cell>.sp                 ──▶     pipelines/deps.py  (cross-dim MultiToSingleDimension mapping)
  Mnpvt_cell_list.tcl, main.tcl     ──▶     pipelines/assets.py  (generators + 2D `characterize`)
  run.scr → liberate → out/*.lib    ──▶     bsub via PipesSubprocessClient → out/*.lib
```

The param files (templates/sections/modelcards/netlists/cell-list) are
path-free, so the generator reproduces them **byte-for-byte**. Only `main.tcl`
and `run.scr` embed paths — so after conversion **only those paths change**, and
`core/diff_proof.py` proves it.

## Run it

```bash
# 1) the BEFORE flow (stdlib only):
cd flow-src && ./run_all.sh                 # → /tmp/liberate-char-ref/out/*.lib,*.ldb

# 2) the converted project's tests (stdlib unittest — generator reproduces flow-src):
cd ../converted && python -m unittest discover -s tests -v

# 3) end-to-end on Dagster 1.13.3 (needs `pip install dagster==1.13.3`):
LIBERATE_DAG_ROOT=/tmp/liberate-char-dag python -m _smoke
#    → materializes 9 generators + 9 characterize leaves (bsub+Pipes),
#      then asserts the products equal flow-src's, ignoring only path lines.

# one leaf (fine-grain rerun), the golden CLI path:
DAGSTER_HOME=<dir-with-dagster.yaml> \
  dagster asset materialize -m pipelines.definitions --select characterize --partition 'INV|tt_25'
```

## What it demonstrates (and what was verified here)

- **Extract, don't copy**: 27+ hardcoded source files → 1 `config/liberate.yaml`
  + generators; `tests/test_generator_equivalence.py` proves byte-for-byte reproduction. ✅
- **2D partitioning** `pvt × cell` with **cross-dimension** `MultiToSingleDimensionPartitionMapping`. ✅
- **bsub via Pipes** (`PipesSubprocessClient` → `lsf_submit` → mock `bsub` → pipes-aware
  inner → mock `liberate`); **no custom RunLauncher**. ✅
- **`AutomationCondition.eager()`** (the current 1.13.3 API — `AutoMaterializePolicy`
  is deprecated here) + a **sensor** for new-netlist drops. ✅
- **Products differ only in paths**: `_smoke.py` diff-proof **PASS**. ✅

Everything was run on `dagster==1.13.3`: `Definitions` loads, the smoke
materializes all leaves, the diff-proof passes, and a single-partition CLI
rerun works. The `liberate`/`bsub` tools are mocks (stdlib) so it runs with no
real EDA tool and no network.

See **`DO_AND_DONT.md`** for the rules and **`CONVERSION.md`** for the charter.
