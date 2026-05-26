# flow-src — the characterization flow BEFORE conversion (reference)

A self-contained, **stdlib-only** mock of a Cadence Liberate
characterization flow. Paths are **hardcoded** on purpose — this is the
"before" state that the Dagster conversion refactors. Toy scale: 3 PVT
(`tt_25 / ff_125 / ss_m40`) × 3 cell (`INV / BUF / NAND2`), sections 2–7.

> 繁中：這是「轉換前」的參考流程，路徑寫死、能獨立跑完。Dagster 版會把它重構成
> config+generator，並證明「產物只有路徑變、其餘 byte 一致」。

## Inputs (mirrors the real flow)

| File(s) | Varies by | Notes |
|---|---|---|
| `templates/template_<pvt>.tcl` | PVT | char template params |
| `sections/<pvt>/section{2..7}.tcl` | PVT | 6 section tcls per PVT (the "PVT SECTION TCL") |
| `modelcard/model_<pvt>.tcl` | PVT | model card |
| `netlist/<cell>.sp` | cell | SPICE subckt |
| `Mnpvt_cell_list.tcl` | — (single) | lists all cells; `set_cell \( … \)` |
| `main.tcl` | — (single) | sources the per-PVT templates (path-bearing) |
| `run.scr` | — (single) | reads everything, sources `tool_env.csh`, runs `liberate` (path-bearing) |
| `bin/liberate` | — | the **mock** tool (stdlib) |

The param files (templates / sections / model cards / netlist / cell
list) are **path-free**; only `main.tcl` and `run.scr` embed absolute
paths. That is the whole point: after conversion only those embedded
paths change.

## Run it

```bash
./run_all.sh
# stages to /tmp/liberate-char-ref, runs liberate, writes 9 .lib + 9 .ldb to out/
```

Each `out/<pvt>__<cell>.lib` has a header (source_scr / inputs — the
path-bearing lines) and a body (`char_digest` / `delay_ps`) derived only
from input *content*. The `.ldb` holds just the content digest.
