# char_dagster

Mock Liberate-style characterization flow on **Dagster 1.13.3**. Companion
reference for the air-gap conversion in `../liberate-char/converted/`. Built
from templates + a single YAML config rather than diff-proofing against a
pre-existing source tree.

## Shape

| Asset (group) | Partition | Sources under `templates/` |
|---|---|---|
| `add_to_liberate_tcl` (sources) | none | `add_to_liberate.tcl.j2` |
| `bolt_tcl` (sources) | none | `Bolt.tcl.j2` |
| `mnpvt_cell_list_tcl` (sources) | trio_group | `.MnPVT_cell_list/_cell_list.tcl.j2` |
| `model_card_files` (sources) | trio_group × pvt | `Model_card/_card.tcl.j2` |
| `netlist_files` (sources) | trio_group × cell | `Netlist/_cell.spi.j2` |
| `pvt_section_files` (sources) | trio_group × pvt | `Template/_template.tcl.j2` + `.Trio_pvt_setting/SECTION/SECTION_{2..7}/_section.tcl.j2` (folder-as-asset, 7 files / partition) |
| `main_char_script` (sources) | none | `main.tcl.j2` |
| `characterization_run` (execution) | trio_group × pvt | `run.scr.j2` + bsub → bin/liberate |
| `validation_check` (execution) | trio_group × pvt | — |

2D MultiPartitions: `trio_group × pvt` and `trio_group × cell`. Cross-dim
mapping (netlist → section) uses a pre-computed `StaticPartitionMapping`
(see `char_dagster/spec/mappings.py`).

Sensor `cell_drop_sensor` watches `<work_dir>/_drop/<trio_group>/*.spi`
and requests the leaf `netlist_files` partition only;
`AutomationCondition.eager()` on every downstream asset cascades the
rest. Real LSF parallelism comes from `QueuedRunCoordinator` +
`dagster/concurrency_key` tag, not custom multi-thread launchers.

## Bring-up (tcsh)

```tcsh
# 1. point DAGSTER_HOME at a project-local dir (export form in parens)
setenv DAGSTER_HOME /work/char/.dagster                 # (export DAGSTER_HOME=/work/char/.dagster)
mkdir -p $DAGSTER_HOME

# 2. sanity-check toolchain
which dagster
dagster --version                                       # expect 1.13.3
python -c "import jinja2; print(jinja2.__version__)"

# 3. unit + smoke
cd /path/to/char_dagster
pytest tests/test_utils.py tests/test_config.py tests/test_paths.py tests/test_partitions.py -v
python tests/_smoke.py

# 4. dev UI (absolute -w; never `cd`-chain into it)
dagster dev -w /path/to/char_dagster/workspace.yaml
```

## Drop-sensor smoke

```tcsh
mkdir -p /work/char/_drop/LPE_ssgnp_cworst_T_25c
cp /path/to/some/BUF.spi /work/char/_drop/LPE_ssgnp_cworst_T_25c/
# the sensor evaluates every 30s and requests netlist_files for
# (LPE_ssgnp_cworst_T_25c, BUF). Eager auto-cond pulls everything
# downstream automatically.
```

## What's mocked vs. real

| Real (production swap) | Mock (this repo) |
|---|---|
| `liberate` EDA tool | `bin/liberate` (stdlib only; emits deterministic `.lib` / `.ldb` via SHA-256 of input file contents) |
| `bsub` LSF submit | `bin/bsub` (`-K` synchronous; forwards env so dagster-pipes vars pass through) |
| `/tools/obf/cshrc`, `/tools/liberate/version.csh` | strings only (referenced by add_to_liberate.tcl + run.scr; never sourced by the mock) |

The mock `bin/liberate` derives the .lib body content from the *content*
of the rendered Template / SECTION / Model_card / Netlist files for that
(trio_group, pvt); only the .lib header line records absolute paths.
Same content → byte-identical bodies regardless of where on disk they live.

## Layout

```
char_dagster/
├── char_dagster/                  # importable package
│   ├── __init__.py
│   ├── utils.py                   # substitute_template (Jinja2, StrictUndefined)
│   ├── config.py                  # @dataclass CharConfig + load_config()
│   ├── paths.py                   # derive_lpe_rc + path helpers
│   ├── partitions.py              # module-level singletons
│   ├── sensor.py                  # cell_drop_sensor
│   ├── definitions.py             # Definitions(assets, sensors, jobs, resources)
│   ├── lsf_inner.py               # pipes-aware bsub wrapper
│   ├── spec/
│   │   └── mappings.py            # NETLIST_TO_SECTION + reserved patterns
│   └── assets/
│       ├── source_generation.py   # 7 source assets
│       └── execution.py           # characterization_run + validation_check
├── config/char_config.yaml        # single source of truth
├── templates/                     # 14 Jinja .j2 source templates
├── bin/
│   ├── bsub                       # mock LSF (executable Python)
│   └── liberate                   # mock tool (executable Python)
├── tests/
│   ├── test_utils.py              # scalar / list / nested / StrictUndefined
│   ├── test_config.py             # validation rules (one seed, regex, …)
│   ├── test_paths.py              # derive_lpe_rc + path constructors
│   ├── test_partitions.py         # 2D singletons + mapping shape (needs Dagster)
│   └── _smoke.py                  # end-to-end render+liberate (no Dagster)
├── workspace.yaml
├── dagster.yaml
└── README.md
```

## Reference points used while building

- `../liberate-char/converted/pipelines/` — sensor pattern, deps shape,
  AutomationCondition usage (`assets.py:97`)
- `../liberate-char/converted/core/{bsub,liberate,lsf_submit.py}` —
  mock-tool conventions
- `personalities/dagster-expert/database/dagster-1.13.3/docs/STANDARD_USAGE.md`
  §3.2 (partition mappings), §8 (LSF), §9 (folder-as-asset, no custom launchers)
- `personalities/dagster-expert/memory/understanding/dagster-1.13.3-gotchas.md`
  Gotcha #4 (never subclass PartitionMapping)
