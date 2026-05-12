# scale-lib — Tier-1 Dagster demo

A production-shaped demo of the Dagster 1.13.3 patterns from lessons
01–12, scaled to 46 variant-tree branches × 21 logical step types per
library. The PVT and cell fan-out lives in Tier-2 (the step scripts);
this demo deliberately does **not** model them as Dagster partitions.

> See ``CONTRACT.md`` for the Tier-1 / Tier-2 boundary.
> See ``ENV_SETUP.md`` for the per-demo ``DAGSTER_HOME`` setup.

## Why this demo exists

Lesson 12 demonstrated SQLite cardinality limits at modest scale. This
demo answers the followup: *what does the same flow look like at
production scale, without losing UI navigability or running into
partition-store ceilings?*

Answer (cardinality_calc.py prints this):

```
DEMO        1 lib × 46 branches × 21 steps  ≈ 1.1k partition records
PRODUCTION  1 lib × 64 branches × 21 steps  ≈ 1.5k partition records
FUTURE 10×  1 lib × 460 branches × 21 steps ≈ 10.6k partition records
```

All three fit SQLite comfortably (1.13.3 default). Postgres is not
required at this scale; the Postgres stanza in ``dagster.yaml`` is
commented for future use.

## Branch model (variant tree)

46 branches in a tree rooted at ``corner``. The graph-theory parent of
each branch is encoded in ``config/branches.yaml``. The ``parent``
relationship drives the *parent-mirror* dep rule — non-root branches
read the upstream of the previous step in the chain from their immediate
variant-tree parent (in addition to themselves).

```
corner ─┬─ em
        ├─ ht
        ├─ lvf ── lvf_ht
        ├─ mpwda ─┬─ mpwda_aged ── mpwda_aged_lvf
        │         └─ mpwda_lvf
        └─ tmsf_self ─┬─ tmsf_self_ht
                      ├─ tmsf_self_lvf ── tmsf_self_lvf_ht
                      ├─ tmsf_lde1 ── tmsf_lde1_ht
                      ├─ tmsf_lde2 ── tmsf_lde2_ht
                      ├─ ... (lde3..23, lde3_ht..lde10_ht)
                      └─ tmsf_lde23
```

The ``ParentMirrorRule`` controls **which** char steps trigger
cross-branch merging. Default: ``frozenset({"step5"})``. Override in
``pipelines/registry.py``.

## Step model

| Kind       | Steps                                          | Partition shape | Runner       |
|------------|------------------------------------------------|------------------|--------------|
| setup      | step0, auto_download                           | root branch only | perl         |
| extraction | phantom, BEpreQ, step1, step7                  | all branches     | perl         |
| char       | step2, step3, step4, step5 (Python Pipes)<br>step6, FunKits (Perl) | all branches | python / perl |
| kits       | rln, trf, cdk, pgv, apl, spm, mpwda_kit, mtbf, meta | root branch only | perl |

step7 fires after step1 (same-branch dep). All non-setup steps depend
on the root branch's step0 (setup gate). Kits (except ``rln``) depend
on the root branch's step6.

## Architecture (four layers)

| Layer | Contents                                       | Imports Dagster? |
|-------|------------------------------------------------|------------------|
| 0     | ``pipelines/spec/`` — pure data, Protocols     | **no** |
| 1     | ``pipelines/rules/`` — dep rules, one per file | **no** |
| 2     | ``pipelines/registry.py`` — single rule list   | **no** |
| 3     | ``pipelines/translator.py`` — to ``PartitionMapping`` | yes |
| 4     | ``pipelines/factory.py`` + ``definitions.py``  | yes |

``tests/test_layer_imports.py`` enforces the no-Dagster rule with a
regex grep over Layer 0/1/2 source.

## Where dep facts live

**One file**: ``pipelines/registry.py``.

```python
DEPS = DepRegistry(rules=(
    StepChainRule(),
    ParentMirrorRule(applies_to=frozenset({"step5"})),
    SetupGateRule(),
    Step7FollowRule(),
    KitStep6Rule(rln_exempt=True),
    KitRlnRule(),
))
```

To change dep behavior:
1. Edit the rule's parameters in ``registry.py``, OR
2. Add a new file under ``pipelines/rules/X.py``, register it.

The factory and asset bodies do not change.

## Run

```bash
# from this folder
export DAGSTER_HOME=~/.dagster-tutor/demo-scale-lib    # bash; tcsh: setenv …
mkdir -p "$DAGSTER_HOME"
cp dagster.yaml "$DAGSTER_HOME/dagster.yaml"

# unit + integration tests (fast)
python3 -m pytest -q

# end-to-end smoke (one family, 4 steps; ~10 s)
python3 _smoke.py

# launch UI
dagster dev -w workspace.yaml
```

## UI verification checklist

Open ``http://localhost:3000`` and confirm:

- **Asset Catalog** lists 23 assets (21 ``lib_a/*`` + 2 sources).
- **Asset graph** shows ``lib_a/step3`` with upstream edges from
  ``lib_a/step2`` and ``lib_a/step0``.
- **Partitions tab** on ``lib_a/step5`` shows 46 partition keys.
- **Partitions tab** on ``lib_a/step0`` shows 1 key (``corner``).
- Materializing ``lib_a/step5`` for partition ``tmsf_lde1`` resolves
  upstream to ``lib_a/step4`` partitions ``{tmsf_lde1, tmsf_self}``.

A scripted equivalent is in ``tests/integration/test_definitions_load.py``.

## File map

```
demo/scale-lib/
├── README.md                 this file
├── CONTRACT.md               Tier-1 / Tier-2 boundary
├── ENV_SETUP.md              per-demo DAGSTER_HOME
├── workspace.yaml            -> pipelines module
├── dagster.yaml              SQLite default; PG stanza commented
├── cardinality_calc.py       prints scenarios
├── _smoke.py                 e2e harness
├── pipelines/
│   ├── __init__.py           exports defs
│   ├── definitions.py        Dagster Definitions
│   ├── factory.py            @asset builder
│   ├── translator.py         PartitionRule -> PartitionMapping
│   ├── registry.py           single source of truth: DEPS
│   ├── partitions.py         branch / root_branch partitions defs
│   ├── runners.py            PerlRunner, PythonRunner resources
│   ├── source_observers.py   @observable_source_asset for sources
│   ├── folder_digest.py      data_version contract
│   ├── spec/                 Layer 0 (pure data)
│   │   ├── branch_hierarchy.py
│   │   ├── dep_edge.py
│   │   ├── dep_rule.py
│   │   ├── partition_rule.py
│   │   └── step_taxonomy.py
│   └── rules/                Layer 1 (concrete rules)
│       ├── step_chain.py
│       ├── parent_mirror.py
│       ├── setup_gate.py
│       ├── step7_follow.py
│       ├── kit_step6.py
│       ├── kit_rln.py
│       └── cross_library.py
├── config/
│   ├── branches.yaml         variant tree (46 entries)
│   ├── library_meta.yaml
│   ├── cells.json
│   └── pvt_manifest.yaml     observed source
├── scripts/
│   ├── perl/                 mock perl steps (17 symlinks → _template.pl)
│   └── python/               mock python steps (4 symlinks → _template.py)
└── tests/                    pytest suite (81 tests)
    ├── spec/                 layer-0 unit tests
    ├── rules/                layer-1 unit tests
    ├── test_registry.py      layer-2 integration
    ├── test_translator.py    layer-3 integration
    ├── test_layer_imports.py layer-boundary enforcement
    └── integration/
        └── test_definitions_load.py  Dagster Definitions integration
```

## Next steps (deferred)

* Multi-library scale-out via second code location (lesson 11 pattern).
* AP compatibility shim (``.ap_done`` touch / sensor).
* Tier-2 Dagster nested inside step scripts (Phase 2; only if leaf-level
  memoization pain emerges).
* LSF swap on the runner resource (one-line change; see ``CONTRACT.md``).
