# Environment setup — scale-lib demo

> Same discipline as ``learn/ENV_SETUP.md``: per-demo ``DAGSTER_HOME``,
> verify each step, refuse-on-empty.

## Step 1 — DAGSTER_HOME

**tcsh** (default):

```tcsh
setenv DAGSTER_HOME ~/.dagster-tutor/demo-scale-lib
mkdir -p $DAGSTER_HOME
cp /home/user/Dagster-expert/personalities/dagster-expert/demo/scale-lib/dagster.yaml \
   $DAGSTER_HOME/dagster.yaml
```

**bash**:

```bash
export DAGSTER_HOME=~/.dagster-tutor/demo-scale-lib
mkdir -p "$DAGSTER_HOME"
cp /home/user/Dagster-expert/personalities/dagster-expert/demo/scale-lib/dagster.yaml \
   "$DAGSTER_HOME/dagster.yaml"
```

Verify:

```
echo $DAGSTER_HOME    # must end in demo-scale-lib
```

## Step 2 — venv

```tcsh
source ~/dagster-venv/bin/activate.csh
```
```bash
source ~/dagster-venv/bin/activate
```

Verify:

```
which dagster
dagster --version    # 1.13.3
```

## Step 3 — launch

```
dagster dev -w /home/user/Dagster-expert/personalities/dagster-expert/demo/scale-lib/workspace.yaml
```

Open ``http://localhost:3000``. Asset Catalog should show:

* ``cell_list``, ``pvt_manifest`` (sources)
* ``lib_a/step0``, ``lib_a/step1``, …, ``lib_a/meta`` — 21 step assets

## Step 4 — verify

CLI smoke:

```
python3 -m _smoke
```

UI smoke: open Asset Catalog → ``lib_a`` group → click ``step0`` →
Partitions tab → 1 partition (``corner``) → Materialize. Then
``step3`` → 46 partitions; pick ``em`` → Materialize (will block on
``step2`` first; expected).

## Step 5 — cleanup

```tcsh
unsetenv DAGSTER_HOME
```
```bash
unset DAGSTER_HOME
```

Optional, removes output trees:

```
rm -rf /tmp/dagster-scale-lib $DAGSTER_HOME
```
