<!-- all-might generated -->
# Lesson environment setup (read FIRST, every lesson)

**Read this file before the first command of every lesson.** It exists
because less-capable agents forget to set `DAGSTER_HOME`, then `dagster
dev` runs but writes to the wrong place — the lesson then looks broken in
ways that aren't documented anywhere.

## Step 1 — Set DAGSTER_HOME (per-lesson sandbox)

**Each lesson gets its OWN `DAGSTER_HOME`** so the run history, event
log, and storage from one lesson never leak into another. Use the
pattern `~/.dagster-tutor/<NN-topic>`, where `<NN-topic>` is the lesson
folder name (e.g. `01-asset-and-materialize`).

**Agent: remind the user every time they start a new lesson to update
this path.** If you're switching from lesson 01 to lesson 02, re-run
Step 1 with the new suffix. Do NOT reuse lesson 01's DAGSTER_HOME for
lesson 02 — different lessons may register the same code-location name
and clash.

**tcsh (default — the user's shell):**
```tcsh
setenv DAGSTER_HOME ~/.dagster-tutor/01-asset-and-materialize
mkdir -p $DAGSTER_HOME
```

**bash equivalent:**
```bash
export DAGSTER_HOME=~/.dagster-tutor/01-asset-and-materialize
mkdir -p $DAGSTER_HOME
```

**Verify (BOTH shells):**
```
echo $DAGSTER_HOME
```
Expected output: an absolute path ending in `.dagster-tutor/<NN-topic>`,
matching the lesson you're about to run. If empty, STOP — re-run the
setenv/export. If it points at a different lesson's directory, STOP and
re-run with the correct suffix.

## Step 2 — Activate the venv

```tcsh
source ~/dagster-venv/bin/activate.csh
```
```bash
source ~/dagster-venv/bin/activate
```

**Verify:**
```
which dagster
dagster --version
```
- `which dagster` must point inside the venv (e.g. `~/dagster-venv/bin/dagster`),
  NOT `/usr/bin/dagster`.
- `dagster --version` must report `1.13.3`. If it doesn't, STOP and ask
  the user.

## Step 3 — Launch dagster against the lesson workspace

Each lesson directory has its own `workspace.yaml`. Use the **absolute
path** (no `cd` chains):

```
dagster dev -w /abs/path/to/personalities/dagster-expert/learn/<NN>-<topic>/<code-dir>/workspace.yaml
```

Example for lesson 01:
```
dagster dev -w $HOME/All-Might/personalities/dagster-expert/learn/01-asset-and-materialize/hello/workspace.yaml
```

**Verify:** the webserver banner appears, listing the lesson's code
location. Open `http://localhost:3000` in a browser; confirm the asset(s)
listed in the lesson's `README.md` are visible.

## Step 4 — Cleanup (only after the lesson is done)

```tcsh
unsetenv DAGSTER_HOME
```
```bash
unset DAGSTER_HOME
```
This prevents the sandbox env var leaking into other sessions where the
user might expect production paths.

---

## Hard refusal patterns

- `echo $DAGSTER_HOME` empty → **REFUSE** to launch dagster. Re-do Step 1.
- `which dagster` shows `/usr/bin/dagster` → **REFUSE** to launch dagster.
  The system pip likely doesn't have 1.13.3. Activate the venv.
- `dagster --version` shows anything other than `1.13.3` → **STOP and
  ASK** the user. Don't guess.
