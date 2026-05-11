---
name: bootstrap-airgap
description: Install Dagster on an air-gap host using a wheelhouse (offline pip mirror). Covers the connected-host download step + the air-gap host install step.
---

<!-- all-might generated -->

# bootstrap-airgap — install Dagster on an air-gap host

## When to use

- User says "I want to install Dagster on a machine without internet"
- User says "set up a fresh Dagster environment"
- User asks about `pip install dagster` and you suspect they're
  air-gapped (no PyPI access)

## What you'll do (overview)

The pattern is **two-step**: download wheels on a connected host,
transfer to the air-gap host, install with `--no-index`. Internet
is never needed on the air-gap side.

```
[connected host] ─── pip download ───→  wheelhouse/
                                         |
                          (USB / approved file transfer)
                                         |
                                         v
[air-gap host] ──── pip install --no-index --find-links=wheelhouse
```

## Step 1 — On the connected host (download wheels)

```bash
# Where to put the wheels (anywhere writable):
mkdir -p ~/wheelhouse
cd ~/wheelhouse

# Download Dagster + the modules you'll actually use.
# CRITICAL: pin Python version + platform tags to MATCH the
# air-gap host. If air-gap is Linux x86_64 + Python 3.11:
pip download \
    dagster==1.13.3 \
    dagster-webserver==1.13.3 \
    dagster-graphql==1.13.3 \
    dagster-postgres==1.13.3 \
    dagster-pipes==1.13.3 \
    --python-version 3.11 \
    --platform manylinux2014_x86_64 \
    --platform manylinux_2_17_x86_64 \
    --only-binary=:all: \
    -d .
```

Adjust `--python-version` and `--platform` to match the air-gap
host's `python3 --version` and `uname -m`. If air-gap is aarch64
(e.g. ARM SoC), use `manylinux2014_aarch64` instead.

For Dagster + Docker launcher add `dagster-docker==1.13.3` to
the list. For MinIO artifact store add `dagster-aws` (which uses
the S3-compatible API).

### Verify the wheelhouse is complete BEFORE transferring

```bash
# Dry-run install to detect missing transitive deps
python3 -m venv /tmp/test-install
source /tmp/test-install/bin/activate
pip install --no-index --find-links=./ \
    --dry-run \
    dagster dagster-webserver dagster-graphql dagster-postgres
deactivate
rm -rf /tmp/test-install
```

If the dry-run errors with "No matching distribution for X",
add X to the download list and re-download. Repeat until
dry-run succeeds. **Skipping this step is the #1 cause of
"missing wheel" outages mid-deployment.**

## Step 2 — Transfer to air-gap host

```bash
# Pack
tar czf wheelhouse.tar.gz wheelhouse/

# Transfer via your approved channel (USB, file server, etc.)
# At TSMC: usually IT's secure file transfer.

# On air-gap host:
mkdir -p ~/wheelhouse
tar xzf wheelhouse.tar.gz -C ~
```

## Step 3 — On the air-gap host (install)

```bash
# Always use a venv. NEVER pip install --user or system-wide.
python3 -m venv ~/dagster-venv
source ~/dagster-venv/bin/activate

# Install with --no-index so pip never reaches the network
pip install --no-index --find-links=~/wheelhouse \
    dagster==1.13.3 \
    dagster-webserver==1.13.3 \
    dagster-graphql==1.13.3 \
    dagster-postgres==1.13.3
```

### Verify the install

```bash
which dagster                 # ~/dagster-venv/bin/dagster
dagster --version             # dagster, version 1.13.3
dagster instance info         # may error until DAGSTER_HOME is set; ok
```

## Step 4 — Set DAGSTER_HOME

```bash
# Production: a real path, persistent across reboots
export DAGSTER_HOME=/var/lib/dagster
# (add to ~/.bashrc or system profile so it sticks)

# Development: anywhere stable
# export DAGSTER_HOME=~/.dagster

mkdir -p "$DAGSTER_HOME"

# DAGSTER_HOME must contain dagster.yaml. See
# skills/dagster-yaml-reference/SKILL.md for the minimal config.
# Without dagster.yaml dagster will create a default but you're
# better off starting with an explicit one.
```

## Common pitfalls

### "No matching distribution for X" on air-gap install

The wheelhouse is missing a transitive dep. Don't try to fix it
on the air-gap host — go back to the connected host, add the
missing package to the download list, re-pack, re-transfer.

If you can't go back, check what package is missing:

```bash
pip install --no-index --find-links=~/wheelhouse dagster -v 2>&1 | grep "No matching"
```

Then `pip download <missing-pkg>` on the connected host.

### "ERROR: Package <X> requires a different Python: 3.11.4 not in <range>"

You downloaded wheels for Python 3.10 but air-gap host is 3.11
(or vice versa). Re-download with the right `--python-version`.

### Wheels are too old / new

Pin your dagster version. `dagster==1.13.3` (or whatever your
team chose) keeps your wheelhouse stable. Don't say `dagster`
alone — pip picks the latest that fits, and a future re-download
gets a different version.

### Air-gap host is aarch64 not x86_64

Use platform tag `manylinux2014_aarch64`. Some packages don't
ship aarch64 wheels; you may need to download `manylinux_2_28_aarch64`
(newer baseline) for those, e.g. `greenlet` 3.5+.

### DAGSTER_HOME points at a tmpfs / `/tmp/`

Dagster will work but your run history and event logs vanish on
reboot. Use a persistent path (`/var/lib/...`, `~/...`, etc).

## Verification (run this after Step 4)

```bash
source ~/dagster-venv/bin/activate
export DAGSTER_HOME=/var/lib/dagster   # or wherever

# All five CLIs should resolve
which dagster
which dagster-daemon
which dagster-webserver
which dagster-webserver-debug
which dagster-graphql

# instance info should print without error (may say "no
# dagster.yaml" — that's the next skill)
dagster instance info
```

If any of these fail, go back to Step 3 and re-check the wheel
install.

## Next step

Once installed, configure with `dagster.yaml`. See
`skills/dagster-yaml-reference/SKILL.md`.
