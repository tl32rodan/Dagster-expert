# PREREQUISITES — `dagster-operator`

Before this personality can help you bootstrap a Dagster
deployment, the host environment needs the following.

## On the connected host (used to build the wheelhouse)

- Internet access to PyPI **at build time only** (`pip download`)
- Python interpreter matching the air-gap host's Python
  (same major.minor — e.g. both 3.11)
- `pip` ≥ 21
- ~2 GB free disk for the wheelhouse + transient build artifacts

## On the air-gap host

- **OS**: Linux x86_64 or aarch64 (RHEL 8+, Ubuntu 20.04+, similar)
- **Python**: CPython 3.9–3.12 (`python3 --version`)
- **`pip`** + **`venv`** modules available
  (`python3 -m venv --help` works)
- **gcc / build-essential** if any wheel needs to compile from sdist
  (rare with pure-binary wheelhouse, but `psycopg2-binary` is the
  preferred choice precisely to avoid this)
- **No `uv`, `dg`, `pipx`, `poetry`** required — refuse if tooling
  asks for them
- **Disk**: ~5 GB free for venv + wheelhouse + DAGSTER_HOME storage
- **`DAGSTER_HOME` path** that is persistent across reboots
  (NOT under `/tmp`)

## Postgres (production only)

- Postgres 12+ reachable from the air-gap host (default port 5432)
- A database (e.g. `dagster`) and user with full rights on it
- Network: `nc -zv <pg-host> 5432` succeeds from the air-gap host
- Credentials available as env vars (NOT in `dagster.yaml`):
  set `DAGSTER_PG_PASSWORD` in the systemd `EnvironmentFile=`

For dev / single-developer setups, SQLite is fine — skip the
Postgres section.

## Network & ports the deployment will use

| Port | Service | Reachable from |
|---|---|---|
| 3000 | dagster-webserver | end users (typically through nginx/Caddy on 443) |
| 4000+ | each gRPC code server | webserver + daemon hosts |
| 5432 | Postgres | webserver + daemon + code server hosts |

Air-gap reminder: **none of these need outbound** access to the
public internet at runtime.

## Permissions

- A dedicated service user (e.g. `dagster:dagster`) owns:
  - `DAGSTER_HOME` (e.g. `/var/lib/dagster`)
  - The venv (e.g. `/opt/dagster-venv`)
  - The code repository (`/opt/my_pipelines`)
  - Wheelhouse (during install only)
- `systemd` available if you want managed service lifecycle (the
  `start-services` skill assumes systemd; adapt to your init if
  not)

## What the operator will need to know about your deployment

These values get baked into ROLE.md's **"Where things actually
are"** section. Have answers ready:

- Wheelhouse path
- `DAGSTER_HOME` path (prod vs dev)
- Code repository root
- `workspace.yaml` path
- Postgres host:port
- gRPC code-server host:port (per location)
- Webserver URL

If the operator doesn't have these yet, read
`skills/bootstrap-airgap/SKILL.md` first — it walks through
deciding them.

## Sanity check before calling this personality "ready"

```bash
python3 --version              # 3.9–3.12
which pip                      # /usr/bin/pip3 or venv pip
ls ~/wheelhouse                # wheels present
echo $DAGSTER_HOME             # set, not under /tmp
nc -zv <pg-host> 5432          # if using Postgres
```

If any of these are surprises, fix the host before running
`bootstrap-airgap`.
