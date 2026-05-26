"""Filesystem conventions (folder-as-asset). The Dagster project writes its
data under DATA_ROOT, completely separate from the code. Nothing here reads
the original $FLOW_SRC — everything is generated from config into SOURCES."""
import os
from pathlib import Path

DATA_ROOT = Path(os.environ.get("LIBERATE_DAG_ROOT", "/tmp/liberate-char-dag"))
SOURCES = DATA_ROOT / "sources"   # generator assets write here (mirrors flow-src layout)
WORK = DATA_ROOT / "work"         # per-leaf run.scr / main.tcl
OUT = DATA_ROOT / "out"           # liberate writes <pvt>__<cell>.lib/.ldb here
DROP = DATA_ROOT / "drop"         # sensor watches here for new netlists

CONVERTED = Path(__file__).resolve().parents[1]          # converted/
LIBERATE_BIN = CONVERTED / "core" / "bin" / "liberate"   # the (mock) tool
BSUB_BIN = CONVERTED / "core" / "bin" / "bsub"           # the (mock) bsub
LSF_SUBMIT = CONVERTED / "core" / "lsf_submit.py"        # bsub wrapper
LIBERATE_INNER = CONVERTED / "core" / "liberate_inner.py"  # pipes-aware inner
