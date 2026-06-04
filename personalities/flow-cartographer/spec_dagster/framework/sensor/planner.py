"""M3 — pure planning function (whitepaper §5.1, simplified).

liberate-char partitions `cell` directly (no work_items dimensionality
reduction), so reconciliation is one RunRequest per missing leaf — there
is no batching. The whitepaper's plan_batches (work_items -> coarse-key
batches) is the *other* reference flow (netlist_files); it is not needed
here and is intentionally not implemented in this D1 slice.

plan_reconcile is a pure function: no Dagster, no I/O.
"""
from __future__ import annotations


def plan_reconcile(desired: set[str], observed: set[str]) -> list[str]:
    """Return the sorted list of partition keys that are desired but not
    yet observed (materialized). One element => one RunRequest."""
    return sorted(desired - observed)
