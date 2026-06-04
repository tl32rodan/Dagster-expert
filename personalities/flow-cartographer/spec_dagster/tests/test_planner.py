"""M3 reconciliation planner (pure)."""
from framework.sensor.planner import plan_reconcile


def test_all_missing():
    desired = {"INV|tt_25", "BUF|tt_25", "INV|ff_125"}
    assert plan_reconcile(desired, set()) == sorted(desired)


def test_partial_observed():
    desired = {"INV|tt_25", "BUF|tt_25", "INV|ff_125"}
    observed = {"INV|tt_25"}
    assert plan_reconcile(desired, observed) == ["BUF|tt_25", "INV|ff_125"]


def test_nothing_to_do():
    desired = {"INV|tt_25"}
    assert plan_reconcile(desired, desired) == []


def test_observed_superset():
    # stray observed keys (e.g. removed from desired) don't break anything
    assert plan_reconcile({"a"}, {"a", "b"}) == []
