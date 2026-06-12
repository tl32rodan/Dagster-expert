"""Data-version base strategies (whitepaper appendix B)."""
import pytest

from framework.versioning.base import resolve_version


def test_content_hash_deterministic():
    fn = resolve_version("content_hash")
    assert fn("hello") == fn("hello")
    assert fn("hello") != fn("world")
    assert len(fn("x")) == 16


def test_timestamp_changes():
    fn = resolve_version("timestamp")
    a = fn("x")
    b = fn("x")
    assert a != b


def test_custom_callable_imported():
    fn = resolve_version("framework.versioning.base:content_hash_version")
    assert fn("hello") == resolve_version("content_hash")("hello")


def test_unknown_strategy_rejected():
    with pytest.raises(ValueError, match="unknown version strategy"):
        resolve_version("nope")
