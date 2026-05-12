"""Layer 1 — concrete dep rules. One file per rule (SRP).

Add a new rule:
  1. New file ``rules/my_rule.py`` exposing a frozen-dataclass class.
  2. Register it in ``registry.py``'s ``_default_rules()``.
  3. Add ``tests/rules/test_my_rule.py``.
"""
