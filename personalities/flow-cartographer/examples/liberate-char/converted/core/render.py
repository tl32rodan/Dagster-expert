"""Generic template rendering (stdlib only)."""
from string import Template


def render(template, /, **values) -> str:
    """Render a `string.Template`. Every `$placeholder` must be supplied
    (a missing one raises `KeyError`); values are str()-converted, so ints
    are fine. Strict-by-default keeps a weak agent from silently emitting a
    file with an unfilled hole."""
    return Template(template).substitute(values)
