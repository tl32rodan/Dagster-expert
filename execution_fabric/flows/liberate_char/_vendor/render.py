"""Generic template rendering (stdlib only). Vendored from converted/core."""
from string import Template


def render(template, /, **values) -> str:
    """Render a `string.Template`. Every `$placeholder` must be supplied
    (a missing one raises `KeyError`); values are str()-converted."""
    return Template(template).substitute(values)
