"""Template substitution for char_dagster.

Jinja2 chosen over ``string.Template`` because TCL / SPI use ``${var}`` and
single-brace ``{...}`` heavily, which collide with ``string.Template``'s
``$placeholder`` syntax. Jinja's ``{{ }}`` and ``{% %}`` don't collide, and
``{% for %}`` lets us declare list expansion in the template rather than
concatenating in Python.

Strict mode is non-negotiable: a missing variable raises
``jinja2.UndefinedError`` at render time. Without it, a weak agent could
emit a half-rendered TCL file that ``liberate`` would then run with
garbage results.
"""
from jinja2 import BaseLoader, Environment, StrictUndefined

_env = Environment(
    loader=BaseLoader(),
    undefined=StrictUndefined,
    keep_trailing_newline=True,
    autoescape=False,            # rendering TCL / SPI, not HTML
)


def substitute_template(template: str, substitutions: dict) -> str:
    """Render a Jinja2 template string.

    Supported placeholder forms:
      ``{{ name }}``                           scalar
      ``{{ obj.attr }}`` / ``{{ obj['k'] }}``  nested dict / dataclass
      ``{% for x in xs %}…{% endfor %}``       list expansion
      ``{% if cond %}…{% endif %}``            conditional
    """
    return _env.from_string(template).render(**substitutions)
