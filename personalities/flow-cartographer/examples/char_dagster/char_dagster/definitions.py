"""Top-level Dagster definitions for char_dagster."""
from dagster import Definitions, PipesSubprocessClient

from char_dagster.assets import ALL_ASSETS
from char_dagster.sensor import cell_drop_sensor, netlist_job

defs = Definitions(
    assets=ALL_ASSETS,
    sensors=[cell_drop_sensor],
    jobs=[netlist_job],
    resources={
        "pipes_subprocess_client": PipesSubprocessClient(),
    },
)
