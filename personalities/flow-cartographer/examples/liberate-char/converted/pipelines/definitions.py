"""The single Definitions object Dagster loads (workspace.yaml ->
pipelines.definitions). No @job anywhere except the asset-selection job the
sensor targets; Dagster builds the implicit __ASSET_JOB for materializes."""
from dagster import Definitions, PipesSubprocessClient

from .assets import (
    cell_list, characterize, main_tcl, model_card, netlist, section_tcl, template_tcl,
)
from .sensor import char_job, netlist_drop_sensor

defs = Definitions(
    assets=[template_tcl, section_tcl, model_card, netlist, cell_list, main_tcl, characterize],
    jobs=[char_job],
    sensors=[netlist_drop_sensor],
    resources={"pipes_subprocess_client": PipesSubprocessClient()},
)
