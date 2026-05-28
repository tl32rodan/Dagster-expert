from char_dagster.assets.source_generation import (
    add_to_liberate_tcl,
    bolt_tcl,
    main_char_script,
    mnpvt_cell_list_tcl,
    model_card_files,
    netlist_files,
    pvt_section_files,
)
from char_dagster.assets.execution import characterization_run, validation_check

ALL_ASSETS = [
    add_to_liberate_tcl,
    bolt_tcl,
    mnpvt_cell_list_tcl,
    model_card_files,
    netlist_files,
    pvt_section_files,
    main_char_script,
    characterization_run,
    validation_check,
]
