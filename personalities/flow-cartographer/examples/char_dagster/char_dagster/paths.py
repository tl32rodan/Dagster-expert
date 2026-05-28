"""All path construction lives here. Asset bodies must NEVER concatenate
their own output paths — they go through these helpers so the (asset name,
partition key) → path mapping has one canonical source.

`derive_lpe_rc` collapses ``LPE_ssgnp_cworst_T_25c`` → ``LPE_cworst_T_25c``
by dropping the process-variant token (index 1 after splitting on ``_``).
"""
from pathlib import Path


def derive_lpe_rc(trio_group: str) -> str:
    """Drop the process-variant token from a trio_group string.

    >>> derive_lpe_rc("LPE_ssgnp_cworst_T_25c")
    'LPE_cworst_T_25c'
    >>> derive_lpe_rc("LPE_typgnp_cworst_T_25c")
    'LPE_cworst_T_25c'
    """
    parts = trio_group.split("_")
    if len(parts) < 3 or parts[0] != "LPE":
        raise ValueError(f"trio_group {trio_group!r} is not a valid LPE_*_… string")
    return "_".join([parts[0]] + parts[2:])


def generated_root(cfg) -> Path:
    return Path(cfg.paths.generated_dir)


def add_to_liberate_path(cfg) -> Path:
    return generated_root(cfg) / "add_to_liberate.tcl"


def bolt_path(cfg) -> Path:
    return generated_root(cfg) / "Bolt.tcl"


def main_tcl_path(cfg) -> Path:
    return generated_root(cfg) / "main.tcl"


def mnpvt_cell_list_path(cfg, trio_group: str) -> Path:
    return generated_root(cfg) / ".MnPVT_cell_list" / f"char_trio_group{trio_group}.tcl"


def model_card_path(cfg, trio_group: str, pvt_name: str) -> Path:
    return generated_root(cfg) / trio_group / "Model_card" / f"{pvt_name}.inc"


def netlist_path(cfg, trio_group: str, cell: str) -> Path:
    lpe_rc = derive_lpe_rc(trio_group)
    return generated_root(cfg) / trio_group / "Netlist" / lpe_rc / f"{cell}.spi"


def template_tcl_path(cfg, trio_group: str, pvt_name: str) -> Path:
    return (generated_root(cfg) / trio_group / "Template"
            / f"{cfg.project.name}{pvt_name}.tcl")


def section_dir(cfg, trio_group: str, section_number: int) -> Path:
    return (generated_root(cfg) / trio_group / ".Trio_pvt_setting"
            / "SECTION" / f"SECTION_{section_number}")


def section_tcl_path(cfg, trio_group: str, pvt_name: str,
                     section_number: int) -> Path:
    return section_dir(cfg, trio_group, section_number) / f"char_{pvt_name}.tcl"


SECTION_NUMBERS = (2, 3, 4, 5, 6, 7)


def all_section_files_for_partition(cfg, trio_group: str, pvt_name: str):
    """Return the 7 files that make up one (trio_group, pvt) partition of
    ``pvt_section_files``: 1 Template + 6 SECTION files."""
    files = [template_tcl_path(cfg, trio_group, pvt_name)]
    files.extend(
        section_tcl_path(cfg, trio_group, pvt_name, n) for n in SECTION_NUMBERS
    )
    return files


def run_scr_path(cfg, trio_group: str, pvt_name: str) -> Path:
    return generated_root(cfg) / trio_group / pvt_name / "run.scr"


def output_dir(cfg, trio_group: str, pvt_name: str) -> Path:
    return Path(cfg.paths.output_dir) / trio_group / pvt_name


def drop_dir(cfg, trio_group: str) -> Path:
    return Path(cfg.paths.work_dir) / "_drop" / trio_group


def template_dir(cfg) -> Path:
    p = Path(cfg.paths.template_dir)
    if not p.is_absolute():
        # template_dir is allowed to be relative to work_dir for repo-portability
        p = Path(cfg.paths.work_dir) / p
    return p
