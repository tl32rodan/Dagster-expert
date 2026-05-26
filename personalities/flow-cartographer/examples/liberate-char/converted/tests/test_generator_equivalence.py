"""Prove the L0 generators reproduce flow-src/ exactly:
  * path-free files            -> byte-identical
  * main.tcl / run.scr         -> byte-identical when given the ref root
  * changing the root          -> ONLY the path substring changes
This is the source-level half of "the products differ only in paths".
"""
import sys
import unittest
from pathlib import Path

CONVERTED = Path(__file__).resolve().parents[1]
FLOW_SRC = CONVERTED.parent / "flow-src"
sys.path.insert(0, str(CONVERTED))

from core.config import load_config            # noqa: E402
from pipelines import generators as g          # noqa: E402

REF_ROOT = "/tmp/liberate-char-ref"


class TestGeneratorReproducesFlowSrc(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cfg = load_config(CONVERTED / "config" / "liberate.yaml")

    def _ref(self, rel):
        return (FLOW_SRC / rel).read_text()

    def test_templates(self):
        for pvt in self.cfg.pvt_keys:
            self.assertEqual(g.gen_template(self.cfg, pvt),
                             self._ref(f"templates/template_{pvt}.tcl"), pvt)

    def test_sections(self):
        for pvt in self.cfg.pvt_keys:
            for n in self.cfg.sections:
                self.assertEqual(g.gen_section(self.cfg, pvt, n),
                                 self._ref(f"sections/{pvt}/section{n}.tcl"),
                                 f"{pvt}/section{n}")

    def test_model_cards(self):
        for pvt in self.cfg.pvt_keys:
            self.assertEqual(g.gen_model_card(self.cfg, pvt),
                             self._ref(f"modelcard/model_{pvt}.tcl"), pvt)

    def test_netlists(self):
        for cell in self.cfg.cell_keys:
            self.assertEqual(g.gen_netlist(self.cfg, cell),
                             self._ref(f"netlist/{cell}.sp"), cell)

    def test_cell_list(self):
        self.assertEqual(g.gen_cell_list(self.cfg), self._ref("Mnpvt_cell_list.tcl"))

    def test_main_tcl_with_ref_root(self):
        self.assertEqual(g.gen_main_tcl(self.cfg, REF_ROOT), self._ref("main.tcl"))

    def test_run_scr_with_ref_root(self):
        self.assertEqual(g.gen_run_scr_full(self.cfg, REF_ROOT), self._ref("run.scr"))

    def test_only_paths_differ_when_root_changes(self):
        other = "/some/other/dagster/root"
        self.assertEqual(g.gen_main_tcl(self.cfg, other).replace(other, REF_ROOT),
                         self._ref("main.tcl"))
        self.assertEqual(g.gen_run_scr_full(self.cfg, other).replace(other, REF_ROOT),
                         self._ref("run.scr"))


if __name__ == "__main__":
    unittest.main()
