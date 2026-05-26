import sys
import tempfile
import unittest
from pathlib import Path

CONVERTED = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(CONVERTED))

from core.render import render          # noqa: E402
from core.config import load_config     # noqa: E402
from core import diff_proof             # noqa: E402


class TestRender(unittest.TestCase):
    def test_basic(self):
        self.assertEqual(render("a=$x b=$y", x="1", y="2"), "a=1 b=2")

    def test_int_value(self):
        self.assertEqual(render("n=$n end", n=7), "n=7 end")

    def test_braced(self):
        self.assertEqual(render("${a}0", a=2), "20")

    def test_missing_raises(self):
        with self.assertRaises(KeyError):
            render("$missing here", x="1")


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.cfg = load_config(CONVERTED / "config" / "liberate.yaml")

    def test_order_preserved(self):
        self.assertEqual(self.cfg.pvt_keys, ["tt_25", "ff_125", "ss_m40"])
        self.assertEqual(self.cfg.cell_keys, ["INV", "BUF", "NAND2"])

    def test_params(self):
        self.assertEqual(self.cfg.pvts["tt_25"]["voltage"], "0.90")
        self.assertEqual(self.cfg.pvts["ss_m40"]["temperature"], "-40")
        self.assertEqual(self.cfg.sections, [2, 3, 4, 5, 6, 7])

    def test_validation_rejects_empty_pvts(self):
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
            f.write("flow_name: x\npvts: {}\ncells: {INV: {pins: A}}\nsections: []\n")
            name = f.name
        with self.assertRaises(ValueError):
            load_config(name)


class TestDiffProof(unittest.TestCase):
    @staticmethod
    def _lib(scr, body_digest):
        return ("/* liberate mock output */\n"
                f"/* source_scr: {scr} */\n"
                f"/* inputs: {scr}/a,{scr}/b */\n"
                "library (x) {\n"
                f"  char_digest : {body_digest};\n"
                "}\n")

    def test_equivalent_ignoring_paths(self):
        with tempfile.TemporaryDirectory() as da, tempfile.TemporaryDirectory() as db:
            (Path(da) / "x.lib").write_text(self._lib("/ref/root", "DIG"))
            (Path(da) / "x.ldb").write_text("digest DIG\n")
            (Path(db) / "x.lib").write_text(self._lib("/totally/other/root", "DIG"))
            (Path(db) / "x.ldb").write_text("digest DIG\n")
            ok, diffs = diff_proof.outputs_equivalent(da, db)
            self.assertTrue(ok, diffs)

    def test_body_difference_detected(self):
        with tempfile.TemporaryDirectory() as da, tempfile.TemporaryDirectory() as db:
            (Path(da) / "x.lib").write_text(self._lib("/ref", "DIG1"))
            (Path(da) / "x.ldb").write_text("digest DIG1\n")
            (Path(db) / "x.lib").write_text(self._lib("/ref", "DIG2"))
            (Path(db) / "x.ldb").write_text("digest DIG2\n")
            ok, diffs = diff_proof.outputs_equivalent(da, db)
            self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
