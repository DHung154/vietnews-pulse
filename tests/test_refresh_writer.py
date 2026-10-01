import unittest
from pathlib import Path


class RefreshWriterTest(unittest.TestCase):
    def test_refresh_does_not_spawn_an_unowned_exporter(self):
        root = Path(__file__).parents[1]
        refresh = (root / "scripts" / "refresh.py").read_text(encoding="utf-8")
        exporter = (root / "scripts" / "export_serving.py").read_text(encoding="utf-8")
        self.assertIn("export_snapshot(", refresh)
        self.assertNotIn("scripts/export_serving.py", refresh)
        self.assertNotIn("--lock-held", exporter)


if __name__ == "__main__":
    unittest.main()
