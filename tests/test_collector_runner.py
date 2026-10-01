import json
import sys
import tempfile
import unittest
from pathlib import Path

from scripts.collector_runner import supervise


class CollectorRunnerTest(unittest.TestCase):
    def test_exit_code_utf8_and_unique_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = supervise([sys.executable, "-c", "print(chr(0x1ec7)); raise SystemExit(7)"], root, 10)
            second = supervise([sys.executable, "-c", "print('ok')"], root, 10)
            self.assertEqual(first["exit_code"], 7)
            self.assertEqual(first["status"], "failed")
            self.assertEqual(Path(first["stdout_path"]).read_text(encoding="utf-8").strip(), "ệ")
            self.assertEqual(second["exit_code"], 0)
            self.assertNotEqual(first["evidence_path"], second["evidence_path"])
            self.assertEqual(json.loads(Path(first["evidence_path"]).read_text())["exit_code"], 7)

    def test_deadline_kills_only_owned_child_and_preserves_next_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            result = supervise([sys.executable, "-c", "import time; print('started', flush=True); time.sleep(20)"], root, 0.3)
            self.assertEqual(result["status"], "timed_out")
            self.assertEqual(result["exit_code"], 124)
            self.assertIsNotNone(result["child_exit_code"])
            self.assertIn("ended_at_utc", json.loads(Path(result["evidence_path"]).read_text()))
            self.assertEqual(supervise([sys.executable, "-c", "pass"], root, 10)["exit_code"], 0)


if __name__ == "__main__":
    unittest.main()
