import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class SmokePathSafetyTest(unittest.TestCase):
    def test_evidence_alias_input_is_rejected_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory(prefix="news-pipeline-path-") as temp_dir:
            temp = Path(temp_dir)
            input_path = temp / "sample.jsonl"
            input_path.write_text('{"article_id":"a1"}\n', encoding="utf-8")
            before = input_path.read_bytes()
            output_path = temp / "already_exists"
            output_path.mkdir()

            completed = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts" / "smoke.py"),
                    "--input",
                    str(input_path),
                    "--output",
                    str(output_path),
                    "--evidence",
                    str(input_path),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(completed.returncode, 2)
            self.assertIn("evidence path", completed.stderr)
            self.assertEqual(input_path.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
