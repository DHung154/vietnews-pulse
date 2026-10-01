import json
import tempfile
import unittest
from pathlib import Path

from scripts.collect_rss import overall_status
from src.news_pipeline.history import import_jsonl
from src.news_pipeline.paths import validate_artifact_paths


class M2SafetyTest(unittest.TestCase):
    def test_all_304_is_success(self):
        self.assertEqual(overall_status([{"status": "no_new_data"}, {"status": "no_new_data"}]), "ok")

    def test_evidence_inside_input_is_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            input_path = base / "raw"; input_path.mkdir()
            with self.assertRaises(ValueError):
                validate_artifact_paths(inputs=(input_path,), outputs=(base / "out",), evidence=input_path / "manifest.json")

    def test_output_cannot_be_nested_in_another_output(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            with self.assertRaises(ValueError):
                validate_artifact_paths(inputs=(), outputs=(base / "raw", base / "raw" / "state.json"), evidence=base / "evidence.json")

    def test_history_keeps_invalid_rows_and_reports_rejections(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root)
            input_path = base / "history.jsonl"
            input_path.write_text('{"title": "ok", "url": "https://example.com/1"}\n{"title": "missing url"}\n', encoding="utf-8")
            mapping = base / "mapping.json"
            mapping.write_text(json.dumps({"title": "title", "link": "url"}), encoding="utf-8")
            result = import_jsonl(input_path, mapping, base / "raw.jsonl", "historical_test")
            self.assertEqual((result["records_seen"], result["accepted"], result["rejected"]), (2, 1, 1))
            self.assertEqual(len((base / "raw.jsonl").read_text(encoding="utf-8").splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
