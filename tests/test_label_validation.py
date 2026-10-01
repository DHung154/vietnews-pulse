import csv
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_labeling import validate_packet


class LabelValidationTest(unittest.TestCase):
    def test_blank_packet_is_valid_but_not_gold_ready(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            text = "Tiêu đề thử nghiệm"
            import hashlib
            metadata = root / "metadata.jsonl"
            metadata.write_text(json.dumps({"annotation_id": "a", "input_text": text, "input_sha256": hashlib.sha256(text.encode()).hexdigest(), "split": "pilot"}) + "\n", encoding="utf-8")
            fields = ["annotation_id", "input_text", "label"]
            for name in ("a.csv", "b.csv"):
                with (root / name).open("w", encoding="utf-8", newline="") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fields)
                    writer.writeheader()
                    writer.writerow({"annotation_id": "a", "input_text": text, "label": ""})
            result = validate_packet(metadata, root / "a.csv", root / "b.csv")
            self.assertEqual(result["status"], "ok")
            self.assertFalse(result["gold_ready"])
            self.assertEqual(result["compared_filled_pairs"], 0)

    def test_disagreement_and_non_three_class_never_become_gold_ready(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            import hashlib
            text = "Nội dung"
            metadata = root / "metadata.jsonl"
            metadata.write_text(json.dumps({"annotation_id": "a", "input_text": text, "input_sha256": hashlib.sha256(text.encode()).hexdigest(), "split": "dev"}) + "\n", encoding="utf-8")
            for path, label in ((root / "a.csv", "positive"), (root / "b.csv", "negative")):
                path.write_text(f"annotation_id,input_text,label\na,{text},{label}\n", encoding="utf-8")
            result = validate_packet(metadata, root / "a.csv", root / "b.csv")
            self.assertEqual(result["disagreement_count_before_adjudication"], 1)
            self.assertTrue(result["annotation_complete"])
            self.assertFalse(result["gold_ready"])

            path.write_text(f"annotation_id,input_text,label\na,{text},needs_context\n", encoding="utf-8")
            result = validate_packet(metadata, root / "a.csv", root / "a.csv")
            self.assertTrue(result["annotation_complete"])
            self.assertFalse(result["gold_ready"])


if __name__ == "__main__":
    unittest.main()
