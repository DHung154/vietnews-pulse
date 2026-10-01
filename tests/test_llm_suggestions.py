import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.validate_llm_suggestions import validate


class LlmSuggestionsTest(unittest.TestCase):
    def _fixture(self, root: Path):
        text = "Một bài viết, có bằng chứng ngắn."
        metadata_row = {
            "annotation_id": "sample-1",
            "input_text": text,
            "input_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        }
        metadata = root / "metadata.jsonl"
        metadata.write_text(json.dumps(metadata_row, ensure_ascii=False) + "\n", encoding="utf-8")
        suggestion = {
            "annotation_id": "sample-1",
            "input_sha256": metadata_row["input_sha256"],
            "suggested_sentiment": "needs_context",
            "evidence_span": "bài viết, có",
            "reason": "Chưa đủ ngữ cảnh.",
            "review_priority": "high",
            "label_origin": "llm",
            "model": "gpt-5.5",
            "reasoning_effort": "high",
            "human_verified": False,
            "suggested_event_group_id": "unresolved",
            "group_reason": "Chưa đủ dấu hiệu cùng sự kiện.",
            "group_origin": "llm",
        }
        suggestions = root / "suggestions.jsonl"
        suggestions.write_text(json.dumps(suggestion, ensure_ascii=False) + "\n", encoding="utf-8")
        review = root / "review.csv"
        row = {**suggestion, "input_text": text, "human_sentiment": "", "human_reason": ""}
        with review.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(row), quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerow(row)
        return metadata, suggestions, review

    def test_accepts_complete_llm_provenance_and_unresolved_group(self):
        with tempfile.TemporaryDirectory() as temp:
            result = validate(*self._fixture(Path(temp)))
            self.assertEqual(result["status"], "ok")
            self.assertFalse(result["human_gold"])

    def test_rejects_evidence_not_in_frozen_input(self):
        with tempfile.TemporaryDirectory() as temp:
            metadata, suggestions, review = self._fixture(Path(temp))
            row = json.loads(suggestions.read_text(encoding="utf-8"))
            row["evidence_span"] = "không có trong bài"
            suggestions.write_text(json.dumps(row) + "\n", encoding="utf-8")
            result = validate(metadata, suggestions, review)
            self.assertEqual(result["status"], "invalid")
            self.assertIn("evidence_not_in_input:sample-1", result["errors"])


if __name__ == "__main__":
    unittest.main()
