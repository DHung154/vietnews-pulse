import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.compare_llm_suggestions import compare
from scripts.prepare_weak_labels import build


def _row(key, text, sentiment, group):
    return {
        "annotation_id": key,
        "input_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "suggested_sentiment": sentiment,
        "suggested_event_group_id": group,
        "review_priority": "normal",
    }


class LlmRoundsTest(unittest.TestCase):
    def test_compare_flags_disagreement_and_weak_builder_excludes_it(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata_rows = [{"annotation_id": "a", "input_text": "A", "input_sha256": hashlib.sha256(b"A").hexdigest(), "content_group_id": "content-a"}, {"annotation_id": "b", "input_text": "B", "input_sha256": hashlib.sha256(b"B").hexdigest(), "content_group_id": "content-b"}]
            metadata = root / "metadata.jsonl"
            metadata.write_text("\n".join(json.dumps(row) for row in metadata_rows) + "\n", encoding="utf-8")
            first = root / "first.jsonl"
            second = root / "second.jsonl"
            first.write_text("\n".join(json.dumps(_row("a", "A", "positive", "g1")) for _ in [0]) + "\n" + json.dumps(_row("b", "B", "negative", "g2")) + "\n", encoding="utf-8")
            second.write_text(json.dumps(_row("a", "A", "positive", "g9")) + "\n" + json.dumps(_row("b", "B", "neutral", "g2")) + "\n", encoding="utf-8")
            comparison = compare(first, second, metadata, root / "comparison")
            self.assertEqual(comparison["sentiment_agreement_count"], 1)
            weak = build(first, second, metadata, root / "weak")
            self.assertEqual(weak["row_count"], 1)
            self.assertEqual(json.loads((root / "weak" / "weak_train.jsonl").read_text(encoding="utf-8").splitlines()[0])["label"], "positive")


if __name__ == "__main__":
    unittest.main()
