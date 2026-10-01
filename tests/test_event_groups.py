import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.apply_event_groups import apply_groups
from scripts.prepare_event_groups import prepare


class EventGroupTest(unittest.TestCase):
    def test_prepare_keeps_unresolved_rows_pending(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = root / "metadata.jsonl"
            rows = [
                {"annotation_id": "a", "article_id": "article-1", "content_group_id": "content-1", "event_group_id": None, "split": "pilot"},
                {"annotation_id": "b", "article_id": "article-1", "content_group_id": "content-2", "event_group_id": None, "split": "pilot"},
                {"annotation_id": "c", "article_id": "article-2", "content_group_id": "content-3", "event_group_id": None, "split": "pilot"},
            ]
            metadata.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            result = prepare(metadata, root / "review")
            self.assertEqual(result["counts"]["safe_article_candidates"], 2)
            self.assertEqual(result["counts"]["pending_manual"], 1)
            header = (root / "review" / "event_group_review.csv").read_text(encoding="utf-8").splitlines()[0]
            self.assertIn("event_group_id", header)
            self.assertIn("input_text", header)
            self.assertIn("input_sha256", header)

    def test_apply_requires_reviewed_complete_groups(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = root / "metadata.jsonl"
            rows = [{"annotation_id": "a", "article_id": "article-1", "content_group_id": "content-1", "event_group_id": None, "input_text": "Text", "split": "test"}]
            metadata.write_text(json.dumps(rows[0]) + "\n", encoding="utf-8")
            prepare(metadata, root / "prepared")
            review = root / "prepared" / "event_group_review.csv"
            with review.open(encoding="utf-8", newline="") as handle:
                review_rows = list(csv.DictReader(handle))
            review_rows[0].update({"event_group_id": "event-1", "review_status": "reviewed", "reviewer": "reviewer-1", "review_reason": "same event"})
            with review.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(review_rows[0]))
                writer.writeheader()
                writer.writerows(review_rows)
            result = apply_groups(metadata, review, root / "gold_metadata.jsonl")
            self.assertEqual(result["event_group_count"], 1)
            self.assertEqual(json.loads((root / "gold_metadata.jsonl").read_text(encoding="utf-8").splitlines()[0])["event_group_source"], "human_reviewed")

    def test_changed_text_same_id_and_missing_provenance_are_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = root / "metadata.jsonl"
            metadata.write_text(json.dumps({"annotation_id": "a", "article_id": "article-1", "content_group_id": "content-1", "event_group_id": None, "input_text": "Original", "split": "test"}) + "\n", encoding="utf-8")
            prepare(metadata, root / "prepared")
            review = root / "prepared" / "event_group_review.csv"
            with review.open(encoding="utf-8", newline="") as handle:
                review_rows = list(csv.DictReader(handle))
            review_rows[0].update({"event_group_id": "event-1", "review_status": "reviewed", "reviewer": "reviewer-1", "review_reason": "same event"})
            with review.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(review_rows[0]))
                writer.writeheader()
                writer.writerows(review_rows)
            changed = root / "changed.jsonl"
            changed.write_text(json.dumps({"annotation_id": "a", "article_id": "article-1", "content_group_id": "content-1", "event_group_id": None, "input_text": "Changed", "split": "test"}) + "\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                apply_groups(changed, review, root / "out.jsonl")
            self.assertFalse((root / "out.jsonl").exists())

    def test_missing_reviewer_or_reason_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = root / "metadata.jsonl"
            metadata.write_text(json.dumps({"annotation_id": "a", "article_id": "article-1", "content_group_id": "content-1", "event_group_id": None, "input_text": "Text", "split": "test"}) + "\n", encoding="utf-8")
            prepare(metadata, root / "prepared")
            review = root / "prepared" / "event_group_review.csv"
            with review.open(encoding="utf-8", newline="") as handle:
                review_rows = list(csv.DictReader(handle))
            review_rows[0].update({"event_group_id": "event-1", "review_status": "reviewed", "reviewer": "", "review_reason": ""})
            with review.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(review_rows[0]))
                writer.writeheader()
                writer.writerows(review_rows)
            with self.assertRaises(ValueError):
                apply_groups(metadata, review, root / "out.jsonl")


if __name__ == "__main__":
    unittest.main()
