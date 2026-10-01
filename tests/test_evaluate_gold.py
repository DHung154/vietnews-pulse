import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.evaluate_gold import _file_sha256, _stable_gold_hash, evaluate


class GoldEvaluationTest(unittest.TestCase):
    def _metadata(self, root, split="test", count=3):
        path = root / "metadata.jsonl"
        with path.open("w", encoding="utf-8") as handle:
            for index in range(count):
                text = f"Tin {index}"
                handle.write(json.dumps({"annotation_id": str(index), "article_id": f"article-{index}", "content_group_id": f"content-{index}", "event_group_id": f"event-{index}", "input_text": text, "input_sha256": hashlib.sha256(text.encode()).hexdigest(), "split": split}) + "\n")
        return path

    def _human_chain(self, root, metadata, count):
        rows = [json.loads(line) for line in metadata.read_text(encoding="utf-8").splitlines()]
        annotator_a = root / "annotator_a.csv"
        annotator_b = root / "annotator_b.csv"
        adjudication = root / "adjudication.csv"
        for path in (annotator_a, annotator_b):
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["annotation_id", "input_text", "label"])
                writer.writeheader()
                for row in rows:
                    writer.writerow({"annotation_id": row["annotation_id"], "input_text": row["input_text"], "label": "positive"})
        with adjudication.open("w", encoding="utf-8", newline="") as handle:
            fields = ["annotation_id", "input_sha256", "label_a", "label_b", "final_label", "adjudicator_id", "adjudicated_at", "reason", "guideline_version"]
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            for row in rows:
                writer.writerow({"annotation_id": row["annotation_id"], "input_sha256": row["input_sha256"], "label_a": "positive", "label_b": "positive", "final_label": "positive", "adjudicator_id": "human-1", "adjudicated_at": "2026-09-17T00:00:00Z", "reason": "agreement confirmed", "guideline_version": "news_sentiment_labeling_guide_v0.1"})
        freeze = root / "freeze.json"
        from scripts.evaluate_gold import _file_sha256
        from scripts.evaluate_gold import _stable_gold_hash
        freeze.write_text(json.dumps({"status": "frozen", "human_adjudication": True, "adjudication_validated": True, "split": rows[0]["split"], "guideline_version": "news_sentiment_labeling_guide_v0.1", "metadata_sha256": _file_sha256(metadata), "annotator_a_sha256": _file_sha256(annotator_a), "annotator_b_sha256": _file_sha256(annotator_b), "adjudication_sha256": _file_sha256(adjudication), "gold_labels_sha256": _stable_gold_hash([{"annotation_id": row["annotation_id"], "final_label": "positive"} for row in rows]), "adjudication_count": count, "annotation_count": count}), encoding="utf-8")
        return annotator_a, annotator_b, adjudication, freeze

    def test_missing_prediction_is_blocked_before_metrics(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root)
            predictions = root / "predictions.csv"
            predictions.write_text("annotation_id,prediction\n0,positive\n1,positive\n", encoding="utf-8")
            chain = self._human_chain(root, metadata, 3)
            result = evaluate(metadata, "test", predictions, chain[3], chain[0], chain[1], chain[2])
            self.assertEqual(result["evaluation_status"], "blocked_prediction_contract_mismatch")
            self.assertEqual(result["missing_prediction_ids"], ["2"])
            self.assertIsNone(result["metrics"])

    def test_pilot_is_never_an_evaluation_split(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root, split="pilot")
            result = evaluate(metadata, "pilot")
            self.assertEqual(result["evaluation_status"], "blocked_pilot_not_evaluation_split")
            self.assertIsNone(result["metrics"])

    def test_duplicate_prediction_ids_are_blocked(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root, count=2)
            predictions = root / "predictions.csv"
            predictions.write_text("annotation_id,prediction\n0,positive\n0,positive\n1,positive\n", encoding="utf-8")
            chain = self._human_chain(root, metadata, 2)
            result = evaluate(metadata, "test", predictions, chain[3], chain[0], chain[1], chain[2])
            self.assertEqual(result["evaluation_status"], "blocked_prediction_contract_mismatch")
            self.assertEqual(result["duplicate_prediction_ids"], ["0"])
            self.assertIsNone(result["metrics"])

    def test_frozen_complete_test_can_be_scored(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root, count=2)
            rows = [json.loads(line) for line in metadata.read_text(encoding="utf-8").splitlines()]
            chain = self._human_chain(root, metadata, 2)
            predictions = root / "predictions.csv"
            predictions.write_text("annotation_id,prediction\n0,positive\n1,positive\n", encoding="utf-8")
            result = evaluate(metadata, "test", predictions, chain[3], chain[0], chain[1], chain[2])
            self.assertEqual(result["evaluation_status"], "ok")
            self.assertTrue(result["gold_manifest_valid"])
            self.assertEqual(result["metrics"]["count"], 2)

    def test_same_article_different_text_across_splits_is_leakage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = root / "metadata.jsonl"
            rows = []
            for annotation_id, split, text in (("a", "dev", "Bản đầu"), ("b", "test", "Bản cập nhật")):
                rows.append({"annotation_id": annotation_id, "article_id": "same-article", "content_group_id": f"content-{annotation_id}", "event_group_id": f"event-{annotation_id}", "input_text": text, "input_sha256": hashlib.sha256(text.encode()).hexdigest(), "split": split})
            metadata.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            result = evaluate(metadata, "test")
            self.assertEqual(result["evaluation_status"], "blocked_invalid_gold")
            self.assertGreater(result["split_group_leakage"]["overlap_counts"]["article_id"], 0)

    def test_annotator_text_mismatch_uses_packet_validator_rules(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root, count=2)
            chain = self._human_chain(root, metadata, 2)
            text = chain[0].read_text(encoding="utf-8").replace("Tin 0", "Tampered")
            chain[0].write_text(text, encoding="utf-8")
            result = evaluate(metadata, "test", None, chain[3], chain[0], chain[1], chain[2])
            self.assertEqual(result["evaluation_status"], "blocked_invalid_human_provenance")

    def test_missing_group_metadata_is_blocked(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root)
            rows = [json.loads(line) for line in metadata.read_text(encoding="utf-8").splitlines()]
            rows[0]["content_group_id"] = None
            metadata.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            result = evaluate(metadata, "test")
            self.assertEqual(result["evaluation_status"], "blocked_invalid_gold")

    def test_missing_event_group_is_blocked(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root)
            rows = [json.loads(line) for line in metadata.read_text(encoding="utf-8").splitlines()]
            rows[0]["event_group_id"] = None
            metadata.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
            result = evaluate(metadata, "test")
            self.assertEqual(result["evaluation_status"], "blocked_invalid_gold")

    def test_adjudication_final_label_and_manifest_provenance_are_required(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = self._metadata(root, count=2)
            chain = self._human_chain(root, metadata, 2)
            adjudication = chain[2]
            text = adjudication.read_text(encoding="utf-8").replace(",positive,human-1,", ",negative,human-1,")
            adjudication.write_text(text, encoding="utf-8")
            result = evaluate(metadata, "test", None, chain[3], chain[0], chain[1], adjudication)
            self.assertEqual(result["evaluation_status"], "blocked_invalid_human_provenance")


if __name__ == "__main__":
    unittest.main()
