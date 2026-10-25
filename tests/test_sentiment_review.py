"""Synthetic fixtures only: tests never create real human labels."""
import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from scripts.sentiment_review import prepare, import_reviews


class SentimentReviewTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.metadata = self.root / "metadata.jsonl"
        self.guide = self.root / "guide.md"
        self.guide.write_text("Synthetic test guideline", encoding="utf-8")
        text = "Thành công </script><img src=x onerror=alert(1)>"
        self.row = {"annotation_id": "fixture-a", "input_text": text,
                    "input_sha256": hashlib.sha256(text.encode()).hexdigest(), "split": "pilot"}
        self.metadata.write_text(json.dumps(self.row) + "\n", encoding="utf-8")
        self.suggestions = []
        for i, label in enumerate(("positive", "negative")):
            p = self.root / f"suggestion{i}.jsonl"
            p.write_text(json.dumps({"annotation_id": "fixture-a", "input_sha256": self.row["input_sha256"],
                "suggested_sentiment": label, "evidence_span": "Thành công", "reason": "Fixture",
                "review_priority": "high", "label_origin": "llm", "model": "gpt-5.5", "reasoning_effort": "high",
                "human_verified": False, "suggested_event_group_id": "unresolved", "group_reason": "Fixture", "group_origin": "llm"}) + "\n", encoding="utf-8")
            self.suggestions.append(p)

    def bundle(self, assisted=False):
        out = self.root / ("assisted" if assisted else "blind")
        prepare(self.metadata, self.guide, out, self.suggestions if assisted else [])
        return out

    def reviews(self, out):
        b = json.loads((out / "bundle.json").read_text(encoding="utf-8"))
        return {"format_version": b["format_version"], "bundle_id": b["bundle_id"], "mode": b["mode"],
            "reviews": [{"annotation_id": "fixture-a", "input_sha256": self.row["input_sha256"],
                "label": "positive", "reason": "Synthetic fixture, not a human label", "evidence_span": "Thành công",
                "reviewer": "SYNTHETIC_TEST_ONLY", "reviewed": True, "reviewed_at": "2026-01-01T00:00:00Z"}]}

    def run_import(self, out, values):
        path = self.root / "submitted.json"
        path.write_text(json.dumps(values), encoding="utf-8")
        return import_reviews(out / "bundle.json", self.metadata, self.guide, path, self.root / "imported")

    def test_blind_has_no_suggestions_and_text_is_script_safe(self):
        out = self.bundle()
        b = json.loads((out / "bundle.json").read_text(encoding="utf-8"))
        page = (out / "review.html").read_text(encoding="utf-8")
        manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["html_sha256"], hashlib.sha256((out / "review.html").read_bytes()).hexdigest())
        self.assertNotIn("suggestions", b["records"][0])
        self.assertNotIn("source", b["records"][0])
        self.assertNotIn(self.row["input_text"], page)
        self.assertIn("\\u003c/script>", page)
        self.assertIn(".textContent=r.input_text", page)

    def test_assisted_import_keeps_ai_and_never_gold(self):
        out = self.bundle(True)
        result = self.run_import(out, self.reviews(out))
        self.assertFalse(result["gold_ready"])
        self.assertIsNone(result["metrics"])
        rows = [json.loads(line) for line in (self.root / "imported/reviewed_labels.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(rows[0]["label_origin"], "human_reviewed_llm")
        self.assertEqual(len(rows[0]["suggestions"]), 2)
        self.assertFalse(rows[0]["human_gold"])

    def test_blind_is_only_self_reported_pilot_not_gold(self):
        out = self.bundle()
        self.run_import(out, self.reviews(out))
        row = json.loads((self.root / "imported/reviewed_labels.jsonl").read_text(encoding="utf-8"))
        self.assertEqual(row["label_origin"], "human_self_reported")
        self.assertFalse(row["human_gold"])

    def test_missing_duplicate_and_unknown_ids_are_rejected(self):
        out = self.bundle()
        p = self.reviews(out)
        for values in ([], p["reviews"] * 2, [dict(p["reviews"][0], annotation_id="unknown")]):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.run_import(out, dict(p, reviews=values))
        self.assertFalse((self.root / "imported").exists())

    def test_bad_label_provenance_time_and_evidence_are_rejected(self):
        out = self.bundle(True)
        p = self.reviews(out)
        for changes in ({"label": "happy"}, {"reviewer": " "}, {"reason": ""}, {"reviewed": False},
                        {"reviewed_at": "2030-01-01T00:00:00Z"}, {"reviewed_at": "2026-01-01"},
                        {"evidence_span": "not in text"}, {"evidence_span": ""}, {"input_sha256": "bad"},
                        {"input_text": "Changed"}, {"human_gold": True}, {"human_verified": True}, {"label_origin": "human"}):
            values = copy.deepcopy(p)
            values["reviews"][0].update(changes)
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.run_import(out, values)

    def test_difficult_labels_are_not_coerced(self):
        out = self.bundle()
        values = self.reviews(out)
        values["reviews"][0].update(label="needs_context", evidence_span="")
        self.assertEqual(self.run_import(out, values)["label_counts"], {"needs_context": 1})

    def test_changed_metadata_guideline_bundle_or_mode_rejected(self):
        out = self.bundle()
        p = self.reviews(out)
        with self.assertRaises(ValueError):
            self.run_import(out, dict(p, mode="assisted"))
        original = self.guide.read_bytes()
        self.guide.write_bytes(b"Changed")
        with self.assertRaises(ValueError):
            self.run_import(out, p)
        self.guide.write_bytes(original)
        original = self.metadata.read_bytes()
        self.metadata.write_bytes(original + b"\n")
        with self.assertRaises(ValueError):
            self.run_import(out, p)
        self.metadata.write_bytes(original)
        b = json.loads((out / "bundle.json").read_text(encoding="utf-8"))
        b["records"][0]["input_text"] = "Changed"
        (out / "bundle.json").write_text(json.dumps(b), encoding="utf-8")
        with self.assertRaises(ValueError):
            self.run_import(out, p)

    def test_existing_output_and_source_alias_untouched(self):
        out = self.bundle()
        with self.assertRaises(FileExistsError):
            prepare(self.metadata, self.guide, out, [])
        values = self.reviews(out)
        self.run_import(out, values)
        original = self.metadata.read_bytes()
        with self.assertRaises(FileExistsError):
            import_reviews(out / "bundle.json", self.metadata, self.guide, self.root / "submitted.json", self.metadata)
        self.assertEqual(self.metadata.read_bytes(), original)
        with self.assertRaises(FileExistsError):
            self.run_import(out, values)

    def test_main_split_and_incomplete_ai_packet_rejected(self):
        row = dict(self.row, split="test")
        self.metadata.write_text(json.dumps(row), encoding="utf-8")
        with self.assertRaises(ValueError):
            prepare(self.metadata, self.guide, self.root / "main", [])
        self.metadata.write_text(json.dumps(self.row), encoding="utf-8")
        self.suggestions[0].write_text("", encoding="utf-8")
        with self.assertRaises(ValueError):
            prepare(self.metadata, self.guide, self.root / "bad", self.suggestions)


if __name__ == "__main__":
    unittest.main()
