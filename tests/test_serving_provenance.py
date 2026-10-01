import unittest
from pathlib import Path
import tempfile

from src.news_pipeline.serving import validate_gold_provenance, validate_provenance


TABLES = ("volume_6h", "sentiment_daily", "category_trends", "keywords_window")


def score_manifest(path: Path) -> dict:
    return {
        "status": "ok",
        "gold_path": str(path),
        "input_snapshot_ids": ["snap-a"],
        "model_versions": ["model-a"],
        "enriched_count": 2,
        "gold_counts": {name: 0 for name in TABLES},
        "window_bounds": {"previous_start": "p", "current_start": "c", "window_end": "e"},
    }


class ServingProvenanceTest(unittest.TestCase):
    def test_mismatch_is_rejected(self):
        manifest = score_manifest(Path("/tmp/gold"))
        with self.assertRaises(ValueError):
            validate_provenance("snap-b", "model-a", ["snap-a"], ["model-a"], manifest, 2)

    def test_matching_provenance_is_accepted(self):
        manifest = score_manifest(Path("/tmp/gold"))
        validate_provenance("snap-a", "model-a", ["snap-a"], ["model-a"], manifest, 2)

    def test_gold_path_and_required_counts_are_checked(self):
        with tempfile.TemporaryDirectory() as root:
            gold = Path(root) / "gold"
            gold.mkdir()
            validate_gold_provenance(gold, score_manifest(gold), TABLES)
            wrong = score_manifest(gold)
            wrong["gold_counts"].pop("keywords_window")
            with self.assertRaises(ValueError):
                validate_gold_provenance(gold, wrong, TABLES)
            wrong = score_manifest(gold)
            wrong["gold_path"] = str(Path(root) / "other")
            with self.assertRaises(ValueError):
                validate_gold_provenance(gold, wrong, TABLES)


if __name__ == "__main__":
    unittest.main()
