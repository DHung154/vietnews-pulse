import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from src.news_pipeline.history import import_jsonl
from src.news_pipeline.rss import fetch_feed, parse_feed, parse_published_at, write_raw_batch


SOURCE = {"source_id": "tuoitre", "feed_url": "https://tuoitre.vn/home.rss", "timezone": "Asia/Ho_Chi_Minh"}


class M2RssTest(unittest.TestCase):
    def test_tuoitre_date_with_narrow_space_and_rfc_date(self):
        local = parse_published_at("9/15/2026 6:19:00\u202fPM", SOURCE["timezone"])
        self.assertEqual(local.isoformat(), "2026-09-15T11:19:00+00:00")
        rfc = parse_published_at("Tue, 15 Sep 2026 15:04:26 +0700", SOURCE["timezone"])
        self.assertEqual(rfc.isoformat(), "2026-09-15T08:04:26+00:00")

    def test_missing_description_is_kept_and_missing_link_quarantined(self):
        payload = b"""<rss><channel><item><title>A</title><link>https://example.test/a</link></item><item><title>B</title></item></channel></rss>"""
        observations, errors = parse_feed(payload, SOURCE, "batch")
        self.assertEqual(len(observations), 1)
        self.assertIsNone(observations[0]["description_raw"])
        self.assertEqual(errors[0]["reason"], "missing_title_or_link")

    def test_raw_publish_failure_does_not_replace_existing_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "batch.jsonl"
            target.write_text("old\n", encoding="utf-8")
            with self.assertRaises(FileExistsError):
                write_raw_batch([], target)
            self.assertEqual(target.read_text(encoding="utf-8"), "old\n")

    def test_atomic_raw_publish(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "batch.jsonl"
            result = write_raw_batch([{"x": 1}], target)
            self.assertEqual(result["records"], 1)
            self.assertEqual(json.loads(target.read_text(encoding="utf-8"))["x"], 1)

    def test_malformed_xml_is_not_silently_accepted(self):
        with self.assertRaises(Exception):
            parse_feed(b"<rss><channel>", SOURCE, "batch")

    def test_304_is_no_new_data(self):
        error = HTTPError("https://example.test/feed", 304, "not modified", {}, None)
        with patch("src.news_pipeline.rss.urlopen", side_effect=error):
            result = fetch_feed("https://example.test/feed", {"etag": "old"})
        self.assertEqual(result["status_code"], 304)
        self.assertEqual(result["payload"], b"")

    def test_timeout_retries_then_succeeds(self):
        class Response:
            status = 200
            headers = {}

            def read(self):
                return b"<rss/>"

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

        with patch("src.news_pipeline.rss.urlopen", side_effect=[TimeoutError("slow"), Response()]), patch("src.news_pipeline.rss.time.sleep"):
            result = fetch_feed("https://example.test/feed", {}, retries=1)
        self.assertEqual(result["status_code"], 200)

    def test_history_requires_explicit_mapping(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp = Path(temp_dir)
            input_path = temp / "history.jsonl"
            mapping_path = temp / "mapping.json"
            output_path = temp / "raw.jsonl"
            input_path.write_text('{"headline":"A","url":"https://example.test/a"}\n', encoding="utf-8")
            mapping_path.write_text(json.dumps({"title": "headline", "link": "url"}), encoding="utf-8")
            result = import_jsonl(input_path, mapping_path, output_path, "historical_fixture")
            self.assertEqual(result["records"], 1)
            self.assertIn("historical_fixture", output_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
