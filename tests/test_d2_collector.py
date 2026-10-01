import contextlib
from datetime import datetime, timedelta, timezone
import email.utils
import io
from http.client import IncompleteRead
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import collect_rss
from src.news_pipeline.rss import _retry_after, fetch_feed
from src.news_pipeline.writer_lock import single_writer


class D2CollectorTest(unittest.TestCase):
    def test_truncated_response_retries_but_never_returns_partial_body(self):
        response = unittest.mock.MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        response.headers = {}
        response.read.side_effect = [IncompleteRead(b"partial", 10), b"complete"]
        with patch("src.news_pipeline.rss.urlopen", return_value=response) as opened, patch("src.news_pipeline.rss.time.sleep"):
            self.assertEqual(fetch_feed("https://example.test/feed", {}, retries=1)["payload"], b"complete")
            self.assertEqual(opened.call_count, 2)
        response.read.side_effect = IncompleteRead(b"partial", 10)
        with patch("src.news_pipeline.rss.urlopen", return_value=response), patch("src.news_pipeline.rss.time.sleep"):
            with self.assertRaises(IncompleteRead):
                fetch_feed("https://example.test/feed", {}, retries=1)

    def test_content_history_does_not_recount_article_after_rss_rollover(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config.json"
            state = root / "state.json"
            lock = root / "collector.lock"
            evidence = root / "manifest.json"
            config.write_text(json.dumps([{"feed_id": "x_general", "source_id": "x", "feed_url": "https://example.test/home.rss", "timezone": "Asia/Ho_Chi_Minh"}]), encoding="utf-8")
            args = ["collect", "--config", str(config), "--state", str(state), "--lock", str(lock), "--raw-root", str(root / "raw"), "--evidence", str(evidence), "--request-gap", "0"]
            payloads = [
                b"<rss><channel><item><title>A</title><link>https://example.test/a</link></item></channel></rss>",
                b"<rss><channel><item><title>B</title><link>https://example.test/b</link></item></channel></rss>",
                b"<rss><channel><item><title>A</title><link>https://example.test/a</link></item></channel></rss>",
            ]
            counts = []
            for payload in payloads:
                with patch("sys.argv", args), patch.object(collect_rss, "fetch_feed", return_value={"status_code": 200, "payload": payload}), contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(collect_rss.main(), 0)
                counts.append(json.loads(evidence.read_text(encoding="utf-8"))["results"][0]["new_article_count"])
            saved = json.loads(state.read_text(encoding="utf-8"))["x_general"]
            self.assertEqual(counts, [1, 1, 0])
            self.assertEqual(len(saved["article_content_history"]), 2)
            self.assertEqual(len(saved["article_content_history"]["https://example.test/a"]), 1)

    def test_retry_uses_bounded_jittered_backoff(self):
        with patch("src.news_pipeline.rss.urlopen", side_effect=TimeoutError("slow")), patch("src.news_pipeline.rss.random.uniform", return_value=0.2), patch("src.news_pipeline.rss.time.sleep") as sleep:
            with self.assertRaises(TimeoutError):
                fetch_feed("https://example.test/feed", {}, retries=0, jitter=0.2)
        sleep.assert_not_called()

        with patch("src.news_pipeline.rss.urlopen", side_effect=[TimeoutError("slow"), TimeoutError("still slow")]), patch("src.news_pipeline.rss.random.uniform", return_value=0.2), patch("src.news_pipeline.rss.time.sleep") as sleep:
            with self.assertRaises(TimeoutError):
                fetch_feed("https://example.test/feed", {}, retries=1, jitter=0.2)
        sleep.assert_called_once_with(5.0)

    def test_retry_after_and_request_gap_keep_server_and_host_floors(self):
        self.assertEqual(_retry_after({"Retry-After": "120"}), 120.0)
        retry_at = email.utils.format_datetime(datetime.now(timezone.utc) + timedelta(seconds=120), usegmt=True)
        self.assertGreater(_retry_after({"Retry-After": retry_at}), 100.0)
        with patch("scripts.collect_rss.time.sleep") as sleep, patch("scripts.collect_rss.random.uniform", return_value=-0.2):
            collect_rss._sleep_gap(5.0, 0.2)
        sleep.assert_called_once_with(5.0)

    def test_collector_lock_blocks_overlapping_run(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            config = root / "config.json"
            state = root / "state.json"
            lock = root / "collector.lock"
            config.write_text(json.dumps([{"feed_id": "x_general", "source_id": "x", "feed_url": "https://example.test/home.rss", "timezone": "Asia/Ho_Chi_Minh"}]), encoding="utf-8")
            args = ["collect", "--config", str(config), "--state", str(state), "--lock", str(lock), "--raw-root", str(root / "rss"), "--evidence", str(root / "evidence.json"), "--request-gap", "0"]
            with single_writer(lock):
                with patch("sys.argv", args), patch.object(collect_rss, "fetch_feed") as fetch, contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(collect_rss.main(), 2)
                fetch.assert_not_called()


if __name__ == "__main__":
    unittest.main()
