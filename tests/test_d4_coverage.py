import unittest

from src.news_pipeline.coverage import coverage_gate, profile_manifests


class CoverageGateTest(unittest.TestCase):
    def test_gate_filters_old_days_and_deduplicates_latest_and_same_slot_runs(self):
        sources = {
            "feed_a": {"source_id": "paper", "timezone": "Asia/Ho_Chi_Minh", "poll_seconds": 86400, "canonical_category": "business"}
        }
        manifests = [
            {"run_id": f"old_{day}", "timestamp_utc": f"2026-01-{day:02d}T01:00:00+00:00", "results": [{"feed_id": "feed_a", "status_code": 200, "records": 99, "last_attempt_at": f"2026-01-{day:02d}T01:00:00+00:00"}]}
            for day in range(1, 15)
        ]
        latest = {"run_id": "latest_a", "timestamp_utc": "2026-02-09T01:00:00+00:00", "results": [{"feed_id": "feed_a", "status_code": 200, "records": 7, "last_attempt_at": "2026-02-09T01:00:00+00:00"}]}
        manifests += [latest, latest.copy(), {"run_id": "latest_b", "timestamp_utc": "2026-02-09T01:00:00+00:00", "results": [{"feed_id": "feed_a", "status_code": 200, "records": 8, "last_attempt_at": "2026-02-09T01:00:00+00:00"}]}]
        result = profile_manifests(manifests, sources, required_days=14, end_local="2026-02-09")
        feed = result["feed_coverage"][0]
        self.assertEqual(result["coverage_status"], "incomplete")
        self.assertEqual(result["manifest_count_unique"], 16)
        self.assertEqual(feed["observed_days"], 1)
        self.assertEqual(feed["observed_poll_slots"], 1)
        self.assertEqual(feed["successful_poll_slots"], 1)
        self.assertEqual(feed["records"], 8)
        self.assertEqual(result["category_counts_by_local_day"], [{"day": "2026-02-09", "category": "business", "records": 8}])

    def test_short_capture_is_incomplete_and_keeps_observed_counts(self):
        sources = {
            "feed_a": {"source_id": "paper", "timezone": "Asia/Ho_Chi_Minh", "poll_seconds": 600, "canonical_category": "business"}
        }
        result = profile_manifests(
            [{"timestamp_utc": "2026-09-17T01:00:00+00:00", "feed_set_version": ["v1"], "results": [{"feed_id": "feed_a", "status_code": 200, "records": 7, "last_attempt_at": "2026-09-17T01:00:00+00:00", "canonical_category": "business"}]}],
            sources,
            required_days=14,
        )
        self.assertEqual(result["coverage_status"], "incomplete")
        self.assertEqual(result["category_counts_by_local_day"], [{"day": "2026-09-17", "category": "business", "records": 7}])
        self.assertEqual(result["feed_coverage"][0]["successful_poll_slots"], 1)
        self.assertEqual(result["cadence"]["run_count"], 1)

    def test_cadence_reports_large_gap_without_changing_coverage_counts(self):
        sources = {"feed_a": {"source_id": "paper", "timezone": "Asia/Ho_Chi_Minh", "poll_seconds": 600}}
        manifests = [
            {"run_id": "a", "timestamp_utc": "2026-09-17T00:00:00+00:00", "results": []},
            {"run_id": "b", "timestamp_utc": "2026-09-17T00:20:00+00:00", "results": []},
        ]
        result = profile_manifests(manifests, sources)
        self.assertEqual(result["cadence"]["run_count"], 2)
        self.assertEqual(result["cadence"]["gaps_over_threshold"], 1)

    def test_gate_requires_all_feeds_to_reach_window(self):
        result = coverage_gate(
            [{"observed_days": 14, "expected_poll_slots": 10, "success_rate": 1.0}, {"observed_days": 13, "expected_poll_slots": 10, "success_rate": 1.0}],
            required_days=14,
        )
        self.assertEqual(result["coverage_status"], "incomplete")


if __name__ == "__main__":
    unittest.main()
