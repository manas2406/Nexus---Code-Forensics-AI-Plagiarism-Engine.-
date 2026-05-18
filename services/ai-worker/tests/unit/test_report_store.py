"""
Unit tests for report_store.py — MinIO write + Redis publish.

All external dependencies (MinIO, Redis) are mocked.
Run with: pytest tests/unit/test_report_store.py -v
"""
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
from unittest.mock import MagicMock, patch


SAMPLE_REPORT = {
    "pairId":                "pair-abc",
    "similarityScore":       0.87,
    "obfuscationTechniques": ["VARIABLE_RENAMING", "LOOP_RESTRUCTURING"],
    "evidence":              [{"type": "VARIABLE_RENAMING", "description": "Renamed vars"}],
    "verdict":               "LIKELY_PLAGIARISM",
    "confidence":            0.92,
    "analystNotes":          "Clear variable renaming pattern detected.",
    "rawLlmResponse":        '{"verdict": "LIKELY_PLAGIARISM"}',
}


class TestStoreReport:

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_stores_json_at_correct_minio_path(self, mock_redis_fn, mock_minio_fn):
        """Report must land at reports/{jobId}/{pairId}.json in nexus-reports."""
        mock_minio = MagicMock()
        mock_minio_fn.return_value = mock_minio
        mock_redis_fn.return_value = MagicMock()

        result = _call_store("job-001", "pair-abc")

        assert result == "reports/job-001/pair-abc.json"
        mock_minio.put_object.assert_called_once()
        kwargs = mock_minio.put_object.call_args.kwargs
        assert kwargs["object_name"] == "reports/job-001/pair-abc.json"
        assert kwargs["bucket_name"] == "nexus-reports"
        assert kwargs["content_type"] == "application/json"

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_stored_json_is_valid_and_includes_metadata(self, mock_redis_fn, mock_minio_fn):
        """Stored bytes must be valid JSON with storedAt, objectKey, jobId added."""
        captured: dict = {}

        def _capture(bucket_name, object_name, data, length, **kwargs):
            captured["bytes"] = data.read()
            captured["length"] = length

        mock_minio = MagicMock()
        mock_minio.put_object.side_effect = _capture
        mock_minio_fn.return_value = mock_minio
        mock_redis_fn.return_value = MagicMock()

        _call_store("job-001", "pair-abc")

        stored = json.loads(captured["bytes"])
        assert stored["pairId"]    == "pair-abc"
        assert stored["verdict"]   == "LIKELY_PLAGIARISM"
        assert stored["jobId"]     == "job-001"         # metadata added
        assert stored["objectKey"] == "reports/job-001/pair-abc.json"
        assert "storedAt" in stored
        assert captured["length"]  == len(captured["bytes"])

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_redis_failure_does_not_raise(self, mock_redis_fn, mock_minio_fn):
        """Redis write is best-effort — failure must NOT propagate to caller."""
        mock_minio_fn.return_value = MagicMock()
        mock_redis = MagicMock()
        mock_redis.hset.side_effect = Exception("Redis connection refused")
        mock_redis_fn.return_value = mock_redis

        # Must NOT raise
        result = _call_store("job-001", "pair-abc")
        assert result == "reports/job-001/pair-abc.json"

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_minio_failure_raises(self, mock_redis_fn, mock_minio_fn):
        """MinIO write failure MUST propagate — caller handles retry logic."""
        mock_minio = MagicMock()
        mock_minio.put_object.side_effect = Exception("MinIO unavailable")
        mock_minio_fn.return_value = mock_minio
        mock_redis_fn.return_value = MagicMock()

        with pytest.raises(Exception, match="MinIO unavailable"):
            _call_store("job-001", "pair-abc")

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_redis_hset_called_with_correct_key(self, mock_redis_fn, mock_minio_fn):
        """Redis reference key pattern must be job:{jobId}:report:{pairId}."""
        mock_minio_fn.return_value = MagicMock()
        mock_redis = MagicMock()
        mock_redis_fn.return_value = mock_redis

        _call_store("job-001", "pair-abc")

        # hset should be called with the correct key
        call_args = mock_redis.hset.call_args
        assert call_args is not None
        key = call_args[0][0] if call_args[0] else call_args.kwargs.get("name", "")
        assert "job:job-001:report:pair-abc" in str(call_args)

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_redis_expire_set_for_report_key(self, mock_redis_fn, mock_minio_fn):
        """Redis key must have a TTL set (7-day expiry)."""
        mock_minio_fn.return_value = MagicMock()
        mock_redis = MagicMock()
        mock_redis_fn.return_value = mock_redis

        _call_store("job-001", "pair-abc")

        mock_redis.expire.assert_called()
        expire_call = mock_redis.expire.call_args_list[0]
        ttl = expire_call[0][1] if len(expire_call[0]) > 1 else expire_call[1].get("time", 0)
        assert ttl == 86_400 * 7, f"Expected 7-day TTL, got {ttl}"

    @patch("report_store.get_minio")
    @patch("report_store.get_redis")
    def test_redis_publish_called_for_report_event(self, mock_redis_fn, mock_minio_fn):
        """REPORT_READY event must be published to job:{jobId}:events channel."""
        mock_minio_fn.return_value = MagicMock()
        mock_redis = MagicMock()
        mock_redis_fn.return_value = mock_redis

        _call_store("job-001", "pair-abc")

        mock_redis.publish.assert_called()
        publish_call = mock_redis.publish.call_args
        channel = publish_call[0][0]
        assert channel == "job:job-001:events"

        payload = json.loads(publish_call[0][1])
        assert payload["jobId"]   == "job-001"
        assert payload["pairId"]  == "pair-abc"


class TestUpdateJobAiStatus:

    @patch("report_store.get_redis")
    def test_hset_called_with_correct_fields(self, mock_redis_fn):
        from report_store import update_job_ai_status
        mock_redis = MagicMock()
        mock_redis_fn.return_value = mock_redis

        update_job_ai_status("job-x", "AI_ANALYSIS", 50, "Half done")

        mock_redis.hset.assert_called_once()
        call_kwargs = mock_redis.hset.call_args.kwargs
        mapping = call_kwargs.get("mapping", {})
        assert mapping["status"]   == "AI_ANALYSIS"
        assert mapping["progress"] == "50"
        assert mapping["message"]  == "Half done"

    @patch("report_store.get_redis")
    def test_publish_called_for_status_update(self, mock_redis_fn):
        from report_store import update_job_ai_status
        mock_redis = MagicMock()
        mock_redis_fn.return_value = mock_redis

        update_job_ai_status("job-x", "COMPLETE", 100, "Done")

        mock_redis.publish.assert_called_once()
        channel, payload_str = mock_redis.publish.call_args[0]
        assert channel == "job:job-x:events"
        payload = json.loads(payload_str)
        assert payload["status"]   == "COMPLETE"
        assert payload["progress"] == 100

    @patch("report_store.get_redis")
    def test_redis_failure_is_non_fatal(self, mock_redis_fn):
        """Redis error during status update must not propagate."""
        from report_store import update_job_ai_status
        mock_redis = MagicMock()
        mock_redis.hset.side_effect = Exception("Redis down")
        mock_redis_fn.return_value = mock_redis

        # Must NOT raise
        update_job_ai_status("job-x", "AI_ANALYSIS", 10, "test")


# ── helpers ───────────────────────────────────────────────────────────────────

def _call_store(job_id: str, pair_id: str) -> str:
    from report_store import store_report
    return store_report(job_id, pair_id, dict(SAMPLE_REPORT))
