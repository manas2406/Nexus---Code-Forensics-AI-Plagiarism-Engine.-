"""Phase 2 test suite — MinIO streaming, Redis state, and full pipeline.

Unit tests (no infra):
    pytest test_phase2.py -m "not integration" -v

Integration tests (requires: docker compose up -d):
    pytest test_phase2.py -m integration -v
"""

from __future__ import annotations

import io
import json
import subprocess
import sys
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from minio_client import MinIOClient, ZipEntry
from state import JobStateManager, JobStatus, JOB_STATUS_TTL_SECONDS


# ─── Helpers ──────────────────────────────────────────────────────────


def _make_zip(files: dict[str, str | bytes]) -> bytes:
    """Create an in-memory ZIP archive from filename → content mapping."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            if isinstance(content, str):
                content = content.encode("utf-8")
            zf.writestr(name, content)
    return buf.getvalue()


def _mock_get_object(zip_bytes: bytes) -> MagicMock:
    """Create a mock MinIO get_object response returning *zip_bytes*."""
    response = MagicMock()
    response.read.return_value = zip_bytes
    response.close = MagicMock()
    response.release_conn = MagicMock()
    return response


# ─── Unit tests (no infra needed) ────────────────────────────────────


class TestZipEntrySkipsNonCpp:
    """Only C/C++ source files should be yielded from the ZIP."""

    def test_zip_entry_skips_non_cpp(self) -> None:
        zip_bytes = _make_zip({
            "readme.txt": "hello world",
            "script.py": "print('hi')",
            "solution.cpp": "int main() { return 0; }",
        })

        client = MinIOClient.__new__(MinIOClient)
        client._client = MagicMock()
        client._client.get_object.return_value = _mock_get_object(zip_bytes)

        entries = list(client.stream_cpp_files("test-bucket", "test.zip"))

        assert len(entries) == 1
        assert entries[0].filename == "solution.cpp"
        assert "int main()" in entries[0].source_code


class TestZipEntrySkipsOversized:
    """Files exceeding max_file_bytes should be skipped silently."""

    def test_zip_entry_skips_oversized(self) -> None:
        large_content = "int x = 0;\n" * 100_000  # ~1.1MB
        zip_bytes = _make_zip({"huge.cpp": large_content})

        client = MinIOClient.__new__(MinIOClient)
        client._client = MagicMock()
        client._client.get_object.return_value = _mock_get_object(zip_bytes)

        entries = list(
            client.stream_cpp_files("test-bucket", "test.zip", max_file_bytes=1000),
        )
        assert len(entries) == 0


class TestZipEntryExtendsFileTypes:
    """All C/C++ extensions should be accepted: .cpp, .h, .cc, .cxx, .hpp, .c."""

    def test_accepts_all_cpp_extensions(self) -> None:
        zip_bytes = _make_zip({
            "main.cpp":    "int main() {}",
            "header.h":    "#pragma once",
            "impl.cc":     "void foo() {}",
            "source.cxx":  "void bar() {}",
            "template.hpp": "template<typename T> class X {};",
            "legacy.c":     "int old_func() { return 0; }",
            "readme.txt":  "should be excluded",
        })

        client = MinIOClient.__new__(MinIOClient)
        client._client = MagicMock()
        client._client.get_object.return_value = _mock_get_object(zip_bytes)

        entries = list(client.stream_cpp_files("test-bucket", "test.zip"))
        filenames = {e.filename for e in entries}

        assert filenames == {"main.cpp", "header.h", "impl.cc", "source.cxx", "template.hpp", "legacy.c"}


class TestZipEntrySkipsMacOSMetadata:
    """macOS __MACOSX/ and ._ prefixed entries should be skipped."""

    def test_skips_macos_metadata(self) -> None:
        zip_bytes = _make_zip({
            "main.cpp":             "int main() {}",
            "__MACOSX/main.cpp":    "mac metadata",
            "__MACOSX/._main.cpp":  "more mac metadata",
        })

        client = MinIOClient.__new__(MinIOClient)
        client._client = MagicMock()
        client._client.get_object.return_value = _mock_get_object(zip_bytes)

        entries = list(client.stream_cpp_files("test-bucket", "test.zip"))
        filenames = [e.filename for e in entries]

        assert filenames == ["main.cpp"]


class TestStateKeyPattern:
    """Key written to Redis must use HSET on ``job:{job_id}:status``."""

    def test_state_key_pattern(self) -> None:
        mock_redis = MagicMock()
        with patch("state.redis_lib.from_url", return_value=mock_redis):
            manager = JobStateManager(redis_url="redis://localhost:6379")
            manager.update_status("test-123", JobStatus.PENDING)

            mock_redis.hset.assert_called_once()
            key_arg = mock_redis.hset.call_args[0][0]
            assert key_arg == "job:test-123:status"


class TestStateTTLIsSet:
    """``expire()`` must be called with the correct TTL after every ``hset()``."""

    def test_state_ttl_is_set(self) -> None:
        mock_redis = MagicMock()
        with patch("state.redis_lib.from_url", return_value=mock_redis):
            manager = JobStateManager(redis_url="redis://localhost:6379")
            manager.update_status("test-456", JobStatus.HASHING, progress=50, message="Computing")

            mock_redis.expire.assert_called_once_with("job:test-456:status", JOB_STATUS_TTL_SECONDS)


class TestStateProgressTracking:
    """update_status() must include progress and message in the hash."""

    def test_state_progress_tracking(self) -> None:
        mock_redis = MagicMock()
        with patch("state.redis_lib.from_url", return_value=mock_redis):
            manager = JobStateManager(redis_url="redis://localhost:6379")
            manager.update_status(
                "test-prog", JobStatus.PARSING, progress=25, message="Parsing file 5 of 20",
            )

            # Check the mapping passed to hset
            call_kwargs = mock_redis.hset.call_args
            mapping = call_kwargs[1]["mapping"] if "mapping" in call_kwargs[1] else call_kwargs[0][1]
            assert mapping["status"] == "PARSING"
            assert mapping["progress"] == "25"
            assert mapping["message"] == "Parsing file 5 of 20"


class TestStatePubSubOnUpdate:
    """Every update_status() call should publish to Pub/Sub."""

    def test_pubsub_fires_on_update(self) -> None:
        mock_redis = MagicMock()
        with patch("state.redis_lib.from_url", return_value=mock_redis):
            manager = JobStateManager(redis_url="redis://localhost:6379")
            manager.update_status("test-pub", JobStatus.EXTRACTING, progress=5, message="Downloading")

            mock_redis.publish.assert_called_once()
            channel, data = mock_redis.publish.call_args[0]
            assert channel == "job:test-pub:events"

            parsed = json.loads(data)
            assert parsed["jobId"] == "test-pub"
            assert parsed["status"] == "EXTRACTING"
            assert parsed["progress"] == 5


class TestStatePublishIsJson:
    """``publish()`` must be called with a valid JSON string payload."""

    def test_state_publish_is_json(self) -> None:
        mock_redis = MagicMock()
        with patch("state.redis_lib.from_url", return_value=mock_redis):
            manager = JobStateManager(redis_url="redis://localhost:6379")
            payload = {"status": "COMPLETE", "jobId": "test-789"}
            manager.publish_event("test-789", payload)

            mock_redis.publish.assert_called_once()
            channel, data = mock_redis.publish.call_args[0]
            assert channel == "job:test-789:events"
            parsed = json.loads(data)  # Must not raise
            assert parsed["jobId"] == "test-789"


class TestSetJobPairs:
    """set_job_pairs() must store pair IDs in a Redis List."""

    def test_set_job_pairs(self) -> None:
        mock_redis = MagicMock()
        with patch("state.redis_lib.from_url", return_value=mock_redis):
            manager = JobStateManager(redis_url="redis://localhost:6379")
            manager.set_job_pairs("test-pairs", ["pair-a", "pair-b", "pair-c"])

            mock_redis.delete.assert_called_once_with("job:test-pairs:pairs")
            mock_redis.rpush.assert_called_once_with(
                "job:test-pairs:pairs", "pair-a", "pair-b", "pair-c",
            )


class TestJobStatusEnum:
    """JobStatus must include all state machine transitions."""

    def test_all_statuses_present(self) -> None:
        expected = {"PENDING", "EXTRACTING", "PARSING", "HASHING", "COMPARING", "AI_ANALYSIS", "COMPLETE", "FAILED"}
        actual = {s.value for s in JobStatus}
        assert actual == expected


# ─── Integration tests (infra required) ──────────────────────────────


@pytest.mark.integration
class TestMinIOHealthCheck:
    """MinIOClient.health_check() returns True when MinIO is up."""

    def test_minio_health_check(self) -> None:
        client = MinIOClient(
            endpoint="localhost:9000",
            access_key="nexus",
            secret_key="nexus-secret-change-in-prod",
            secure=False,
        )
        assert client.health_check() is True


@pytest.mark.integration
class TestRedisHealthCheck:
    """JobStateManager.health_check() returns True when Redis is up."""

    def test_redis_health_check(self) -> None:
        manager = JobStateManager(redis_url="redis://localhost:6379")
        assert manager.health_check() is True


@pytest.mark.integration
class TestRedisStateRoundTrip:
    """Write status → read it back → verify all fields."""

    def test_redis_state_round_trip(self) -> None:
        import redis as redis_lib

        manager = JobStateManager(redis_url="redis://localhost:6379")
        job_id = "test-roundtrip-001"

        try:
            manager.update_status(job_id, JobStatus.COMPARING, progress=75, message="Running Jaccard")
            result = manager.get_status(job_id)

            assert result is not None
            assert result["status"] == "COMPARING"
            assert result["progress"] == "75"
            assert result["message"] == "Running Jaccard"
            assert "updated_at" in result

            # TTL should be set
            r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
            ttl = r.ttl(f"job:{job_id}:status")
            assert ttl > 0, f"TTL should be positive, got {ttl}"
        finally:
            r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
            r.delete(f"job:{job_id}:status")


@pytest.mark.integration
class TestRedisJobPairsRoundTrip:
    """Write pair IDs → read them back."""

    def test_redis_job_pairs_round_trip(self) -> None:
        import redis as redis_lib

        manager = JobStateManager(redis_url="redis://localhost:6379")
        job_id = "test-pairs-001"

        try:
            manager.set_job_pairs(job_id, ["pair-aaa", "pair-bbb", "pair-ccc"])
            result = manager.get_job_pairs(job_id)
            assert result == ["pair-aaa", "pair-bbb", "pair-ccc"]
        finally:
            r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
            r.delete(f"job:{job_id}:pairs")


@pytest.mark.integration
class TestFullPipelineOnRealZip:
    """End-to-end: create ZIP → upload → run pipeline → verify results."""

    def test_full_pipeline_on_real_zip(self) -> None:
        import redis as redis_lib
        from minio import Minio

        # 3 plagiarism pairs (6 files total)
        cpp_files = {
            "pair1_orig.cpp": (
                "int solve(int n) {\n"
                "    int result = 0;\n"
                "    for (int i = 0; i < n; i++) {\n"
                "        result += i * i;\n"
                "        if (i % 2 == 0) { result -= i; }\n"
                "    }\n"
                "    int final_val = result + n;\n"
                "    for (int j = 0; j < n; j++) { final_val += j; }\n"
                "    return final_val;\n"
                "}\n"
            ),
            "pair1_clone.cpp": (
                "int solve(int count) {\n"
                "    int total = 0;\n"
                "    for (int idx = 0; idx < count; idx++) {\n"
                "        total += idx * idx;\n"
                "        if (idx % 2 == 0) { total -= idx; }\n"
                "    }\n"
                "    int answer = total + count;\n"
                "    for (int k = 0; k < count; k++) { answer += k; }\n"
                "    return answer;\n"
                "}\n"
            ),
            "pair2_orig.cpp": (
                "int sort_arr(int arr[], int size) {\n"
                "    for (int i = 0; i < size - 1; i++) {\n"
                "        for (int j = 0; j < size - i - 1; j++) {\n"
                "            if (arr[j] > arr[j + 1]) {\n"
                "                int temp = arr[j];\n"
                "                arr[j] = arr[j + 1];\n"
                "                arr[j + 1] = temp;\n"
                "            }\n"
                "        }\n"
                "    }\n"
                "    return 0;\n"
                "}\n"
            ),
            "pair2_clone.cpp": (
                "int sort_arr(int data[], int length) {\n"
                "    for (int x = 0; x < length - 1; x++) {\n"
                "        for (int y = 0; y < length - x - 1; y++) {\n"
                "            if (data[y] > data[y + 1]) {\n"
                "                int tmp = data[y];\n"
                "                data[y] = data[y + 1];\n"
                "                data[y + 1] = tmp;\n"
                "            }\n"
                "        }\n"
                "    }\n"
                "    return 0;\n"
                "}\n"
            ),
            "pair3_orig.cpp": (
                "int fibonacci(int n) {\n"
                "    if (n <= 0) return 0;\n"
                "    if (n == 1) return 1;\n"
                "    int a = 0, b = 1;\n"
                "    for (int i = 2; i <= n; i++) {\n"
                "        int c = a + b; a = b; b = c;\n"
                "    }\n"
                "    return b;\n"
                "}\n"
                "int factorial(int n) {\n"
                "    int result = 1;\n"
                "    for (int i = 2; i <= n; i++) { result *= i; }\n"
                "    return result;\n"
                "}\n"
            ),
            "pair3_clone.cpp": (
                "int fibonacci(int num) {\n"
                "    if (num <= 0) return 0;\n"
                "    if (num == 1) return 1;\n"
                "    int prev = 0, curr = 1;\n"
                "    for (int step = 2; step <= num; step++) {\n"
                "        int next = prev + curr; prev = curr; curr = next;\n"
                "    }\n"
                "    return curr;\n"
                "}\n"
                "int factorial(int val) {\n"
                "    int product = 1;\n"
                "    for (int k = 2; k <= val; k++) { product *= k; }\n"
                "    return product;\n"
                "}\n"
            ),
        }

        # Create and upload ZIP
        zip_bytes = _make_zip(cpp_files)
        minio_client = Minio(
            "localhost:9000",
            access_key="nexus",
            secret_key="nexus-secret-change-in-prod",
            secure=False,
        )
        bucket = "nexus-submissions"
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)

        object_key = "test-phase2.zip"
        minio_client.put_object(
            bucket_name=bucket,
            object_name=object_key,
            data=io.BytesIO(zip_bytes),
            length=len(zip_bytes),
            content_type="application/zip",
        )

        job_id = "test-phase2-001"

        try:
            # Run the pipeline as a subprocess
            result = subprocess.run(
                [
                    sys.executable, "main.py",
                    "--zip", f"{bucket}/{object_key}",
                    "--job-id", job_id,
                ],
                capture_output=True,
                text=True,
                timeout=60,
                env={
                    **dict(__import__("os").environ),
                    "MINIO_ENDPOINT": "localhost:9000",
                    "MINIO_ACCESS_KEY": "nexus",
                    "MINIO_SECRET_KEY": "nexus-secret-change-in-prod",
                    "REDIS_HOST": "localhost",
                    "REDIS_PORT": "6379",
                },
            )

            assert result.returncode == 0, (
                f"Pipeline exited with code {result.returncode}\n"
                f"STDOUT:\n{result.stdout}\n"
                f"STDERR:\n{result.stderr}"
            )

            # Verify result JSON in MinIO
            response = minio_client.get_object(bucket, f"results/{job_id}.json")
            try:
                result_data = json.loads(response.read().decode("utf-8"))
            finally:
                response.close()
                response.release_conn()

            assert result_data["totalFiles"] == 6, (
                f"Expected 6 total files, got {result_data['totalFiles']}"
            )
            assert len(result_data["suspiciousPairs"]) >= 3, (
                f"Expected >= 3 suspicious pairs, got {len(result_data['suspiciousPairs'])}"
            )
            for pair in result_data["suspiciousPairs"]:
                assert pair["similarityScore"] >= 0.5, (
                    f"Pair {pair['pairId']} similarity {pair['similarityScore']} < 0.5"
                )
                # Verify SuspiciousPairEvent schema fields
                assert "schemaVersion" in pair
                assert "eventType" in pair
                assert pair["eventType"] == "SUSPICIOUS_PAIR"
                assert pair["fileAObjectKey"].startswith("extracted/")

            # Verify Redis status (now HGETALL instead of GET)
            r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
            status_data = r.hgetall(f"job:{job_id}:status")
            assert status_data, "Redis status hash not found"
            assert status_data["status"] == "COMPLETE"
            assert status_data["progress"] == "100"

            # Verify job pairs stored
            pairs = r.lrange(f"job:{job_id}:pairs", 0, -1)
            assert len(pairs) >= 3, f"Expected >= 3 pair IDs, got {len(pairs)}"

        finally:
            # Cleanup
            try:
                minio_client.remove_object(bucket, object_key)
            except Exception:
                pass
            try:
                minio_client.remove_object(bucket, f"results/{job_id}.json")
            except Exception:
                pass
            try:
                r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
                r.delete(f"job:{job_id}:status")
                r.delete(f"job:{job_id}:pairs")
            except Exception:
                pass


@pytest.mark.integration
class TestPipelineFailsGracefullyOnMissingZip:
    """Pipeline must exit 1 and set FAILED status for a non-existent ZIP."""

    def test_pipeline_fails_gracefully_on_missing_zip(self) -> None:
        import redis as redis_lib

        job_id = "test-missing-zip-001"

        result = subprocess.run(
            [
                sys.executable, "main.py",
                "--zip", "nexus-submissions/does-not-exist.zip",
                "--job-id", job_id,
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env={
                **dict(__import__("os").environ),
                "MINIO_ENDPOINT": "localhost:9000",
                "MINIO_ACCESS_KEY": "nexus",
                "MINIO_SECRET_KEY": "nexus-secret-change-in-prod",
                "REDIS_HOST": "localhost",
                "REDIS_PORT": "6379",
            },
        )

        assert result.returncode == 1, (
            f"Expected exit code 1, got {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

        # Verify Redis status is FAILED (now HGETALL)
        r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
        status_data = r.hgetall(f"job:{job_id}:status")
        assert status_data, "Redis status hash not found after failure"
        assert status_data["status"] == "FAILED"

        # Cleanup
        r.delete(f"job:{job_id}:status")
