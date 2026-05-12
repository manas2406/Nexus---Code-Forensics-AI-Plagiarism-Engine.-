"""Phase 3 test suite — Kafka integration, handler, retry logic, SIGTERM.

Unit tests (no infra):
    pytest test_phase3.py -m "not integration" -v

Integration tests (requires: docker compose up -d):
    pytest test_phase3.py -m integration -v
"""

from __future__ import annotations

import asyncio
import base64
import json
import signal
import threading
import time
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from handler import WorkerConfig, handle_job_created
from minio_client import ZipEntry
from state import JobStatus


# ─── Unit tests (no infra) ───────────────────────────────────────────


class TestHandleJobCreatedMissingJobId:
    """Payload without jobId must raise ValueError immediately."""

    @pytest.mark.asyncio
    async def test_handle_job_created_missing_job_id(
        self, mock_kafka_client: MagicMock, mock_state_manager: MagicMock,
        mock_minio_client: MagicMock,
    ) -> None:
        payload: dict[str, object] = {
            "submissionZipKey": "test.zip",
            "createdAt": "2026-01-01T00:00:00Z",
        }
        config = WorkerConfig()

        with pytest.raises(ValueError, match="jobId"):
            await handle_job_created(
                payload, kafka=mock_kafka_client, state=mock_state_manager,
                minio=mock_minio_client, config=config,
            )

        mock_state_manager.update_status.assert_not_called()


class TestHandleJobCreatedMissingZipKey:
    """Payload without submissionZipKey must raise ValueError."""

    @pytest.mark.asyncio
    async def test_handle_job_created_missing_zip_key(
        self, mock_kafka_client: MagicMock, mock_state_manager: MagicMock,
        mock_minio_client: MagicMock,
    ) -> None:
        payload: dict[str, object] = {
            "jobId": "test-job-001",
            "createdAt": "2026-01-01T00:00:00Z",
        }
        config = WorkerConfig()

        with pytest.raises(ValueError, match="submissionZipKey"):
            await handle_job_created(
                payload, kafka=mock_kafka_client, state=mock_state_manager,
                minio=mock_minio_client, config=config,
            )


class TestHandleJobCreatedEmptyZip:
    """Empty ZIP must set FAILED status and raise ValueError."""

    @pytest.mark.asyncio
    async def test_handle_job_created_empty_zip(
        self, mock_kafka_client: MagicMock, mock_state_manager: MagicMock,
        valid_job_payload: dict[str, str],
    ) -> None:
        empty_minio = MagicMock()
        empty_minio.stream_cpp_files = MagicMock(return_value=iter([]))
        empty_minio.put_json = MagicMock()

        config = WorkerConfig()

        with pytest.raises(ValueError, match="No .cpp files"):
            await handle_job_created(
                valid_job_payload, kafka=mock_kafka_client,
                state=mock_state_manager, minio=empty_minio, config=config,
            )

        # Should have called FAILED
        failed_calls = [
            c for c in mock_state_manager.update_status.call_args_list
            if c[0][1] == JobStatus.FAILED
        ]
        assert len(failed_calls) == 1


class TestHandleJobCreatedProducesPairs:
    """Handler must call produce_suspicious_pair for each suspicious pair."""

    @pytest.mark.asyncio
    async def test_handle_job_created_produces_pairs(
        self, mock_kafka_client: MagicMock, mock_state_manager: MagicMock,
        mock_minio_client: MagicMock, valid_job_payload: dict[str, str],
    ) -> None:
        config = WorkerConfig(threshold=0.0)  # Low threshold to catch all pairs

        await handle_job_created(
            valid_job_payload, kafka=mock_kafka_client,
            state=mock_state_manager, minio=mock_minio_client, config=config,
        )

        # With 2 files and threshold=0.0, we should get at least 1 pair
        assert mock_kafka_client.produce_suspicious_pair.call_count >= 1

        # Verify schema of produced pairs
        for c in mock_kafka_client.produce_suspicious_pair.call_args_list:
            pair = c[0][0]
            assert "jobId" in pair
            assert "pairId" in pair
            assert "fileA" in pair
            assert "fileB" in pair
            assert "similarity" in pair
            assert pair["jobId"] == "test-job-001"


class TestHandleJobCreatedProducesJobComplete:
    """produce_job_complete must be called exactly once with correct key."""

    @pytest.mark.asyncio
    async def test_handle_job_created_produces_job_complete(
        self, mock_kafka_client: MagicMock, mock_state_manager: MagicMock,
        mock_minio_client: MagicMock, valid_job_payload: dict[str, str],
    ) -> None:
        config = WorkerConfig()

        await handle_job_created(
            valid_job_payload, kafka=mock_kafka_client,
            state=mock_state_manager, minio=mock_minio_client, config=config,
        )

        mock_kafka_client.produce_job_complete.assert_called_once()
        args = mock_kafka_client.produce_job_complete.call_args[0]
        assert args[0] == "test-job-001"
        assert args[1] == "results/test-job-001.json"


class TestRetryLogicRetriesOnTransientFailure:
    """Handler retried on first 2 failures, succeeds on 3rd."""

    def test_retry_logic_retries_on_transient_failure(self) -> None:
        from kafka_client import HashWorkerKafkaClient, KafkaConfig

        call_count = 0

        async def flaky_handler(payload: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("transient failure")

        config = KafkaConfig(
            broker="localhost:9092",
            consumer_group="hash-workers",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            max_retries=3,
            retry_backoff_seconds=0.01,  # Fast for testing
        )

        client = HashWorkerKafkaClient.__new__(HashWorkerKafkaClient)
        client._config = config
        client._consumer = MagicMock()
        client._producer = MagicMock()
        client._shutdown = False

        loop = asyncio.new_event_loop()
        try:
            payload = {"jobId": "test"}
            raw = json.dumps(payload).encode("utf-8")
            client._process_with_retry(flaky_handler, payload, raw, loop)
        finally:
            loop.close()

        assert call_count == 3
        client._consumer.commit.assert_called_once()
        client._producer.send.assert_not_called()  # No DLQ


class TestRetryLogicRoutesToDlqAfterMaxRetries:
    """Handler always fails → DLQ after max_retries."""

    def test_retry_logic_routes_to_dlq_after_max_retries(self) -> None:
        from kafka_client import HashWorkerKafkaClient, KafkaConfig

        call_count = 0

        async def always_fail(payload: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("permanent failure")

        config = KafkaConfig(
            broker="localhost:9092",
            consumer_group="hash-workers",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            max_retries=3,
            retry_backoff_seconds=0.01,
        )

        client = HashWorkerKafkaClient.__new__(HashWorkerKafkaClient)
        client._config = config
        client._consumer = MagicMock()
        client._producer = MagicMock()
        client._shutdown = False

        loop = asyncio.new_event_loop()
        try:
            payload = {"jobId": "test"}
            raw = json.dumps(payload).encode("utf-8")
            client._process_with_retry(always_fail, payload, raw, loop)
        finally:
            loop.close()

        assert call_count == 3
        client._consumer.commit.assert_called_once()
        # DLQ message was sent
        assert client._producer.send.call_count == 1
        dlq_call = client._producer.send.call_args
        assert dlq_call[0][0] == "dead-letter"


class TestRetryLogicNoRetryOnValueError:
    """ValueError skips retries, goes straight to DLQ."""

    def test_retry_logic_no_retry_on_value_error(self) -> None:
        from kafka_client import HashWorkerKafkaClient, KafkaConfig

        call_count = 0

        async def value_error_handler(payload: dict[str, object]) -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError("bad payload")

        config = KafkaConfig(
            broker="localhost:9092",
            consumer_group="hash-workers",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            max_retries=3,
            retry_backoff_seconds=0.01,
        )

        client = HashWorkerKafkaClient.__new__(HashWorkerKafkaClient)
        client._config = config
        client._consumer = MagicMock()
        client._producer = MagicMock()
        client._shutdown = False

        loop = asyncio.new_event_loop()
        try:
            payload = {"jobId": "test"}
            raw = json.dumps(payload).encode("utf-8")
            client._process_with_retry(value_error_handler, payload, raw, loop)
        finally:
            loop.close()

        assert call_count == 1  # No retries
        client._consumer.commit.assert_called_once()
        assert client._producer.send.call_count == 1  # DLQ


class TestManualCommitOnlyOnSuccessOrDlq:
    """commit() must never be called mid-retry."""

    def test_manual_commit_only_on_success_or_dlq(self) -> None:
        from kafka_client import HashWorkerKafkaClient, KafkaConfig

        commit_times: list[int] = []
        attempt_count = 0

        original_commit = MagicMock()

        async def fail_twice(payload: dict[str, object]) -> None:
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                # Record that commit should NOT have been called yet
                assert original_commit.call_count == 0, (
                    f"commit() called during retry attempt {attempt_count}"
                )
                raise RuntimeError("transient")

        config = KafkaConfig(
            broker="localhost:9092",
            consumer_group="hash-workers",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            max_retries=3,
            retry_backoff_seconds=0.01,
        )

        client = HashWorkerKafkaClient.__new__(HashWorkerKafkaClient)
        client._config = config
        client._consumer = MagicMock()
        client._consumer.commit = original_commit
        client._producer = MagicMock()
        client._shutdown = False

        loop = asyncio.new_event_loop()
        try:
            payload = {"jobId": "test"}
            raw = json.dumps(payload).encode("utf-8")
            client._process_with_retry(fail_twice, payload, raw, loop)
        finally:
            loop.close()

        # Commit called exactly once — after success
        original_commit.assert_called_once()


class TestSigtermDrainsInflight:
    """SIGTERM mid-processing: current message completes, close() called."""

    def test_sigterm_drains_inflight(self) -> None:
        from kafka_client import HashWorkerKafkaClient, KafkaConfig

        processing_started = threading.Event()
        processing_done = threading.Event()

        async def slow_handler(payload: dict[str, object]) -> None:
            processing_started.set()
            # Simulate work
            await asyncio.sleep(0.1)
            processing_done.set()

        config = KafkaConfig(
            broker="localhost:9092",
            consumer_group="hash-workers",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            max_retries=3,
            retry_backoff_seconds=0.01,
            poll_timeout_ms=100,
        )

        client = HashWorkerKafkaClient.__new__(HashWorkerKafkaClient)
        client._config = config
        client._shutdown = False

        # Mock consumer to return one message then empty
        mock_message = MagicMock()
        mock_message.value = {"jobId": "test-sigterm"}

        mock_tp = MagicMock()
        client._consumer = MagicMock()
        client._consumer.poll = MagicMock(
            side_effect=[
                {mock_tp: [mock_message]},  # First poll: one message
            ]
        )
        client._consumer.commit = MagicMock()
        client._producer = MagicMock()

        def run_consumer() -> None:
            client.start(slow_handler)

        thread = threading.Thread(target=run_consumer)
        thread.start()

        # Wait for processing to start, then signal shutdown
        processing_started.wait(timeout=5)
        client._shutdown = True

        thread.join(timeout=10)

        # Message should have completed
        assert processing_done.is_set(), "In-flight message was not completed"
        client._consumer.commit.assert_called_once()


# ─── Integration tests ───────────────────────────────────────────────


@pytest.mark.integration
class TestKafkaHealthCheck:
    """HashWorkerKafkaClient.health_check() returns True when broker is up."""

    def test_kafka_health_check(self) -> None:
        from kafka_client import HashWorkerKafkaClient, KafkaConfig

        config = KafkaConfig(
            broker="localhost:9093",
            consumer_group="hash-workers-test",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
        )
        client = HashWorkerKafkaClient(config)
        try:
            assert client.health_check() is True
        finally:
            client.close()


@pytest.mark.integration
class TestFullKafkaFlow:
    """E2E: produce JOB_CREATED → worker processes → verify outputs."""

    def test_full_kafka_flow(self) -> None:
        import io
        import zipfile
        import redis as redis_lib
        from kafka import KafkaProducer as RawProducer
        from kafka import KafkaConsumer as RawConsumer
        from minio import Minio

        job_id = f"test-kafka-e2e-{int(time.time())}"
        broker = "localhost:9093"
        bucket = "nexus-submissions"

        # 1. Create and upload test ZIP to MinIO
        cpp_files = {
            "a.cpp": "int solve(int n) { int r=0; for(int i=0;i<n;i++){r+=i*i;} return r; }",
            "b.cpp": "int solve(int c) { int t=0; for(int j=0;j<c;j++){t+=j*j;} return t; }",
        }
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for name, code in cpp_files.items():
                zf.writestr(name, code)
        zip_bytes = buf.getvalue()

        minio_client = Minio(
            "localhost:9000", access_key="nexus",
            secret_key="nexus-secret-change-in-prod", secure=False,
        )
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)

        zip_key = f"test-{job_id}.zip"
        minio_client.put_object(
            bucket, zip_key, io.BytesIO(zip_bytes), len(zip_bytes),
            content_type="application/zip",
        )

        # 2. Produce JOB_CREATED to submissions topic
        producer = RawProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send("submissions", value={
            "jobId": job_id,
            "submissionZipKey": zip_key,
            "createdAt": "2026-01-01T00:00:00Z",
        })
        producer.flush()
        producer.close()

        # 3. Start kafka worker in background thread with timeout
        from kafka_client import HashWorkerKafkaClient, KafkaConfig
        from handler import WorkerConfig, handle_job_created
        from minio_client import MinIOClient
        from state import JobStateManager

        kafka_config = KafkaConfig(
            broker=broker, consumer_group=f"hash-workers-test-{job_id}",
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            poll_timeout_ms=500,
        )
        worker_config = WorkerConfig(bucket=bucket, threshold=0.0)
        minio_cl = MinIOClient(
            endpoint="localhost:9000", access_key="nexus",
            secret_key="nexus-secret-change-in-prod", secure=False,
        )
        state_mgr = JobStateManager(redis_url="redis://localhost:6379")
        client = HashWorkerKafkaClient(kafka_config)

        def run_worker() -> None:
            client.start(
                handler=lambda p: handle_job_created(
                    p, kafka=client, state=state_mgr,
                    minio=minio_cl, config=worker_config,
                )
            )

        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()

        # 4. Poll Redis for COMPLETE status
        r = redis_lib.from_url("redis://localhost:6379", decode_responses=True)
        deadline = time.time() + 15
        status = None
        while time.time() < deadline:
            status_data = r.hgetall(f"job:{job_id}:status")
            if status_data and status_data.get("status") == "COMPLETE":
                status = "COMPLETE"
                break
            time.sleep(0.5)

        client.shutdown = True
        worker_thread.join(timeout=5)
        client.close()

        # 5. Assertions
        try:
            assert status == "COMPLETE", f"Expected COMPLETE, got {status}"

            # Result JSON exists in MinIO
            response = minio_client.get_object(bucket, f"results/{job_id}.json")
            result_data = json.loads(response.read().decode("utf-8"))
            response.close()
            response.release_conn()
            assert result_data["jobId"] == job_id

        finally:
            # Cleanup
            try:
                minio_client.remove_object(bucket, zip_key)
            except Exception:
                pass
            try:
                minio_client.remove_object(bucket, f"results/{job_id}.json")
            except Exception:
                pass
            try:
                r.delete(f"job:{job_id}:status")
                r.delete(f"job:{job_id}:pairs")
            except Exception:
                pass


@pytest.mark.integration
class TestDlqRoutingOnBadPayload:
    """Malformed payload routes to DLQ topic."""

    def test_dlq_routing_on_bad_payload(self) -> None:
        from kafka import KafkaProducer as RawProducer
        from kafka import KafkaConsumer as RawConsumer
        from kafka_client import HashWorkerKafkaClient, KafkaConfig
        from handler import WorkerConfig, handle_job_created
        from minio_client import MinIOClient
        from state import JobStateManager

        broker = "localhost:9093"
        group = f"hash-workers-dlq-test-{int(time.time())}"

        # Produce a bad payload (missing jobId)
        producer = RawProducer(
            bootstrap_servers=broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        producer.send("submissions", value={"bad": "payload"})
        producer.flush()
        producer.close()

        kafka_config = KafkaConfig(
            broker=broker, consumer_group=group,
            input_topic="submissions",
            suspicious_pairs_topic="suspicious-pairs",
            results_topic="forensic-results",
            dlq_topic="dead-letter",
            poll_timeout_ms=500,
        )
        worker_config = WorkerConfig()
        minio_cl = MinIOClient(
            endpoint="localhost:9000", access_key="nexus",
            secret_key="nexus-secret-change-in-prod", secure=False,
        )
        state_mgr = JobStateManager(redis_url="redis://localhost:6379")
        client = HashWorkerKafkaClient(kafka_config)

        def run_worker() -> None:
            client.start(
                handler=lambda p: handle_job_created(
                    p, kafka=client, state=state_mgr,
                    minio=minio_cl, config=worker_config,
                )
            )

        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_thread.start()

        # Wait for processing, then stop
        time.sleep(3)
        client.shutdown = True
        worker_thread.join(timeout=5)
        client.close()

        # Check DLQ topic for the message
        dlq_consumer = RawConsumer(
            "dead-letter",
            bootstrap_servers=broker,
            group_id=f"dlq-checker-{int(time.time())}",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=5000,
        )

        found = False
        for msg in dlq_consumer:
            if "error" in msg.value and "originalPayload" in msg.value:
                found = True
                break

        dlq_consumer.close()
        assert found, "Bad payload was not routed to DLQ"
