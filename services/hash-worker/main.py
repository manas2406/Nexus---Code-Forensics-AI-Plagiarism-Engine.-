"""Nexus Hash Worker — Phase 2 + Phase 3 entry point.

Usage:
    python main.py --zip <bucket>/<object_key> --job-id <uuid>   # Phase 2 mode
    python main.py --kafka                                        # Phase 3 mode

Phase 2 (--zip):
    Reads a ZIP from MinIO, processes every .cpp/.h file through the
    AST → Winnowing → LSH → Jaccard pipeline, writes results to MinIO
    and job status to Redis.

Phase 3 (--kafka):
    Consumes JOB_CREATED events from the Kafka 'submissions' topic,
    runs the full pipeline, produces suspicious pairs to 'suspicious-pairs',
    produces JOB_COMPLETE to 'results', and routes failures to the DLQ.

State transitions published to Redis throughout:
    PENDING → EXTRACTING → PARSING → HASHING → COMPARING → COMPLETE (or FAILED)
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import signal
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import FrameType

from dotenv import load_dotenv

from comparator import compare_candidates
from lsh_index import LSHIndex
from minio_client import MinIOClient
from pipeline import process_batch
from state import JobStateManager, JobStatus

# Load .env from repo root (two levels up from services/hash-worker/)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")

# Add shared/ to sys.path for schemas import
sys.path.insert(0, str(_REPO_ROOT / "shared"))
from schemas import SuspiciousPairEvent, extracted_file_key

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("nexus.hash-worker")


def _build_minio_client() -> MinIOClient:
    """Construct MinIOClient from environment variables."""
    endpoint = os.getenv("MINIO_ENDPOINT", "localhost")
    port = os.getenv("MINIO_PORT", "9000")
    # If endpoint already contains a port, use as-is
    full_endpoint = endpoint if ":" in endpoint else f"{endpoint}:{port}"

    access_key = os.getenv(
        "MINIO_ROOT_USER", os.getenv("MINIO_ACCESS_KEY", "nexus"),
    )
    secret_key = os.getenv(
        "MINIO_ROOT_PASSWORD",
        os.getenv("MINIO_SECRET_KEY", "nexus-secret-change-in-prod"),
    )
    secure = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

    return MinIOClient(
        endpoint=full_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )


def _build_state_manager() -> JobStateManager:
    """Construct JobStateManager from environment variables."""
    redis_url = os.getenv("REDIS_URL")
    if redis_url is None:
        host = os.getenv("REDIS_HOST", "localhost")
        port = os.getenv("REDIS_PORT", "6379")
        redis_url = f"redis://{host}:{port}"
    return JobStateManager(redis_url=redis_url)


class PipelineError(Exception):
    """Raised for unrecoverable errors that should fail the entire job."""
    pass


def run_pipeline(
    job_id: str,
    bucket: str,
    object_key: str,
    threshold: float = 0.6,
    lsh_threshold: float = 0.5,
    lsh_num_perm: int = 128,
    max_file_bytes: int = 500_000,
    minio: MinIOClient | None = None,
    state: JobStateManager | None = None,
) -> list[SuspiciousPairEvent]:
    """Full pipeline: MinIO ZIP → suspicious pairs list.

    State transitions published to Redis throughout:
      PENDING → EXTRACTING → PARSING → HASHING → COMPARING → COMPLETE (or FAILED)

    Returns list of SuspiciousPairEvent dataclasses.
    Raises PipelineError on unrecoverable failure.
    """
    t_start = time.perf_counter()

    if minio is None:
        minio = _build_minio_client()
    if state is None:
        state = _build_state_manager()

    logger.info(
        "Pipeline starting | job=%s | bucket=%s | key=%s | threshold=%.2f",
        job_id, bucket, object_key, threshold,
    )

    def _transition(status: JobStatus, progress: int, message: str) -> None:
        state.update_status(job_id, status, progress, message)
        logger.info("[%s] %s (%d%%) — %s", job_id[:8], status.value, progress, message)

    # ── PENDING ─────────────────────────────────────────────────────
    _transition(JobStatus.PENDING, 0, "Job received")

    try:
        # ── EXTRACTING ──────────────────────────────────────────────
        _transition(JobStatus.EXTRACTING, 2, "Downloading ZIP from MinIO...")

        files: dict[str, str] = {}
        try:
            for entry in minio.stream_cpp_files(
                bucket, object_key, max_file_bytes=max_file_bytes,
            ):
                files[entry.filename] = entry.source_code
                logger.debug("  Found: %s (%d bytes)", entry.filename, entry.size_bytes)
        except Exception as exc:
            raise PipelineError(f"Failed to read ZIP {bucket}/{object_key}: {exc}") from exc

        if not files:
            raise PipelineError(
                f"ZIP contains zero processable C++ files in {bucket}/{object_key}"
            )

        n = len(files)
        logger.info("Extracted %d C++ files from ZIP", n)

        # ── PARSING ─────────────────────────────────────────────────
        _transition(JobStatus.PARSING, 10, f"Parsing {n} C++ files...")

        batch = process_batch(files, max_bytes=max_file_bytes)

        logger.info(
            "Parsing complete: %d/%d files yielded tokens (%d skipped)",
            batch.processed, batch.total_files, len(batch.skipped),
        )

        if batch.processed < 2:
            raise PipelineError(
                f"Need at least 2 parseable files for comparison. "
                f"Got {batch.processed} parseable, {len(batch.skipped)} skipped."
            )

        # Log partial parse info
        for fname, result in batch.parse_results.items():
            if result.had_errors:
                logger.info("Partial parse: %s (had ERROR nodes)", fname)

        # ── HASHING ─────────────────────────────────────────────────
        _transition(
            JobStatus.HASHING, 40,
            f"Computing Winnowing fingerprints for {batch.processed} files...",
        )

        avg_fp = (
            sum(len(fp) for fp in batch.fingerprints.values()) / len(batch.fingerprints)
            if batch.fingerprints
            else 0
        )
        logger.info(
            "Fingerprints computed | files=%d | avg_size=%.1f",
            len(batch.fingerprints), avg_fp,
        )

        # ── COMPARING (LSH + Jaccard) ──────────────────────────────
        total_possible = batch.processed * (batch.processed - 1) // 2

        _transition(
            JobStatus.COMPARING, 55,
            f"Building LSH index for {batch.processed} files "
            f"({total_possible:,} possible pairs)...",
        )

        lsh = LSHIndex(threshold=lsh_threshold, num_perm=lsh_num_perm)
        candidates = lsh.get_all_candidates(batch.fingerprints)

        logger.info(
            "LSH pre-filter: %d/%d pairs are candidates (%.1f%% reduction)",
            len(candidates), total_possible,
            (1 - len(candidates) / max(total_possible, 1)) * 100,
        )

        _transition(
            JobStatus.COMPARING, 65,
            f"Running Jaccard similarity on {len(candidates)} candidate pairs...",
        )

        suspicious_pairs_raw = compare_candidates(
            candidates, batch.fingerprints, threshold=threshold,
        )

        # Convert to SuspiciousPairEvent schema
        suspicious_pairs: list[SuspiciousPairEvent] = []
        for pair in suspicious_pairs_raw:
            event = SuspiciousPairEvent.create(
                job_id=job_id,
                file_a=pair.file_a,
                file_b=pair.file_b,
                score=pair.similarity,
                fp_size_a=len(batch.fingerprints.get(pair.file_a, set())),
                fp_size_b=len(batch.fingerprints.get(pair.file_b, set())),
            )
            suspicious_pairs.append(event)
            logger.info(
                "🚨 SUSPICIOUS  %s ↔ %s  score=%.4f",
                pair.file_a, pair.file_b, pair.similarity,
            )

        logger.info("Found %d suspicious pairs", len(suspicious_pairs))

        # ── Build result JSON ───────────────────────────────────────
        elapsed = time.perf_counter() - t_start

        result: dict[str, object] = {
            "jobId": job_id,
            "totalFiles": batch.processed,
            "skipped": batch.skipped,
            "suspiciousPairs": [dataclasses.asdict(p) for p in suspicious_pairs],
            "lshCandidates": len(candidates),
            "totalPossiblePairs": total_possible,
            "elapsedSeconds": round(elapsed, 3),
            "completedAt": datetime.now(timezone.utc).isoformat(),
        }

        # Upload result JSON to MinIO
        result_key = f"results/{job_id}.json"
        minio.put_json(bucket=bucket, object_key=result_key, data=result)
        logger.info("Result uploaded to %s/%s", bucket, result_key)

        # Store pair IDs in Redis for AI worker
        if suspicious_pairs:
            state.set_job_pairs(job_id, [p.pairId for p in suspicious_pairs])

        # ── COMPLETE ────────────────────────────────────────────────
        _transition(
            JobStatus.COMPLETE, 100,
            f"Complete | {batch.processed} files | "
            f"{len(suspicious_pairs)} suspicious pairs | "
            f"{elapsed:.2f}s",
        )

        logger.info(
            "Pipeline complete | job=%s | files=%d | pairs=%d | elapsed=%.3fs",
            job_id, batch.processed, len(suspicious_pairs), elapsed,
        )

        return suspicious_pairs

    except PipelineError as e:
        logger.error("Pipeline failed | job=%s | %s", job_id, e)
        _transition(JobStatus.FAILED, 0, f"Pipeline error: {e}")
        raise

    except Exception as e:
        tb = traceback.format_exc()
        logger.exception("Unexpected pipeline error | job=%s", job_id)
        _transition(JobStatus.FAILED, 0, f"Unexpected error: {type(e).__name__}: {e}")
        raise


# ── Kafka mode (Phase 3) ─────────────────────────────────────────────────────

# Global shutdown flag for SIGTERM handling
_shutdown = False


def _handle_sigterm(sig: int, frame: FrameType | None) -> None:
    """SIGTERM handler — sets shutdown flag for graceful drain."""
    global _shutdown
    _shutdown = True
    logging.info("[hash-worker] SIGTERM received — draining in-flight messages")


def run_kafka_worker() -> int:
    """Start the hash-worker in Kafka consumer mode.

    1. Load config from env
    2. Health check Kafka + MinIO + Redis — exit code 1 if any unreachable
    3. Register SIGTERM handler → sets a shutdown flag
    4. Build KafkaConfig, MinIOClient, JobStateManager
    5. Instantiate HashWorkerKafkaClient
    6. Start consuming
    """
    from handler import WorkerConfig, handle_job_created
    from kafka_client import HashWorkerKafkaClient, KafkaConfig

    # Register SIGTERM handler
    signal.signal(signal.SIGTERM, _handle_sigterm)

    # Load config
    broker = os.getenv("KAFKA_BROKERS", "localhost:9092")
    kafka_config = KafkaConfig(
        broker=broker,
        consumer_group="hash-workers",
        input_topic=os.getenv("KAFKA_INPUT_TOPIC", "submissions"),
        suspicious_pairs_topic=os.getenv(
            "KAFKA_SUSPICIOUS_PAIRS_TOPIC", "suspicious-pairs",
        ),
        results_topic=os.getenv("KAFKA_RESULTS_TOPIC", "forensic-results"),
        dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", "dead-letter"),
    )

    worker_config = WorkerConfig.from_env()
    minio_client = _build_minio_client()
    state_mgr = _build_state_manager()

    # Health checks
    if not minio_client.health_check():
        logger.error("MinIO is unreachable — aborting Kafka mode")
        return 1
    if not state_mgr.health_check():
        logger.error("Redis is unreachable — aborting Kafka mode")
        return 1

    # Build Kafka client (also validates broker connectivity)
    try:
        client = HashWorkerKafkaClient(kafka_config)
    except Exception as e:
        logger.error("Failed to connect to Kafka broker: %s", e)
        return 1

    if not client.health_check():
        logger.warning(
            "Kafka health check returned False — broker may not be fully ready, "
            "but continuing (consumer will retry on poll)"
        )

    logger.info(
        "[hash-worker] Kafka mode — listening on '%s' topic",
        kafka_config.input_topic,
    )

    # Wire the shutdown flag into the client
    def _check_shutdown() -> None:
        if _shutdown:
            client.shutdown = True

    # Start consuming
    try:
        # Periodic shutdown check — we hook into the client's poll loop
        # by setting the shutdown flag on the client instance
        import threading

        def _shutdown_watcher() -> None:
            """Background thread that propagates SIGTERM to the client."""
            while not _shutdown:
                import time as _time
                _time.sleep(0.5)
            client.shutdown = True

        watcher = threading.Thread(target=_shutdown_watcher, daemon=True)
        watcher.start()

        client.start(
            handler=lambda payload: handle_job_created(
                payload,
                kafka=client,
                state=state_mgr,
                minio=minio_client,
                config=worker_config,
            )
        )
    except KeyboardInterrupt:
        logger.info("[hash-worker] KeyboardInterrupt — shutting down")
    finally:
        client.close()

    logger.info("[hash-worker] Shutdown complete")
    return 0


# ── CLI entry point ──────────────────────────────────────────────────────────


def main() -> int:
    """CLI entry point for the Nexus plagiarism-detection pipeline."""
    parser = argparse.ArgumentParser(
        description="Nexus Hash Worker — standalone pipeline runner",
    )

    # Mode selection — mutually exclusive
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--kafka", action="store_true",
        help="Start in Kafka consumer mode (Phase 3)",
    )
    mode_group.add_argument(
        "--zip",
        help="<bucket>/<object_key> path to the ZIP in MinIO (Phase 2)",
    )

    parser.add_argument(
        "--job-id",
        help="Unique job identifier (UUID) — required with --zip",
    )
    parser.add_argument(
        "--threshold", type=float,
        default=None,
        help="Jaccard similarity threshold (default: env SUSPICIOUS_PAIR_THRESHOLD or 0.6)",
    )
    parser.add_argument(
        "--output", choices=["json", "table"],
        default="table",
        help="Output format (default: table)",
    )
    args = parser.parse_args()

    # ── Kafka mode ────────────────────────────────────────────────────────
    if args.kafka:
        return run_kafka_worker()

    # ── ZIP mode (Phase 2 — unchanged) ────────────────────────────────────
    if not args.job_id:
        print("ERROR: --job-id is required when using --zip mode", file=sys.stderr)
        return 1

    # Parse bucket / key
    zip_path: str = args.zip
    if "/" not in zip_path:
        print(f"ERROR: --zip must be <bucket>/<key>, got: {zip_path}", file=sys.stderr)
        return 1
    bucket, object_key = zip_path.split("/", 1)
    job_id: str = args.job_id

    # Config from environment
    threshold = args.threshold or float(os.getenv("SUSPICIOUS_PAIR_THRESHOLD", "0.6"))
    lsh_threshold = float(os.getenv("LSH_THRESHOLD", "0.5"))
    lsh_num_perm = int(os.getenv("LSH_NUM_PERM", "128"))
    max_file_bytes = int(os.getenv("MAX_FILE_BYTES", "500000"))

    # Build clients
    minio = _build_minio_client()
    state = _build_state_manager()

    # Health checks
    if not minio.health_check():
        print("ERROR: MinIO is unreachable", file=sys.stderr)
        return 1
    if not state.health_check():
        print("ERROR: Redis is unreachable", file=sys.stderr)
        return 1
    logger.info("Health checks passed — MinIO and Redis are reachable")

    try:
        pairs = run_pipeline(
            job_id=job_id,
            bucket=bucket,
            object_key=object_key,
            threshold=threshold,
            lsh_threshold=lsh_threshold,
            lsh_num_perm=lsh_num_perm,
            max_file_bytes=max_file_bytes,
            minio=minio,
            state=state,
        )

        if args.output == "json":
            output = [dataclasses.asdict(p) for p in pairs]
            print(json.dumps(output, indent=2))
        else:
            # Table output
            print(f"\n{'=' * 70}")
            print(f"  Nexus Pipeline — Job {job_id}")
            print(f"{'=' * 70}")
            if not pairs:
                print("  No suspicious pairs found.")
            else:
                print(f"\n{'Score':>6}  {'File A':<30}  {'File B':<30}")
                print("-" * 70)
                for p in sorted(pairs, key=lambda x: x.similarityScore, reverse=True):
                    print(f"{p.similarityScore:>6.3f}  {p.fileAName:<30}  {p.fileBName:<30}")
            print(f"{'=' * 70}\n")

        return 0

    except PipelineError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
