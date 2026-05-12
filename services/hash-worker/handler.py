"""Kafka message handler — glue layer between infra and algorithm.

Receives a ``JOB_CREATED`` event payload and runs the full hash pipeline:
    validate → extract → parse → hash → compare → upload → publish

This module imports from both infrastructure (kafka_client, state, minio_client)
and algorithm modules (pipeline, lsh_index, comparator) — that is expected.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from comparator import compare_candidates
from lsh_index import LSHIndex
from minio_client import MinIOClient
from pipeline import process_batch
from state import JobStateManager, JobStatus

if TYPE_CHECKING:
    from kafka_client import HashWorkerKafkaClient

logger = logging.getLogger("nexus.handler")


@dataclass
class WorkerConfig:
    """Pipeline configuration — loaded from environment variables."""

    bucket: str = "nexus-submissions"
    threshold: float = 0.6
    lsh_threshold: float = 0.5
    lsh_num_perm: int = 128
    max_file_bytes: int = 500_000

    @classmethod
    def from_env(cls) -> WorkerConfig:
        """Build config from environment variables with sensible defaults."""
        return cls(
            bucket=os.getenv("MINIO_BUCKET_SUBMISSIONS", "nexus-submissions"),
            threshold=float(os.getenv("SUSPICIOUS_PAIR_THRESHOLD", "0.6")),
            lsh_threshold=float(os.getenv("LSH_THRESHOLD", "0.5")),
            lsh_num_perm=int(os.getenv("LSH_NUM_PERM", "128")),
            max_file_bytes=int(os.getenv("MAX_FILE_BYTES", "500000")),
        )


async def handle_job_created(
    payload: dict[str, object],
    kafka: HashWorkerKafkaClient,
    state: JobStateManager,
    minio: MinIOClient,
    config: WorkerConfig,
) -> None:
    """Process a JOB_CREATED event through the full hash pipeline.

    Expected payload schema::

        {
          "jobId": str,
          "submissionZipKey": str,   # e.g. "test.zip"
          "createdAt": str
        }

    State transitions:
        EXTRACTING → PARSING → HASHING → COMPARING → COMPLETE (or FAILED)

    Error handling:
        - ``ValueError`` (bad payload, empty ZIP) → re-raise immediately
        - MinIO connection errors → re-raise, let retry logic handle it
        - Errors after result upload → update_status(FAILED) before re-raise
        - Never calls update_status(FAILED) twice for the same job
    """
    t_start = time.perf_counter()
    failed = False  # Guard against double FAILED status

    # ── Step 1: Validate payload ─────────────────────────────────────
    job_id = payload.get("jobId")
    zip_key = payload.get("submissionZipKey")

    if not job_id or not isinstance(job_id, str):
        raise ValueError("Payload missing required field: jobId")
    if not zip_key or not isinstance(zip_key, str):
        raise ValueError("Payload missing required field: submissionZipKey")

    logger.info(
        "[handler] Processing JOB_CREATED | jobId=%s | zipKey=%s",
        job_id, zip_key,
    )

    try:
        # ── Step 2: Extract files from ZIP ───────────────────────────
        state.update_status(
            job_id, JobStatus.EXTRACTING, progress=2,
            message="Streaming ZIP from MinIO",
        )

        files: dict[str, str] = {}
        for entry in minio.stream_cpp_files(
            config.bucket, str(zip_key), max_file_bytes=config.max_file_bytes,
        ):
            files[entry.filename] = entry.source_code

        # ── Step 3: Validate files found ─────────────────────────────
        if not files:
            state.update_status(
                job_id, JobStatus.FAILED, progress=0,
                message="No .cpp files found in ZIP",
            )
            failed = True
            raise ValueError(f"No .cpp files in ZIP: {zip_key}")

        n = len(files)

        # ── Step 4: Parse files ──────────────────────────────────────
        state.update_status(
            job_id, JobStatus.PARSING, progress=10,
            message=f"{n} files found",
        )

        batch = process_batch(files, max_bytes=config.max_file_bytes)

        # ── Step 5: Hashing ──────────────────────────────────────────
        state.update_status(
            job_id, JobStatus.HASHING, progress=40,
            message=f"{batch.processed} parsed, {len(batch.skipped)} skipped",
        )

        # ── Step 6: LSH candidate detection ──────────────────────────
        lsh = LSHIndex(
            threshold=config.lsh_threshold,
            num_perm=config.lsh_num_perm,
        )
        candidates = lsh.get_all_candidates(batch.fingerprints)

        # ── Step 7: Compare candidates ───────────────────────────────
        state.update_status(
            job_id, JobStatus.COMPARING, progress=55,
            message=f"{len(candidates)} candidate pairs",
        )

        suspicious_pairs = compare_candidates(
            candidates, batch.fingerprints, threshold=config.threshold,
        )

        # ── Step 8: Build and upload result JSON ─────────────────────
        elapsed = time.perf_counter() - t_start
        total_possible = batch.processed * (batch.processed - 1) // 2

        result: dict[str, object] = {
            "jobId": job_id,
            "totalFiles": batch.processed,
            "skipped": batch.skipped,
            "suspiciousPairs": [
                {
                    "pairId": pair.pair_id,
                    "fileA": pair.file_a,
                    "fileB": pair.file_b,
                    "similarity": pair.similarity,
                }
                for pair in suspicious_pairs
            ],
            "lshCandidates": len(candidates),
            "totalPossiblePairs": total_possible,
            "elapsedSeconds": round(elapsed, 3),
            "completedAt": datetime.now(timezone.utc).isoformat(),
        }

        result_key = f"results/{job_id}.json"
        minio.put_json(bucket=config.bucket, object_key=result_key, data=result)
        logger.info("Result uploaded to %s/%s", config.bucket, result_key)

    except ValueError:
        # Already handled (or no status to set) — re-raise for DLQ
        raise
    except Exception as e:
        if not failed:
            state.update_status(
                job_id, JobStatus.FAILED, progress=0,
                message=f"Pipeline error: {type(e).__name__}: {e}",
            )
            failed = True
        raise

    # ── Step 9: Publish suspicious pairs to Kafka ────────────────────
    try:
        for pair in suspicious_pairs:
            kafka.produce_suspicious_pair({
                "jobId": job_id,
                "pairId": pair.pair_id,
                "fileA": pair.file_a,
                "fileB": pair.file_b,
                "similarity": pair.similarity,
            })

        # ── Step 10: Publish job complete ────────────────────────────
        kafka.produce_job_complete(job_id, result_key)

        # ── Step 11: Final status ────────────────────────────────────
        state.update_status(
            job_id, JobStatus.COMPLETE, progress=100,
            message=f"{len(suspicious_pairs)} pairs → ai-worker",
        )

        logger.info(
            "[handler] Pipeline complete | jobId=%s | files=%d | pairs=%d | "
            "elapsed=%.3fs",
            job_id, batch.processed, len(suspicious_pairs), elapsed,
        )

    except Exception as e:
        if not failed:
            state.update_status(
                job_id, JobStatus.FAILED, progress=0,
                message=f"Post-processing error: {type(e).__name__}: {e}",
            )
        raise
