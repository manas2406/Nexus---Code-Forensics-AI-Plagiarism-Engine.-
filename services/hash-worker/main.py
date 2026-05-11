"""Nexus Hash Worker — Phase 2 standalone pipeline.

Usage:
    python main.py --zip <bucket>/<object_key> --job-id <uuid>

Example:
    python main.py --zip nexus-submissions/test.zip --job-id abc-123

Reads a ZIP from MinIO, processes every .cpp/.h file through the
AST → Winnowing → LSH → Jaccard pipeline, writes results to MinIO
and job status to Redis.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

from comparator import compare_candidates
from lsh_index import LSHIndex
from minio_client import MinIOClient
from pipeline import process_batch
from state import JobStateManager, JobStatus

# Load .env from repo root (two levels up from services/hash-worker/)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("nexus.main")


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


def main() -> int:
    """Run the full Nexus plagiarism-detection pipeline."""
    parser = argparse.ArgumentParser(description="Nexus Hash Worker Pipeline")
    parser.add_argument(
        "--zip", required=True,
        help="<bucket>/<object_key> path to the ZIP in MinIO",
    )
    parser.add_argument(
        "--job-id", required=True,
        help="Unique job identifier (UUID)",
    )
    args = parser.parse_args()

    # ── Parse bucket / key ────────────────────────────────────────────
    zip_path: str = args.zip
    if "/" not in zip_path:
        print(f"ERROR: --zip must be <bucket>/<key>, got: {zip_path}", file=sys.stderr)
        return 1
    bucket, object_key = zip_path.split("/", 1)
    job_id: str = args.job_id

    # ── Config from environment ───────────────────────────────────────
    threshold = float(os.getenv("SUSPICIOUS_PAIR_THRESHOLD", "0.6"))
    lsh_threshold = float(os.getenv("LSH_THRESHOLD", "0.5"))
    lsh_num_perm = int(os.getenv("LSH_NUM_PERM", "128"))
    max_file_bytes = int(os.getenv("MAX_FILE_BYTES", "500000"))

    # ── Build clients ─────────────────────────────────────────────────
    minio = _build_minio_client()
    state = _build_state_manager()

    # ── Health checks ─────────────────────────────────────────────────
    if not minio.health_check():
        print("ERROR: MinIO is unreachable", file=sys.stderr)
        return 1
    if not state.health_check():
        print("ERROR: Redis is unreachable", file=sys.stderr)
        return 1
    logger.info("Health checks passed — MinIO and Redis are reachable")

    # ── Step 4: PENDING ───────────────────────────────────────────────
    state.update_status(job_id, JobStatus.PENDING)

    try:
        # ── Step 5: Stream files from ZIP ─────────────────────────────
        logger.info("Streaming %s/%s from MinIO...", bucket, object_key)
        files: dict[str, str] = {}
        try:
            for entry in minio.stream_cpp_files(
                bucket, object_key, max_file_bytes=max_file_bytes,
            ):
                files[entry.filename] = entry.source_code
                logger.info("  Found: %s (%d bytes)", entry.filename, entry.size_bytes)
        except Exception as exc:
            state.update_status(job_id, JobStatus.FAILED, detail=f"ZIP error: {exc}")
            print(f"ERROR: Failed to read ZIP {bucket}/{object_key}: {exc}", file=sys.stderr)
            return 1

        if not files:
            state.update_status(
                job_id, JobStatus.FAILED, detail="ZIP contains zero .cpp/.h files",
            )
            print("ERROR: ZIP contains zero .cpp/.h files", file=sys.stderr)
            return 1

        n = len(files)
        logger.info("Found %d .cpp/.h files", n)

        # ── Step 6: PARSING ───────────────────────────────────────────
        state.update_status(job_id, JobStatus.PARSING, detail=f"{n} files found")

        # ── Step 7: process_batch ─────────────────────────────────────
        logger.info("Processing batch...")
        batch = process_batch(files, max_bytes=max_file_bytes)

        # ── Step 8: HASHING ───────────────────────────────────────────
        state.update_status(
            job_id, JobStatus.HASHING,
            detail=f"{batch.processed} files parsed, {len(batch.skipped)} skipped",
        )

        # ── Step 9: LSH candidates ────────────────────────────────────
        logger.info(
            "Running LSH pre-filter (threshold=%.2f, num_perm=%d)...",
            lsh_threshold, lsh_num_perm,
        )
        lsh = LSHIndex(threshold=lsh_threshold, num_perm=lsh_num_perm)
        candidates = lsh.get_all_candidates(batch.fingerprints)
        logger.info("Found %d candidate pairs", len(candidates))

        # ── Step 10: Exact comparison ─────────────────────────────────
        logger.info("Running exact Jaccard comparison (threshold=%.2f)...", threshold)
        suspicious_pairs = compare_candidates(
            candidates, batch.fingerprints, threshold=threshold,
        )
        logger.info("Found %d suspicious pairs", len(suspicious_pairs))

        # ── Step 11: Build result JSON ────────────────────────────────
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
            "completedAt": datetime.now(timezone.utc).isoformat(),
        }

        # ── Step 12: Upload result ────────────────────────────────────
        result_key = f"results/{job_id}.json"
        minio.put_json(bucket=bucket, object_key=result_key, data=result)
        logger.info("Result uploaded to %s/%s", bucket, result_key)

        # ── Step 13: COMPLETE ─────────────────────────────────────────
        state.update_status(
            job_id, JobStatus.COMPLETE,
            detail=f"{len(suspicious_pairs)} suspicious pairs found",
        )

        # ── Step 14: Publish event ────────────────────────────────────
        state.publish_event(job_id, result)

        # ── Step 15: Print summary ────────────────────────────────────
        print(f"\n{'=' * 60}")
        print(f"  Nexus Pipeline — Job {job_id}")
        print(f"{'=' * 60}")
        print(f"  Files processed : {batch.processed}")
        print(f"  Files skipped   : {len(batch.skipped)}")
        print(f"  Candidates (LSH): {len(candidates)}")
        print(f"  Suspicious pairs: {len(suspicious_pairs)}")
        print(f"  Elapsed         : {batch.elapsed_seconds:.3f}s")
        print(f"  Result JSON     : {bucket}/{result_key}")
        print(f"{'=' * 60}\n")

        for pair in suspicious_pairs:
            print(f"  \u26a0 {pair.file_a} \u2194 {pair.file_b}  (similarity: {pair.similarity:.4f})")

        return 0

    except Exception:
        tb = traceback.format_exc()
        logger.error("Pipeline failed:\n%s", tb)
        state.update_status(job_id, JobStatus.FAILED, detail=tb[:500])
        return 1


if __name__ == "__main__":
    sys.exit(main())
