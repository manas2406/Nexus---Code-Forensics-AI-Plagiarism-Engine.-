# services/ai-worker/report_store.py
"""
ForensicReport persistence layer.

Responsibilities:
1. Serialize ForensicReport dict to JSON and store in MinIO
   Path: reports/{jobId}/{pairId}.json
2. Write report reference to Redis Hash
   Key: job:{jobId}:report:{pairId}  (7-day TTL)
3. Publish completion event to Redis Pub/Sub
   Channel: job:{jobId}:events  (consumed by API gateway WebSocket bridge)

Design decisions:
- MinIO write is the source of truth — S3Error propagates to caller for retry.
- Redis writes are best-effort — RedisError is caught and logged (non-fatal).
  The API gateway falls back to polling MinIO directly if Redis is stale.
- Source code lives in nexus-submissions at extracted/{jobId}/{filename}.
  Reports live in nexus-reports at reports/{jobId}/{pairId}.json.
"""

from __future__ import annotations

import json
import logging
import os
import time
from io import BytesIO
from typing import Any

import redis as redis_lib
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger("nexus.report-store")

# ── Singletons (lazy-initialized) ────────────────────────────────────────────
_minio: Minio | None = None
_redis: redis_lib.Redis | None = None

# Bucket for forensic reports (separate from submissions bucket)
BUCKET_REPORTS    = os.environ.get("MINIO_BUCKET_REPORTS", "nexus-reports")
BUCKET_SUBMISSIONS = os.environ.get("MINIO_BUCKET_SUBMISSIONS", "nexus-submissions")

# Redis TTL for report reference keys (7 days)
REPORT_TTL = 86_400 * 7


def get_minio() -> Minio:
    """Lazy singleton MinIO client."""
    global _minio
    if _minio is None:
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost")
        port     = os.environ.get("MINIO_PORT", "9000")
        full_ep  = endpoint if ":" in endpoint else f"{endpoint}:{port}"
        _minio = Minio(
            endpoint=full_ep,
            access_key=os.environ.get("MINIO_ACCESS_KEY", "nexus"),
            secret_key=os.environ.get(
                "MINIO_SECRET_KEY", "nexus-secret-change-in-prod"
            ),
            secure=os.environ.get("MINIO_USE_SSL", "false").lower() == "true",
        )
    return _minio


def get_redis() -> redis_lib.Redis:
    """Lazy singleton Redis client."""
    global _redis
    if _redis is None:
        _redis = redis_lib.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", "6379")),
            decode_responses=True,
        )
    return _redis


def ensure_report_bucket() -> None:
    """Create the nexus-reports bucket if it doesn't exist (idempotent)."""
    minio = get_minio()
    if not minio.bucket_exists(BUCKET_REPORTS):
        minio.make_bucket(BUCKET_REPORTS)
        logger.info("Created MinIO bucket: %s", BUCKET_REPORTS)


def store_report(
    job_id: str,
    pair_id: str,
    report: dict[str, Any],
) -> str:
    """
    Persist a forensic report to MinIO and index it in Redis.

    Args:
        job_id:  The job this report belongs to.
        pair_id: The suspicious pair this report covers.
        report:  The ForensicReport as a dict (from dataclasses.asdict() or
                 build_synthetic_report()).

    Returns:
        The MinIO object key for the stored report (reports/{jobId}/{pairId}.json).

    Raises:
        S3Error: If MinIO write fails — caller should retry.
        Exception: Any other unexpected error from MinIO.
    """
    object_key = f"reports/{job_id}/{pair_id}.json"

    # ── 1. Serialize with storage metadata ────────────────────────────────────
    report_with_meta = {
        **report,
        "storedAt":  time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "objectKey": object_key,
        "jobId":     job_id,
    }

    json_bytes = json.dumps(report_with_meta, indent=2, ensure_ascii=False).encode("utf-8")
    json_size  = len(json_bytes)

    # ── 2. Upload to MinIO ────────────────────────────────────────────────────
    minio = get_minio()
    minio.put_object(
        bucket_name=BUCKET_REPORTS,
        object_name=object_key,
        data=BytesIO(json_bytes),
        length=json_size,
        content_type="application/json",
        metadata={
            "job-id":  job_id,
            "pair-id": pair_id,
            "verdict": str(report.get("verdict", "UNKNOWN")),
        },
    )

    logger.info(
        "Report stored | job=%s | pair=%s | key=%s | size=%dB",
        job_id, pair_id, object_key, json_size,
    )

    # ── 3. Write Redis reference (best-effort) ────────────────────────────────
    _write_report_ref_to_redis(job_id, pair_id, object_key, report)

    # ── 4. Publish REPORT_READY event (best-effort) ───────────────────────────
    _publish_report_event(job_id, pair_id, object_key, report)

    return object_key


def fetch_source_from_minio(object_key: str, bucket: str | None = None) -> str:
    """
    Fetch raw C++ source code from MinIO.

    The hash worker extracted source files to: extracted/{jobId}/{filename}
    in the nexus-submissions bucket.

    Args:
        object_key: MinIO object key (e.g. "extracted/job-001/alice.cpp").
        bucket:     Optional bucket override. Defaults to nexus-submissions.

    Returns:
        Decoded source string (UTF-8 with latin-1 fallback).

    Raises:
        S3Error: If the object doesn't exist or MinIO is unreachable.
    """
    target_bucket = bucket or BUCKET_SUBMISSIONS
    minio    = get_minio()
    response = minio.get_object(target_bucket, object_key)
    try:
        raw_bytes = response.read()
    finally:
        response.close()
        response.release_conn()

    # UTF-8 first, latin-1 fallback (same strategy as hash-worker ast_engine.py)
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return raw_bytes.decode("latin-1")


def update_job_ai_status(
    job_id: str,
    status: str,
    progress: int,
    message: str,
) -> None:
    """
    Update the top-level job status hash in Redis during AI analysis.
    Mirrors the same HSET pattern used by hash-worker state.py.
    Also publishes to the job's event channel for WebSocket fan-out.
    """
    try:
        r   = get_redis()
        key = f"job:{job_id}:status"
        r.hset(key, mapping={
            "status":    status,
            "progress":  str(progress),
            "message":   message,
            "updatedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        r.expire(key, 86_400)   # Refresh 24h TTL on every update

        r.publish(f"job:{job_id}:events", json.dumps({
            "jobId":     job_id,
            "status":    status,
            "progress":  progress,
            "message":   message,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }))

    except Exception as e:
        # Non-fatal — broad catch includes OSError, ConnectionRefusedError
        logger.warning("Redis status update failed (non-fatal) | job=%s: %s", job_id, e)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _write_report_ref_to_redis(
    job_id: str,
    pair_id: str,
    object_key: str,
    report: dict[str, Any],
) -> None:
    """Write a compact report reference to Redis (best-effort)."""
    try:
        r   = get_redis()
        key = f"job:{job_id}:report:{pair_id}"

        r.hset(key, mapping={
            "pairId":     pair_id,
            "jobId":      job_id,
            "objectKey":  object_key,
            "verdict":    str(report.get("verdict", "UNKNOWN")),
            "confidence": str(report.get("confidence", 0.0)),
            "storedAt":   time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        r.expire(key, REPORT_TTL)

    except Exception as e:
        # Non-fatal — report is in MinIO regardless. Catching broadly because
        # connection errors (OSError, ConnectionRefusedError) are not RedisError subclasses.
        logger.warning(
            "Redis report ref write failed (non-fatal) | pair=%s: %s", pair_id, e,
        )


def _publish_report_event(
    job_id: str,
    pair_id: str,
    object_key: str,
    report: dict[str, Any],
) -> None:
    """
    Publish a REPORT_READY event to job's Redis Pub/Sub channel.
    The API gateway bridges this to the WebSocket subscriber (Phase 5).
    progress=-1 signals a pair completion event (not job-level progress).
    """
    try:
        r = get_redis()
        r.publish(f"job:{job_id}:events", json.dumps({
            "jobId":        job_id,
            "status":       "AI_ANALYSIS",
            "progress":     -1,
            "message":      f"Forensic report ready: {pair_id}",
            "timestamp":    time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "eventSubtype": "REPORT_READY",
            "pairId":       pair_id,
            "reportKey":    object_key,
            "verdict":      str(report.get("verdict", "UNKNOWN")),
        }))

    except Exception as e:
        # Non-fatal — broad catch for connection errors outside RedisError hierarchy
        logger.warning(
            "Redis publish failed (non-fatal) | pair=%s: %s", pair_id, e,
        )
