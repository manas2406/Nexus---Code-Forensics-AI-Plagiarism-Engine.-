"""Redis-based job state management for Nexus pipeline.

Infrastructure-only module — never imports from pipeline.py or algorithm files.
Provides status tracking, TTL-managed keys, and pub/sub event publishing.

Redis key patterns:
    job:{jobId}:status  → Hash  (HSET/HGETALL) — point-in-time queries
    job:{jobId}:pairs   → List  (RPUSH/LRANGE) — pair IDs for AI worker
    job:{jobId}:events  → Pub/Sub channel     — real-time WebSocket delivery
"""

from __future__ import annotations

import json
import logging
import time
from enum import Enum
from typing import Any, Optional

import redis as redis_lib

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Pipeline job lifecycle states.

    Transition ownership:
        PENDING      → set by api-gateway when JOB_CREATED is published
        EXTRACTING   → set by hash-worker when ZIP download from MinIO starts
        PARSING      → set by hash-worker when tree-sitter AST loop starts
        HASHING      → set by hash-worker when Winnowing fingerprinting starts
        COMPARING    → set by hash-worker when pairwise Jaccard comparison starts
        AI_ANALYSIS  → set by ai-worker when first LLM call fires
        COMPLETE     → set by ai-worker after last ForensicResult is stored
        FAILED       → set by either worker on unrecoverable error
    """

    PENDING     = "PENDING"
    EXTRACTING  = "EXTRACTING"
    PARSING     = "PARSING"
    HASHING     = "HASHING"
    COMPARING   = "COMPARING"
    AI_ANALYSIS = "AI_ANALYSIS"
    COMPLETE    = "COMPLETE"
    FAILED      = "FAILED"


# ── TTL Constants ─────────────────────────────────────────────────────────────
JOB_STATUS_TTL_SECONDS = 86_400        # 24 hours — refreshed on every update
JOB_PAIRS_TTL_SECONDS  = 86_400 * 7   # 7 days — AI worker needs these later


class JobStateManager:
    """Manages job state in Redis.

    Key patterns:
        ``job:{job_id}:status`` → Redis Hash (HSET/HGETALL)
        ``job:{job_id}:pairs``  → Redis List (RPUSH/LRANGE)
        ``job:{job_id}:events`` → Pub/Sub channel
    """

    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        self._client: redis_lib.Redis[str] = redis_lib.from_url(
            redis_url, decode_responses=True,
        )

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        progress: int = 0,
        message: str = "",
        detail: str | None = None,
    ) -> None:
        """Write job status to Redis Hash and publish to Pub/Sub.

        Redis Hash ``job:{jobId}:status``:
            Fields: status, progress, message, updated_at, job_id
            TTL: refreshed to 24h on every call

        Pub/Sub ``job:{jobId}:events``:
            Payload: JSON with jobId, status, progress, message, timestamp
            Note: In Phase 2, nobody is subscribed. Messages are dropped.
            In Phase 3, the API gateway subscribes and bridges to WebSocket.
        """
        key = f"job:{job_id}:status"
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # Use detail as message fallback for backward compat
        msg = message or detail or ""

        # Atomic hash update
        self._client.hset(key, mapping={
            "status":     status.value,
            "progress":   str(progress),
            "message":    msg,
            "updated_at": now,
            "job_id":     job_id,
        })
        self._client.expire(key, JOB_STATUS_TTL_SECONDS)

        # Pub/Sub publish (fire-and-forget — no error if nobody subscribed)
        channel = f"job:{job_id}:events"
        payload = json.dumps({
            "jobId":     job_id,
            "status":    status.value,
            "progress":  progress,
            "message":   msg,
            "timestamp": now,
        })
        self._client.publish(channel, payload)
        logger.debug("State: job=%s status=%s progress=%d", job_id, status.value, progress)

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        """Retrieve current job status from Redis Hash.

        Returns None if job does not exist (expired or never created).
        """
        key = f"job:{job_id}:status"
        result = self._client.hgetall(key)
        return result if result else None

    def publish_event(self, job_id: str, payload: dict[str, Any]) -> None:
        """Publish to Redis channel ``job:{job_id}:events``.

        Fire-and-forget — does not wait for subscribers.
        """
        channel = f"job:{job_id}:events"
        self._client.publish(channel, json.dumps(payload))

    def set_job_pairs(self, job_id: str, pair_ids: list[str]) -> None:
        """Store the list of suspicious pair IDs for this job.

        Used by the AI worker to enumerate which pairs need forensic analysis.
        """
        key = f"job:{job_id}:pairs"
        if pair_ids:
            self._client.delete(key)
            self._client.rpush(key, *pair_ids)
            self._client.expire(key, JOB_PAIRS_TTL_SECONDS)

    def get_job_pairs(self, job_id: str) -> list[str]:
        """Retrieve all suspicious pair IDs for a job."""
        return self._client.lrange(f"job:{job_id}:pairs", 0, -1)

    def health_check(self) -> bool:
        """Return True if Redis responds to PING. Never raises."""
        try:
            return bool(self._client.ping())
        except Exception:
            return False
