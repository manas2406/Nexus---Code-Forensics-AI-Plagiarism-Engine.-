"""Redis-based job state management for Nexus pipeline.

Infrastructure-only module — never imports from pipeline.py or algorithm files.
Provides status tracking, TTL-managed keys, and pub/sub event publishing.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import redis as redis_lib

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    """Pipeline job lifecycle states."""

    PENDING  = "PENDING"
    PARSING  = "PARSING"
    HASHING  = "HASHING"
    COMPLETE = "COMPLETE"
    FAILED   = "FAILED"


class JobStateManager:
    """Manages job state in Redis.

    Key pattern: ``job:{job_id}:status``
    Channel pattern: ``nexus:job:{job_id}``
    """

    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        self._client: redis_lib.Redis[str] = redis_lib.from_url(
            redis_url, decode_responses=True,
        )

    def update_status(
        self,
        job_id: str,
        status: JobStatus,
        detail: str | None = None,
        ttl_seconds: int = 86400,
    ) -> None:
        """Write ``job:{job_id}:status`` as a JSON blob to Redis.

        Schema: ``{ "status": str, "detail": str | null, "updatedAt": ISO8601 }``
        Resets TTL on every write.
        """
        key = f"job:{job_id}:status"
        blob = json.dumps({
            "status": status.value,
            "detail": detail,
            "updatedAt": datetime.now(timezone.utc).isoformat(),
        })
        self._client.set(key, blob)
        self._client.expire(key, ttl_seconds)

    def get_status(self, job_id: str) -> dict[str, Any] | None:
        """Return the status blob or ``None`` if key does not exist."""
        key = f"job:{job_id}:status"
        raw = self._client.get(key)
        if raw is None:
            return None
        return json.loads(raw)  # type: ignore[no-any-return]

    def publish_event(self, job_id: str, payload: dict[str, Any]) -> None:
        """Publish to Redis channel ``nexus:job:{job_id}``.

        Fire-and-forget — does not wait for subscribers.
        """
        channel = f"nexus:job:{job_id}"
        self._client.publish(channel, json.dumps(payload))

    def health_check(self) -> bool:
        """Return True if Redis responds to PING. Never raises."""
        try:
            return bool(self._client.ping())
        except Exception:
            return False
