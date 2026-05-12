"""Nexus Kafka Event Schemas — Python equivalent of shared/types/index.ts.

These dataclasses mirror the TypeScript interfaces exactly. Both the Node.js
API gateway and the Python workers must produce JSON matching these shapes.

Usage:
    from schemas import SuspiciousPairEvent, JobStatusEvent

    event = SuspiciousPairEvent.create(
        job_id="abc-123",
        file_a="student_alice.cpp",
        file_b="student_bob.cpp",
        score=0.847,
    )
    json_dict = dataclasses.asdict(event)
"""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class SuspiciousPairEvent:
    """Produced by hash-worker, consumed by ai-worker.

    Partition key: jobId
    Topic: suspicious-pairs
    """

    schemaVersion: int
    eventType: str
    jobId: str
    pairId: str
    fileAName: str
    fileBName: str
    fileAObjectKey: str
    fileBObjectKey: str
    similarityScore: float
    fingerprintSizeA: int
    fingerprintSizeB: int
    detectedAt: str

    @classmethod
    def create(
        cls,
        job_id: str,
        file_a: str,
        file_b: str,
        score: float,
        fp_size_a: int = 0,
        fp_size_b: int = 0,
    ) -> SuspiciousPairEvent:
        """Factory method with sensible defaults."""
        # Deterministic pair ID — same two files always produce the same ID
        pair_key = "|".join(sorted([file_a, file_b]))
        pair_id = hashlib.sha256(
            f"{job_id}:{pair_key}".encode()
        ).hexdigest()[:12]

        return cls(
            schemaVersion=1,
            eventType="SUSPICIOUS_PAIR",
            jobId=job_id,
            pairId=pair_id,
            fileAName=file_a,
            fileBName=file_b,
            fileAObjectKey=f"extracted/{job_id}/{file_a}",
            fileBObjectKey=f"extracted/{job_id}/{file_b}",
            similarityScore=round(score, 6),
            fingerprintSizeA=fp_size_a,
            fingerprintSizeB=fp_size_b,
            detectedAt=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )


@dataclass
class JobStatusEvent:
    """Published to job-lifecycle topic for real-time monitoring.

    Partition key: jobId
    Topic: job-lifecycle
    """

    schemaVersion: int
    eventType: str
    jobId: str
    status: str
    progress: int
    message: str
    timestamp: str

    @classmethod
    def create(
        cls,
        job_id: str,
        status: str,
        progress: int = 0,
        message: str = "",
    ) -> JobStatusEvent:
        return cls(
            schemaVersion=1,
            eventType="JOB_STATUS",
            jobId=job_id,
            status=status,
            progress=progress,
            message=message,
            timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )


# ── Topic constants ───────────────────────────────────────────────────────────

TOPIC_SUBMISSIONS      = "submissions"
TOPIC_SUSPICIOUS_PAIRS = "suspicious-pairs"
TOPIC_FORENSIC_RESULTS = "forensic-results"
TOPIC_JOB_LIFECYCLE    = "job-lifecycle"
TOPIC_DEAD_LETTER      = "dead-letter"

# ── MinIO path conventions ────────────────────────────────────────────────────


def submission_key(job_id: str) -> str:
    """MinIO object key for a submission ZIP."""
    return f"submissions/{job_id}.zip"


def extracted_file_key(job_id: str, filename: str) -> str:
    """MinIO object key for an extracted source file."""
    return f"extracted/{job_id}/{filename}"


def forensic_report_key(job_id: str, pair_id: str) -> str:
    """MinIO object key for a forensic analysis report."""
    return f"reports/{job_id}/{pair_id}.json"
