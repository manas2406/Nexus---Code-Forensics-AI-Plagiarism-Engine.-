# services/ai-worker/forensic_analyst.py
# INTERFACE CONTRACT — locked Phase 4 Day 1
#
# Dev A owns the wrapper. Dev B implements analyze_pair().
# Do NOT change function signatures without notifying Dev A.
#
# Contract summary:
#   - analyze_pair(SuspiciousPair) -> ForensicReport
#   - NEVER raises (fallback ForensicReport always returned on any failure)
#   - Concurrency controlled internally via asyncio.Semaphore (LLM_MAX_CONCURRENT)
#   - Source code strings provided by caller (Dev A fetches from MinIO before calling)
#   - Truncation to LLM_MAX_SOURCE_CHARS applied by Dev A BEFORE passing to this function
#
# Dev B implements:
#   - asyncio.Semaphore cap (LLM_MAX_CONCURRENT env var)
#   - Exponential backoff with jitter on 429 / 5xx
#   - JSON response parsing + markdown fence stripping
#   - Fallback report on total LLM failure

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("nexus.forensic-analyst")


@dataclass
class SuspiciousPair:
    """
    Input to analyze_pair(). Dev A constructs this from the Kafka payload + MinIO source.

    Fields:
        job_id:           Parent job identifier.
        pair_id:          Unique pair identifier (SHA-256 prefix from hash-worker).
        file_a_name:      Original filename (e.g., "alice.cpp").
        file_a_source:    Raw C++ source code, pre-truncated to LLM_MAX_SOURCE_CHARS.
        file_b_name:      Original filename (e.g., "bob.cpp").
        file_b_source:    Raw C++ source code, pre-truncated to LLM_MAX_SOURCE_CHARS.
        similarity_score: Jaccard similarity from the hash worker (0.0–1.0).
    """

    job_id:           str
    pair_id:          str
    file_a_name:      str
    file_a_source:    str
    file_b_name:      str
    file_b_source:    str
    similarity_score: float


@dataclass
class ForensicReport:
    """
    Output from analyze_pair(). Dev A serializes this to MinIO JSON.

    Fields:
        pair_id:                Echoes the input pair_id (used as MinIO object name).
        similarity_score:       Echoes the input score (for denormalization/display).
        obfuscation_techniques: e.g. ["VARIABLE_RENAMING", "LOOP_RESTRUCTURING"].
        evidence:               List of dicts with {"type": str, "description": str}.
        verdict:                "LIKELY_PLAGIARISM" | "POSSIBLE_COINCIDENCE" | "INCONCLUSIVE".
        confidence:             Model's confidence in the verdict (0.0–1.0).
        analyst_notes:          Free-text narrative explanation.
        raw_llm_response:       Always preserved verbatim for audit (even on fallback).
    """

    pair_id:                str
    similarity_score:       float
    obfuscation_techniques: list[str]
    evidence:               list[dict[str, Any]]
    verdict:                str    # "LIKELY_PLAGIARISM" | "POSSIBLE_COINCIDENCE" | "INCONCLUSIVE"
    confidence:             float
    analyst_notes:          str
    raw_llm_response:       str


async def analyze_pair(pair: SuspiciousPair) -> ForensicReport:
    """
    STUB — Dev B implements this function.

    Dev B's implementation must:
    1. Acquire asyncio.Semaphore (LLM_MAX_CONCURRENT) before HTTP call
    2. Call LLM API with system prompt + formatted C++ sources
    3. Strip markdown fences (```json...```) from response
    4. Parse JSON into ForensicReport fields
    5. On ANY failure: return a fallback ForensicReport (never raise)
    6. On 429/5xx: exponential backoff with jitter, then retry

    This stub returns INCONCLUSIVE with a note so the pipeline works end-to-end
    during development before Dev B delivers the real implementation.
    """
    logger.warning(
        "[STUB] analyze_pair called for pair=%s — Dev B has not implemented this yet. "
        "Returning INCONCLUSIVE fallback report.",
        pair.pair_id,
    )
    return ForensicReport(
        pair_id=pair.pair_id,
        similarity_score=pair.similarity_score,
        obfuscation_techniques=[],
        evidence=[],
        verdict="INCONCLUSIVE",
        confidence=0.0,
        analyst_notes=(
            "[STUB] Dev B has not yet implemented forensic_analyst.analyze_pair(). "
            "This is a placeholder report. Replace this file with Dev B's implementation."
        ),
        raw_llm_response="",
    )
