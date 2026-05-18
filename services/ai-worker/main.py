# services/ai-worker/main.py
"""
Nexus AI Worker — Entry Point

Pipeline per job batch:
1. Kafka batch arrives (all SuspiciousPairEvents for one job)
2. Deduplicator collapses cheating rings → representative pairs
3. For each representative pair (concurrently via asyncio.gather):
   a. Fetch source code from MinIO (extracted/{jobId}/{filename})
   b. Truncate to LLM_MAX_SOURCE_CHARS
   c. Call analyze_pair() — Dev B's LLM call (semaphore + backoff inside)
   d. Store ForensicReport JSON in MinIO at reports/{jobId}/{pairId}.json
   e. Write Redis report ref + publish REPORT_READY event
4. For each non-representative pair:
   a. Build synthetic report (no LLM call)
   b. Store synthetic report in MinIO
   c. Write Redis report ref
5. Update job status: AI_ANALYSIS → COMPLETE (or catch exception from handler)

State transitions during AI phase:
  COMPLETE (from hash-worker) → AI_ANALYSIS (0%) → AI_ANALYSIS (5–95%) → COMPLETE (100%)
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import sys
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Load .env from repo root (two levels up from services/ai-worker/)
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(_REPO_ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("nexus.ai-worker")

from deduplicator import deduplicate_pairs, build_synthetic_report
from forensic_analyst import ForensicReport, SuspiciousPair, analyze_pair
from kafka_client import PermanentFailure, run_consumer_loop
from report_store import (
    ensure_report_bucket,
    fetch_source_from_minio,
    store_report,
    update_job_ai_status,
)

# Max C++ source chars sent to the LLM per file. Truncation applied HERE —
# Dev B's analyze_pair() receives already-truncated strings.
MAX_SOURCE_CHARS = int(os.environ.get("LLM_MAX_SOURCE_CHARS", "4000"))


async def process_job_batch(pair_payloads: list[dict[str, Any]]) -> None:
    """
    Process all suspicious pairs for one job.

    Called by kafka_client.py via asyncio.run() after batch accumulation.
    Raises on unrecoverable errors (caught by kafka_client retry loop).

    Args:
        pair_payloads: All SuspiciousPairEvent dicts for this job.
    """
    if not pair_payloads:
        return

    job_id = str(pair_payloads[0].get("jobId", "unknown"))
    logger.info("Processing batch | job=%s | pairs=%d", job_id, len(pair_payloads))

    # ── 1. Mark job as AI_ANALYSIS in Redis ───────────────────────────────────
    update_job_ai_status(
        job_id, "AI_ANALYSIS", 0,
        f"Starting forensic analysis of {len(pair_payloads)} suspicious pairs...",
    )

    # ── 2. Deduplicate cheating rings ─────────────────────────────────────────
    dedup = deduplicate_pairs(pair_payloads)

    logger.info(
        "Deduplication | job=%s | total=%d | llm_calls=%d | synthetic=%d",
        job_id, dedup.total_pairs, dedup.llm_call_count, dedup.reduction_count,
    )

    update_job_ai_status(
        job_id, "AI_ANALYSIS", 5,
        f"Cheating ring analysis: {dedup.llm_call_count} LLM calls needed "
        f"(deduplicated from {dedup.total_pairs} pairs, {dedup.reduction_count} synthetic).",
    )

    # ── 3. Fetch source code for all representative pairs ─────────────────────
    # Cache by objectKey to avoid double-fetching files shared across pairs.
    source_cache: dict[str, str] = {}

    for payload in dedup.representative_pairs:
        for key_field in ("fileAObjectKey", "fileBObjectKey"):
            obj_key = str(payload.get(key_field, ""))
            if obj_key and obj_key not in source_cache:
                try:
                    raw = fetch_source_from_minio(obj_key)
                    source_cache[obj_key] = raw[:MAX_SOURCE_CHARS]
                    logger.debug(
                        "Fetched source | key=%s | chars=%d (truncated to %d)",
                        obj_key, len(raw), MAX_SOURCE_CHARS,
                    )
                except Exception as e:
                    logger.error(
                        "Cannot fetch source | key=%s: %s — using placeholder",
                        obj_key, e,
                    )
                    source_cache[obj_key] = f"[SOURCE UNAVAILABLE: {type(e).__name__}: {e}]"

    # ── 4. Run LLM analysis concurrently on all representative pairs ──────────
    # analyze_pair() manages its own asyncio.Semaphore internally (LLM_MAX_CONCURRENT).
    # We fire all representative pairs simultaneously — the semaphore inside
    # forensic_analyst.py caps the actual in-flight LLM requests.

    async def analyze_one(payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        """Analyze one representative pair. Returns (pair_id, normalized_report_dict)."""
        pair = SuspiciousPair(
            job_id           = str(payload.get("jobId", job_id)),
            pair_id          = str(payload["pairId"]),
            file_a_name      = str(payload["fileAName"]),
            file_a_source    = source_cache.get(str(payload.get("fileAObjectKey", "")), ""),
            file_b_name      = str(payload["fileBName"]),
            file_b_source    = source_cache.get(str(payload.get("fileBObjectKey", "")), ""),
            similarity_score = float(payload["similarityScore"]),
        )

        report: ForensicReport = await analyze_pair(pair)
        report_dict = dataclasses.asdict(report)

        # Normalize snake_case → camelCase for JSON storage
        return str(payload["pairId"]), _normalize_keys(report_dict)

    # Gather all representative analyses — exceptions captured via return_exceptions
    rep_tasks   = [analyze_one(p) for p in dedup.representative_pairs]
    raw_results = await asyncio.gather(*rep_tasks, return_exceptions=True)

    # Build pair_id → report_dict map, logging any task failures
    rep_reports: dict[str, dict[str, Any]] = {}
    for result in raw_results:
        if isinstance(result, Exception):
            logger.error("LLM analysis task raised unexpectedly: %s", result)
            continue
        pair_id, report_dict = result
        rep_reports[pair_id] = report_dict

    # ── 5. Store all reports ──────────────────────────────────────────────────
    total_stored = 0
    total_pairs  = len(pair_payloads)

    # 5a. Store representative reports
    for pair_id, report_dict in rep_reports.items():
        try:
            store_report(job_id, pair_id, report_dict)
            total_stored += 1
        except Exception as e:
            logger.error("Failed to store report for pair=%s: %s", pair_id, e)
            # Don't abort the whole job — log and continue

        _emit_progress(job_id, total_stored, total_pairs)

    # 5b. Store synthetic reports for non-representative pairs
    for payload in pair_payloads:
        pair_id     = str(payload["pairId"])
        rep_pair_id = dedup.ring_map.get(pair_id, pair_id)

        if rep_pair_id == pair_id:
            continue   # This IS the representative — already stored above

        rep_report_key = f"reports/{job_id}/{rep_pair_id}.json"
        synthetic      = build_synthetic_report(payload, rep_report_key)

        try:
            store_report(job_id, pair_id, synthetic)
            total_stored += 1
        except Exception as e:
            logger.error("Failed to store synthetic report for pair=%s: %s", pair_id, e)

        _emit_progress(job_id, total_stored, total_pairs)

    # ── 6. Final COMPLETE status ───────────────────────────────────────────────
    update_job_ai_status(
        job_id, "COMPLETE", 100,
        f"Forensic analysis complete: {total_stored}/{total_pairs} reports stored. "
        f"{dedup.reduction_count} pairs deduplicated via cheating ring detection.",
    )

    logger.info(
        "Batch complete | job=%s | reports=%d/%d | llm_calls=%d | synthetic=%d",
        job_id, total_stored, total_pairs,
        dedup.llm_call_count, dedup.reduction_count,
    )


def _emit_progress(job_id: str, stored: int, total: int) -> None:
    """Emit a progress update during report storage (10–95% range)."""
    if total == 0:
        return
    progress = 10 + int((stored / total) * 85)
    update_job_ai_status(
        job_id, "AI_ANALYSIS", progress,
        f"Stored {stored}/{total} forensic reports...",
    )


def _normalize_keys(report_dict: dict[str, Any]) -> dict[str, Any]:
    """
    Convert Python snake_case dataclass field names to camelCase for JSON storage.

    ForensicReport uses snake_case (Python convention) but our JSON schema uses camelCase
    (TypeScript convention for frontend consumption).
    """
    key_map = {
        "pair_id":                "pairId",
        "similarity_score":       "similarityScore",
        "obfuscation_techniques": "obfuscationTechniques",
        "verdict":                "verdict",
        "confidence":             "confidence",
        "analyst_notes":          "analystNotes",
        "raw_llm_response":       "rawLlmResponse",
    }
    return {key_map.get(k, k): v for k, v in report_dict.items()}


def main() -> None:
    """Entry point — starts the Kafka consumer loop."""
    logger.info("Nexus AI Worker starting...")
    logger.info(
        "Config | LLM_MAX_CONCURRENT=%s | LLM_MODEL=%s | MAX_SOURCE_CHARS=%d | "
        "BATCH_WINDOW=%.1fs",
        os.environ.get("LLM_MAX_CONCURRENT", "5"),
        os.environ.get("LLM_MODEL", "gpt-4o-mini"),
        MAX_SOURCE_CHARS,
        float(os.environ.get("AI_BATCH_WINDOW_SECONDS", "2.0")),
    )

    # Ensure the reports bucket exists before consuming any messages
    try:
        ensure_report_bucket()
    except Exception as e:
        logger.warning("Could not verify nexus-reports bucket: %s", e)

    # Blocking — runs until SIGTERM/SIGINT
    run_consumer_loop(handler=process_job_batch)
    logger.info("AI Worker shut down cleanly.")


if __name__ == "__main__":
    main()
