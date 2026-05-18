# services/ai-worker/kafka_client.py
"""
AI Worker Kafka Consumer.

Key design decisions vs hash-worker kafka_client.py:
┌─────────────────────┬──────────────────────┬─────────────────────────────┐
│ Dimension           │ Hash Worker          │ AI Worker                   │
├─────────────────────┼──────────────────────┼─────────────────────────────┤
│ Consumer group      │ hash-workers         │ ai-workers                  │
│ Source topic        │ submissions          │ suspicious-pairs             │
│ Message size        │ ~metadata only       │ Up to 5MB (C++ source)      │
│ Handler runtime     │ Minutes (CPU)        │ 5–30s per pair (LLM I/O)    │
│ Handler type        │ Sync                 │ Async (asyncio.run)         │
│ Parallelism         │ Sequential           │ asyncio.gather within batch │
└─────────────────────┴──────────────────────┴─────────────────────────────┘

Batch accumulation strategy:
- Pairs for the same jobId arrive in a burst (hash-worker produces all pairs
  before committing, so they land in the same Kafka batch).
- We accumulate all pairs for a job for BATCH_WINDOW_SECONDS after the last
  pair for that job, then flush the batch to the async handler.
- This enables transitive closure deduplication: the handler sees ALL pairs
  for a job simultaneously and can collapse cheating rings before firing LLM calls.
- Trade-off: 2s added latency per job vs dramatically reduced LLM cost.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import signal
import time
from collections import defaultdict
from typing import Any, Awaitable, Callable

from confluent_kafka import Consumer, KafkaError, Message, Producer

logger = logging.getLogger("nexus.ai-kafka")

# ── Config ────────────────────────────────────────────────────────────────────
_DLQ_TOPIC       = "dead-letter"
_MAX_RETRIES     = 5
_INITIAL_BACKOFF = 1.0
_BACKOFF_MULT    = 2.0
_MAX_BACKOFF     = 60.0

# Seconds to wait after last pair for a job before flushing the batch.
# Pairs for one job arrive in a burst. 2s catches stragglers.
_BATCH_WINDOW_SECONDS = float(os.environ.get("AI_BATCH_WINDOW_SECONDS", "2.0"))

# Max pairs per job before force-flushing (memory safety for massive batches)
_MAX_BATCH_SIZE = int(os.environ.get("AI_MAX_BATCH_SIZE", "500"))

# Async batch handler: receives all pair payloads for one job
AsyncBatchHandler = Callable[[list[dict[str, Any]]], Awaitable[None]]


class PermanentFailure(Exception):
    """
    Raise inside the async handler to skip retries and route directly to DLQ.
    Use for unrecoverable errors (e.g. corrupt payload schema).
    """
    pass


def _get_brokers() -> str:
    return os.environ.get("KAFKA_BROKERS", "localhost:9092")


def _make_consumer() -> Consumer:
    """
    Configure the AI worker Kafka consumer.

    max.poll.interval.ms = 300s (5 min):
        A full job batch (e.g. 50 pairs with LLM_MAX_CONCURRENT=5)
        takes at most ~150s. We use 300s for headroom.

    fetch.max.bytes = 5MB:
        SuspiciousPairEvent messages include full C++ source code.
        Must match producer's message.max.bytes.
    """
    return Consumer({
        "bootstrap.servers":         _get_brokers(),
        "group.id":                  "ai-workers",
        "auto.offset.reset":         "earliest",
        "enable.auto.commit":        False,         # Manual commit — at-least-once
        "max.poll.interval.ms":      300_000,       # 5 minutes
        "session.timeout.ms":        45_000,
        "heartbeat.interval.ms":     15_000,
        "fetch.max.bytes":           5_242_880,     # 5MB
        "max.partition.fetch.bytes": 5_242_880,
    })


def _make_dlq_producer() -> Producer:
    return Producer({
        "bootstrap.servers": _get_brokers(),
        "enable.idempotence": True,
        "compression.type":   "gzip",
        "acks":               "all",
    })


def run_consumer_loop(handler: AsyncBatchHandler) -> None:
    """
    Production consumer loop for the AI worker.

    Blocking — runs until SIGTERM/SIGINT.

    Batch accumulation:
    1. Poll messages continuously, accumulating pairs by jobId
    2. After _BATCH_WINDOW_SECONDS with no new messages for a jobId,
       flush that job's batch to the async handler
    3. Force-flush if batch reaches _MAX_BATCH_SIZE (memory safety)
    4. On shutdown: flush all pending batches before closing

    Manual commit protocol:
    - Offsets are committed ONLY after the handler succeeds or the batch is DLQ'd.
    - Never commits mid-retry (at-least-once delivery guarantee).
    """
    consumer     = _make_consumer()
    dlq_producer = _make_dlq_producer()

    consumer.subscribe(["suspicious-pairs"])

    # Accumulated batches: jobId → list of (message, payload)
    batches:   dict[str, list[tuple[Message, dict[str, Any]]]] = defaultdict(list)
    last_seen: dict[str, float] = {}   # jobId → monotonic time of last message

    _shutdown = False

    def _handle_signal(sig: int, frame: object) -> None:
        nonlocal _shutdown
        logger.info("Signal %d received — flushing pending batches before exit...", sig)
        _shutdown = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT,  _handle_signal)

    logger.info(
        "AI worker consumer started | group=ai-workers | topic=suspicious-pairs | "
        "batch_window=%.1fs | max_batch=%d",
        _BATCH_WINDOW_SECONDS, _MAX_BATCH_SIZE,
    )

    try:
        while not _shutdown:
            # ── Poll for new messages ──────────────────────────────────────────
            msg: Message | None = consumer.poll(timeout=0.5)

            if msg is not None:
                if msg.error():
                    _handle_kafka_error(msg)
                else:
                    payload = _decode_message(msg)
                    if payload is not None:
                        job_id = str(payload.get("jobId", "unknown"))
                        batches[job_id].append((msg, payload))
                        last_seen[job_id] = time.monotonic()

                        # Force-flush if batch is too large
                        if len(batches[job_id]) >= _MAX_BATCH_SIZE:
                            logger.warning(
                                "Force-flush: job=%s hit max batch size (%d pairs)",
                                job_id, _MAX_BATCH_SIZE,
                            )
                            _flush_job_batch(
                                job_id, batches, consumer, dlq_producer, handler,
                            )

            # ── Flush stale batches (window expired) ───────────────────────────
            now = time.monotonic()
            for job_id in list(last_seen.keys()):
                if (job_id in batches
                        and now - last_seen[job_id] >= _BATCH_WINDOW_SECONDS):
                    _flush_job_batch(
                        job_id, batches, consumer, dlq_producer, handler,
                    )
                    last_seen.pop(job_id, None)

    finally:
        # Flush any remaining batches before exiting
        for job_id in list(batches.keys()):
            logger.info(
                "Shutdown flush: job=%s (%d pairs)", job_id, len(batches[job_id])
            )
            _flush_job_batch(job_id, batches, consumer, dlq_producer, handler)

        consumer.close()
        dlq_producer.flush(timeout=10)
        logger.info("AI worker consumer shut down cleanly")


def _flush_job_batch(
    job_id: str,
    batches: dict[str, list[tuple[Message, dict[str, Any]]]],
    consumer: Consumer,
    dlq_producer: Producer,
    handler: AsyncBatchHandler,
) -> None:
    """
    Process and commit all messages for one job.

    Runs the async batch handler synchronously via asyncio.run().
    On success: commit all offsets.
    On PermanentFailure: DLQ all messages, commit offsets.
    On transient Exception: exponential backoff retry, then DLQ.
    """
    items = batches.pop(job_id, [])
    if not items:
        return

    messages = [msg for msg, _ in items]
    payloads  = [p   for _, p  in items]

    logger.info("Flushing batch | job=%s | pairs=%d", job_id, len(payloads))

    attempt = 0
    backoff = _INITIAL_BACKOFF

    while attempt < _MAX_RETRIES:
        try:
            asyncio.run(handler(payloads))

            # Success — commit all offsets in this batch
            for msg in messages:
                consumer.commit(message=msg, asynchronous=False)
            logger.info("Batch committed | job=%s | pairs=%d", job_id, len(payloads))
            return

        except PermanentFailure as e:
            logger.error(
                "PermanentFailure for job=%s: %s — routing %d pairs to DLQ",
                job_id, e, len(messages),
            )
            for msg in messages:
                _send_to_dlq(dlq_producer, msg, str(e), attempt + 1)
                consumer.commit(message=msg, asynchronous=False)
            return

        except Exception as e:
            attempt += 1
            if attempt >= _MAX_RETRIES:
                logger.error(
                    "Exhausted %d retries for job=%s — routing %d pairs to DLQ: %s",
                    _MAX_RETRIES, job_id, len(messages), e,
                )
                for msg in messages:
                    _send_to_dlq(dlq_producer, msg, str(e), attempt)
                    consumer.commit(message=msg, asynchronous=False)
                return

            jitter = backoff * (0.5 + random.random())
            logger.warning(
                "Attempt %d/%d failed for job=%s — retrying in %.2fs: %s",
                attempt, _MAX_RETRIES, job_id, jitter, e,
            )
            time.sleep(jitter)
            backoff = min(backoff * _BACKOFF_MULT, _MAX_BACKOFF)


# ── Internal helpers ──────────────────────────────────────────────────────────

def _decode_message(msg: Message) -> dict[str, Any] | None:
    """Decode Kafka message value as JSON. Returns None on malformed input."""
    try:
        raw = msg.value()
        if raw is None:
            return None
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError) as e:
        logger.error(
            "Malformed JSON at %s/%d/offset=%s: %s",
            msg.topic(), msg.partition(), msg.offset(), e,
        )
        return None


def _send_to_dlq(
    producer: Producer,
    msg: Message,
    error: str,
    attempt: int,
) -> None:
    """Route a failed message to the dead-letter topic with diagnostic headers."""
    producer.produce(
        topic=_DLQ_TOPIC,
        value=msg.value(),
        key=msg.key(),
        headers={
            "original-topic":     msg.topic() or "suspicious-pairs",
            "original-partition": str(msg.partition()),
            "original-offset":    str(msg.offset()),
            "failure-reason":     error[:500],
            "failed-attempts":    str(attempt),
            "failed-at":          str(int(time.time())),
            "worker":             "ai-worker",
        },
    )
    producer.poll(0)
    logger.warning("Routed message to DLQ | error=%s", error[:200])


def _handle_kafka_error(msg: Message) -> None:
    """Log Kafka consumer errors, ignoring normal PARTITION_EOF."""
    err = msg.error()
    if err.code() == KafkaError._PARTITION_EOF:
        return   # Normal — reached end of partition
    logger.error("Kafka consumer error: %s", err)
