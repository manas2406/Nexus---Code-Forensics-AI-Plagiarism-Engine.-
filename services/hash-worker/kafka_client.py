"""Kafka consumer/producer infrastructure for the Nexus hash-worker.

Infrastructure-only module — never imports from algorithm files (pipeline,
ast_engine, winnowing, comparator, lsh_index).  Provides a robust consumer
base that the handler plugs into.

Consumer group: ``hash-workers`` (Dev A's API gateway expects this exact name).

Topic defaults (overridable via env vars):
    KAFKA_INPUT_TOPIC           = submissions
    KAFKA_SUSPICIOUS_PAIRS_TOPIC = suspicious-pairs
    KAFKA_RESULTS_TOPIC         = forensic-results
    KAFKA_DLQ_TOPIC             = dead-letter
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable

from kafka import KafkaConsumer, KafkaProducer  # type: ignore[import-untyped]
from kafka.errors import KafkaError  # type: ignore[import-untyped]

logger = logging.getLogger("nexus.kafka-client")


@dataclass
class KafkaConfig:
    """Configuration for the hash-worker Kafka client.

    All topic names are overridable via environment variables.
    """

    broker: str                          # e.g. "localhost:9092"
    consumer_group: str                  # "hash-workers"
    input_topic: str                     # "submissions"
    suspicious_pairs_topic: str          # "suspicious-pairs"
    results_topic: str                   # "forensic-results"
    dlq_topic: str                       # "dead-letter"
    max_retries: int = 3
    retry_backoff_seconds: float = 2.0
    poll_timeout_ms: int = 1000


# Type alias for the async message handler function
MessageHandler = Callable[[dict[str, object]], Awaitable[None]]


class HashWorkerKafkaClient:
    """Kafka consumer + producer for the hash-worker service.

    The consumer reads ``JOB_CREATED`` events from the input topic.
    The producer publishes suspicious pairs, job-complete events, and
    routes unrecoverable failures to the dead-letter queue.

    Manual offset commit only — never auto-commits.
    """

    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._shutdown = False

        self._consumer = KafkaConsumer(
            config.input_topic,
            bootstrap_servers=config.broker,
            group_id=config.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        self._producer = KafkaProducer(
            bootstrap_servers=config.broker,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",              # wait for all replicas
            retries=5,
            enable_idempotence=True,
        )

        logger.info(
            "Kafka client initialised | broker=%s | group=%s | input=%s",
            config.broker, config.consumer_group, config.input_topic,
        )

    @property
    def shutdown(self) -> bool:
        """Whether shutdown has been requested (e.g. via SIGTERM)."""
        return self._shutdown

    @shutdown.setter
    def shutdown(self, value: bool) -> None:
        self._shutdown = value

    def start(self, handler: MessageHandler) -> None:
        """Start consuming from input_topic in a blocking loop.

        For each message:
          1. Deserialise JSON payload (done by consumer's value_deserializer)
          2. Call ``handler(payload)`` with retry logic
          3. On success: commit offset manually
          4. On failure after MAX_RETRIES: route to DLQ, commit offset

        Never stops unless ``self.shutdown`` is set to ``True`` (via SIGTERM).
        """
        logger.info(
            "[hash-worker] Consuming from '%s' topic — waiting for messages...",
            self._config.input_topic,
        )

        loop = asyncio.new_event_loop()

        try:
            while not self._shutdown:
                # poll() returns dict of {TopicPartition: [ConsumerRecord, ...]}
                records = self._consumer.poll(
                    timeout_ms=self._config.poll_timeout_ms,
                )

                if not records:
                    continue

                for _tp, messages in records.items():
                    for message in messages:
                        if self._shutdown:
                            # Drain: finish current message, then stop
                            logger.info(
                                "[hash-worker] Shutdown flag set — "
                                "completing in-flight message before exit"
                            )

                        payload = message.value
                        raw_message = (
                            json.dumps(payload).encode("utf-8")
                            if isinstance(payload, dict)
                            else message.value
                        )

                        self._process_with_retry(
                            handler, payload, raw_message, loop,
                        )

                        if self._shutdown:
                            logger.info(
                                "[hash-worker] In-flight message processed — "
                                "exiting consumer loop"
                            )
                            return

        except KeyboardInterrupt:
            logger.info("[hash-worker] KeyboardInterrupt — shutting down")
        finally:
            loop.close()
            logger.info("[hash-worker] Consumer loop ended")

    def _process_with_retry(
        self,
        handler: MessageHandler,
        payload: dict[str, object],
        raw_message: bytes,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Execute handler with retry logic.

        - ValueError skips retries entirely (bad payload / empty ZIP).
        - Other exceptions are retried up to max_retries with linear backoff.
        - After exhausting retries, the message is routed to the DLQ.
        - Offset is committed only after success or DLQ routing.
        """
        config = self._config

        for attempt in range(1, config.max_retries + 1):
            try:
                loop.run_until_complete(handler(payload))
                # Success — commit and return
                self._consumer.commit()
                return
            except ValueError as e:
                # Unrecoverable — skip retries, route to DLQ immediately
                logger.warning(
                    "[hash-worker] ValueError on attempt %d/%d — "
                    "routing to DLQ (no retry): %s",
                    attempt, config.max_retries, e,
                )
                self.produce_to_dlq(raw_message, str(e))
                self._consumer.commit()
                return
            except Exception as e:
                if attempt == config.max_retries:
                    logger.error(
                        "[hash-worker] Handler failed after %d attempts — "
                        "routing to DLQ: %s",
                        config.max_retries, e,
                    )
                    self.produce_to_dlq(raw_message, str(e))
                    self._consumer.commit()
                    return
                else:
                    backoff = config.retry_backoff_seconds * attempt
                    logger.warning(
                        "[hash-worker] Attempt %d/%d failed (%s) — "
                        "retrying in %.1fs",
                        attempt, config.max_retries, e, backoff,
                    )
                    loop.run_until_complete(asyncio.sleep(backoff))

    def produce_suspicious_pair(self, pair: dict[str, object]) -> None:
        """Publish a SuspiciousPairEvent to the suspicious-pairs topic."""
        self._producer.send(
            self._config.suspicious_pairs_topic,
            value=pair,
        )
        self._producer.flush()
        logger.debug(
            "Published suspicious pair | pairId=%s",
            pair.get("pairId", "unknown"),
        )

    def produce_job_complete(self, job_id: str, result_key: str) -> None:
        """Publish a JOB_COMPLETE event to the results topic.

        Payload:
            ``{ "jobId": ..., "resultKey": ..., "completedAt": ISO8601 }``
        """
        event: dict[str, object] = {
            "jobId": job_id,
            "resultKey": result_key,
            "completedAt": datetime.now(timezone.utc).isoformat(),
        }
        self._producer.send(self._config.results_topic, value=event)
        self._producer.flush()
        logger.info(
            "Published JOB_COMPLETE | jobId=%s | resultKey=%s",
            job_id, result_key,
        )

    def produce_to_dlq(self, original_message: bytes, error: str) -> None:
        """Route a failed message to the dead-letter queue topic.

        Payload:
            ``{ "originalPayload": base64, "error": str, "failedAt": ISO8601 }``
        """
        event: dict[str, object] = {
            "originalPayload": base64.b64encode(original_message).decode("ascii"),
            "error": error,
            "failedAt": datetime.now(timezone.utc).isoformat(),
        }
        self._producer.send(self._config.dlq_topic, value=event)
        self._producer.flush()
        logger.warning(
            "Routed message to DLQ | error=%s", error[:200],
        )

    def health_check(self) -> bool:
        """Return True if the Kafka broker is reachable. Never raises."""
        try:
            metadata = self._producer.bootstrap_connected()
            return bool(metadata)
        except Exception:
            return False

    def close(self) -> None:
        """Flush producer, close consumer cleanly."""
        try:
            self._producer.flush(timeout=10)
            self._producer.close(timeout=10)
        except Exception as e:
            logger.warning("Error closing producer: %s", e)

        try:
            self._consumer.close()
        except Exception as e:
            logger.warning("Error closing consumer: %s", e)

        logger.info("[hash-worker] Kafka client closed")
