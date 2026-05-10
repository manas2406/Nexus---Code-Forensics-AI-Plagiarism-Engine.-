#!/usr/bin/env bash
# infra/kafka/init-topics.sh
#
# Creates all Nexus Kafka topics with production-grade configs.
# Idempotent: safe to run multiple times (--if-not-exists).
#
# Run context: inside the nexus-kafka-init container, after Kafka is healthy.

set -euo pipefail

BROKER="${KAFKA_BROKER:-kafka:9092}"
REPLICATION=1   # Increase to 3 in a multi-broker production cluster

echo "══════════════════════════════════════════════════════"
echo "Nexus Kafka Topic Initialization"
echo "Broker: ${BROKER}"
echo "══════════════════════════════════════════════════════"

# Helper: create a topic only if it doesn't already exist
create_topic() {
  local TOPIC="$1"
  local PARTITIONS="$2"
  local RETENTION_MS="$3"
  local EXTRA_CONFIGS="${4:-}"

  echo ""
  echo "→ Creating topic: ${TOPIC}"
  echo "  partitions=${PARTITIONS}, retention=${RETENTION_MS}ms"

  kafka-topics \
    --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC}" \
    --partitions "${PARTITIONS}" \
    --replication-factor "${REPLICATION}" \
    --config "retention.ms=${RETENTION_MS}" \
    --config "max.message.bytes=10485760" \
    ${EXTRA_CONFIGS}

  echo "  ✓ ${TOPIC}"
}

# Wait for Kafka to be fully ready (belt-and-suspenders beyond Docker healthcheck)
echo "Waiting for Kafka to be available..."
for i in $(seq 1 30); do
  if kafka-topics --bootstrap-server "${BROKER}" --list &>/dev/null; then
    echo "✓ Kafka is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "✗ Kafka did not become ready in time. Exiting."
    exit 1
  fi
  echo "  Attempt ${i}/30 — sleeping 2s..."
  sleep 2
done

echo ""
echo "── Creating topics ────────────────────────────────────"

# ── submissions ─────────────────────────────────────────────────────────────
# Consumed by: hash-worker
#
# Partitions = 4:
# Each partition is processed by exactly one hash-worker instance at a time.
# 4 partitions = up to 4 parallel hash workers without rebalancing contention.
# The bottleneck is CPU (tree-sitter parsing), not I/O, so 4 is a safe ceiling
# for a single-node dev environment. Scale to 8 in production.
#
# Retention = 7 days:
# If the hash-worker cluster goes down for maintenance, messages must survive.
# 7 days gives ops team time to diagnose and restart without data loss.
create_topic "submissions" 4 604800000

# ── suspicious-pairs ────────────────────────────────────────────────────────
# Consumed by: ai-worker
#
# Partitions = 6:
# AI workers are I/O bound (waiting on LLM API). More partitions = more
# parallelism. 6 lets us run 6 AI worker instances simultaneously.
# Matches LLM_MAX_CONCURRENT=5 per worker (6 workers × 5 concurrent = 30
# max in-flight LLM calls — well within most provider rate limits).
#
# Retention = 7 days:
# Critical: if the LLM API is down for a day, these messages must survive
# for retry. Without this, suspicious pairs are permanently lost.
#
# max.message.bytes: Already set globally via --config above. Both
# fileA and fileB source code travel in this message (~10KB each typical).
create_topic "suspicious-pairs" 6 604800000

# ── forensic-results ────────────────────────────────────────────────────────
# Consumed by: api-gateway (for WebSocket fan-out to browser)
#
# Partitions = 2:
# Low partition count is intentional — the api-gateway fan-out is not the
# bottleneck. 2 gives basic redundancy if one api-gateway instance dies.
#
# Retention = 30 days:
# Forensic results are audit records. Instructors may need to re-query them
# weeks after a submission batch. 30 days gives a reasonable audit window.
create_topic "forensic-results" 2 2592000000

# ── job-lifecycle ────────────────────────────────────────────────────────────
# Status update events: PENDING → EXTRACTING → PARSING → HASHING → AI_ANALYSIS → COMPLETE
# Consumed by: api-gateway → Redis Pub/Sub → GraphQL Subscription → WebSocket → Browser
#
# Partitions = 4:
# Matches submissions topic. One partition per hash-worker instance producing
# updates. Keeps events for one job ordered (same partition key = jobId).
#
# Retention = 24 hours:
# Status events are ephemeral UI updates. After a job completes, historical
# status transitions have no value. 24h is generous for debugging.
create_topic "job-lifecycle" 4 86400000

# ── dead-letter ─────────────────────────────────────────────────────────────
# Receives messages that exhausted all retry attempts.
# Consumed by: a future DLQ monitor service (Phase 6).
# Contains original message payload + diagnostic headers.
#
# Partitions = 1:
# DLQ messages are rare operational exceptions, not high-throughput.
# Single partition makes it trivial to consume and inspect sequentially.
#
# Retention = 30 days:
# These are failure records. Engineering needs them for post-mortems.
# Never set this lower — you need the corpus to tune your retry logic.
create_topic "dead-letter" 1 2592000000

echo ""
echo "── Verification ───────────────────────────────────────"
echo "Created topics:"
kafka-topics \
  --bootstrap-server "${BROKER}" \
  --list | grep -E "^(submissions|suspicious-pairs|forensic-results|job-lifecycle|dead-letter)$"

echo ""
echo "Topic details:"
kafka-topics \
  --bootstrap-server "${BROKER}" \
  --describe \
  --topic submissions,suspicious-pairs,forensic-results,job-lifecycle,dead-letter

echo ""
echo "══════════════════════════════════════════════════════"
echo "✓ All Nexus topics initialized successfully"
echo "══════════════════════════════════════════════════════"
