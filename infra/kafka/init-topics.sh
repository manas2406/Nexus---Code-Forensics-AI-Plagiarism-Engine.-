#!/usr/bin/env bash
set -euo pipefail

BROKER="kafka:29092"

echo "[kafka-init] Waiting for Kafka to be ready…"

create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="$3"

  echo "[kafka-init] Creating topic: ${name} (partitions=${partitions}, retention=${retention_ms}ms)"
  kafka-topics --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${name}" \
    --partitions "${partitions}" \
    --config retention.ms="${retention_ms}"
}

# 7 days = 604800000 ms
# 14 days = 1209600000 ms
create_topic "submissions"      3 604800000
create_topic "job-status"       3 604800000
create_topic "suspicious-pairs" 3 604800000
create_topic "results"          3 604800000
create_topic "dlq"              1 1209600000

echo "[kafka-init] All topics created."
