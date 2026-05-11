#!/usr/bin/env bash
# infra/validate.sh
#
# Nexus Infrastructure Validation Script
# Run from repo root: bash infra/validate.sh
#
# Exit codes:
#   0 = all checks passed
#   1 = one or more checks failed

set -uo pipefail

PASS=0
FAIL=0
ERRORS=()

# ── Helpers ────────────────────────────────────────────────────────────────────
green()  { echo -e "\033[0;32m  ✓ $1\033[0m"; }
red()    { echo -e "\033[0;31m  ✗ $1\033[0m"; ERRORS+=("$1"); }
header() { echo -e "\n\033[1;34m── $1 ──────────────────────────────────\033[0m"; }

check() {
  local DESC="$1"
  shift
  if "$@" &>/dev/null; then
    green "$DESC"
    ((PASS++))
  else
    red "$DESC"
    ((FAIL++))
  fi
}

# Detect Docker network name (depends on project directory name)
NETWORK=$(docker network ls --format '{{.Name}}' | grep nexus-net | head -1)
if [ -z "$NETWORK" ]; then
  echo "ERROR: No Docker network matching 'nexus-net' found."
  echo "Run 'docker compose up -d' first."
  exit 1
fi
echo "Using Docker network: $NETWORK"

# ── Docker service health ──────────────────────────────────────────────────────
header "Docker Service Health"

for service in nexus-kafka nexus-zookeeper nexus-minio nexus-redis; do
  STATUS=$(docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null || echo "missing")
  if [ "$STATUS" = "healthy" ]; then
    green "$service is healthy"
    ((PASS++))
  else
    red "$service is $STATUS (expected: healthy)"
    ((FAIL++))
  fi
done

# ── Kafka topics ──────────────────────────────────────────────────────────────
header "Kafka Topics"

TOPICS=("submissions" "suspicious-pairs" "forensic-results" "job-lifecycle" "dead-letter")
for topic in "${TOPICS[@]}"; do
  check "Topic exists: $topic" \
    docker run --rm --network "$NETWORK" \
      confluentinc/cp-kafka:7.6.1 \
      kafka-topics --bootstrap-server kafka:9092 --describe --topic "$topic"
done

# Verify partition counts
check "submissions has 4 partitions" bash -c "
  docker run --rm --network $NETWORK confluentinc/cp-kafka:7.6.1 \
    kafka-topics --bootstrap-server kafka:9092 --describe --topic submissions \
    | grep -q 'PartitionCount: 4'
"
check "suspicious-pairs has 6 partitions" bash -c "
  docker run --rm --network $NETWORK confluentinc/cp-kafka:7.6.1 \
    kafka-topics --bootstrap-server kafka:9092 --describe --topic suspicious-pairs \
    | grep -q 'PartitionCount: 6'
"
check "forensic-results has 2 partitions" bash -c "
  docker run --rm --network $NETWORK confluentinc/cp-kafka:7.6.1 \
    kafka-topics --bootstrap-server kafka:9092 --describe --topic forensic-results \
    | grep -q 'PartitionCount: 2'
"
check "dead-letter has 1 partition" bash -c "
  docker run --rm --network $NETWORK confluentinc/cp-kafka:7.6.1 \
    kafka-topics --bootstrap-server kafka:9092 --describe --topic dead-letter \
    | grep -q 'PartitionCount: 1'
"

# ── Kafka external listener (host access) ─────────────────────────────────────
header "Kafka External Listener (Host → Container)"

# This tests that a producer on the HOST machine can reach Kafka via localhost:9093.
# If this fails, Dev B's pytest suite cannot produce test messages.
check "EXTERNAL listener reachable at localhost:9093" bash -c "
  echo 'validation-test' | timeout 10 docker run --rm -i --network host \
    confluentinc/cp-kafka:7.6.1 \
    kafka-console-producer --broker-list localhost:9093 --topic job-lifecycle 2>&1 \
    | grep -v 'LEADER_NOT_AVAILABLE' | grep -v 'UnknownTopicOrPartition'
"

# ── MinIO ─────────────────────────────────────────────────────────────────────
header "MinIO Buckets & Lifecycle"

check "Bucket nexus-submissions exists" bash -c "
  docker run --rm --network $NETWORK --entrypoint sh minio/mc:latest \
    -c 'mc alias set local http://minio:9000 nexus nexus-secret-change-in-prod >/dev/null 2>&1 && \
        mc ls local/nexus-submissions >/dev/null 2>&1'
"
check "Bucket nexus-reports exists" bash -c "
  docker run --rm --network $NETWORK --entrypoint sh minio/mc:latest \
    -c 'mc alias set local http://minio:9000 nexus nexus-secret-change-in-prod >/dev/null 2>&1 && \
        mc ls local/nexus-reports >/dev/null 2>&1'
"
check "nexus-submissions lifecycle policy applied" bash -c "
  docker run --rm --network $NETWORK --entrypoint sh minio/mc:latest \
    -c 'mc alias set local http://minio:9000 nexus nexus-secret-change-in-prod >/dev/null 2>&1 && \
        mc ilm ls local/nexus-submissions' | grep -q 'expire-raw-submissions'
"

# ── Redis ─────────────────────────────────────────────────────────────────────
header "Redis"

check "Redis PING responds" bash -c "
  docker exec nexus-redis redis-cli ping | grep -q PONG
"
check "AOF persistence enabled" bash -c "
  docker exec nexus-redis redis-cli CONFIG GET appendonly | grep -q yes
"
check "maxmemory-policy is volatile-lru" bash -c "
  docker exec nexus-redis redis-cli CONFIG GET maxmemory-policy | grep -q volatile-lru
"

# Redis persistence validation: write key, verify it's readable
check "Redis can write and read keys" bash -c "
  docker exec nexus-redis redis-cli SET nexus:validate:test 'phase1-ok' EX 60 >/dev/null && \
  docker exec nexus-redis redis-cli GET nexus:validate:test | grep -q 'phase1-ok' && \
  docker exec nexus-redis redis-cli DEL nexus:validate:test >/dev/null
"

# ── Host access ───────────────────────────────────────────────────────────────
header "Host Access (for Dev B's Python tests)"

check "MinIO S3 API reachable at localhost:9000" bash -c "
  curl -sf -o /dev/null http://localhost:9000/minio/health/live
"
check "Redis reachable at localhost:6379" bash -c "
  docker exec nexus-redis redis-cli -h localhost ping | grep -q PONG
"

# ── Dev tooling (optional — only if running with --profile dev) ────────────────
if docker inspect nexus-kafka-ui &>/dev/null; then
  header "Developer Tooling"

  STATUS=$(docker inspect --format='{{.State.Status}}' nexus-kafka-ui 2>/dev/null || echo "missing")
  if [ "$STATUS" = "running" ]; then
    green "Kafka UI (nexus-kafka-ui) is running"
    ((PASS++))
  else
    red "Kafka UI (nexus-kafka-ui) is $STATUS (expected: running)"
    ((FAIL++))
  fi

  STATUS=$(docker inspect --format='{{.State.Status}}' nexus-redis-ui 2>/dev/null || echo "missing")
  if [ "$STATUS" = "running" ]; then
    green "Redis UI (nexus-redis-ui) is running"
    ((PASS++))
  else
    red "Redis UI (nexus-redis-ui) is $STATUS (expected: running)"
    ((FAIL++))
  fi
fi

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════"
echo "Validation Summary"
echo "══════════════════════════════════════════════════"
echo "  Passed: ${PASS}"
echo "  Failed: ${FAIL}"

if [ "${FAIL}" -gt 0 ]; then
  echo ""
  echo "Failed checks:"
  for err in "${ERRORS[@]}"; do
    echo "  ✗ $err"
  done
  echo ""
  exit 1
else
  echo ""
  echo "  All checks passed. Infra is ready for Phase 2."
  echo ""
  exit 0
fi
