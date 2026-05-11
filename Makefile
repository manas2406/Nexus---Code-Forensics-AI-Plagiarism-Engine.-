# Makefile
# Usage: make <target>
# Run 'make help' to list all targets.

.PHONY: help infra-up infra-down infra-reset infra-validate \
        dev-up dev-down logs-kafka logs-minio logs-redis \
        kafka-topics kafka-produce kafka-consume kafka-lag \
        redis-cli redis-flush redis-job \
        worker-build hash-shell \
        test-unit test-integration clean

# Detect Docker network name dynamically
NETWORK := $(shell docker network ls --format '{{.Name}}' 2>/dev/null | grep nexus-net | head -1)

# ── Default target ─────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Nexus Development Commands"
	@echo "======================================================"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make infra-up          Start Kafka, Zookeeper, MinIO, Redis"
	@echo "  make infra-down        Stop and remove all containers"
	@echo "  make infra-reset       Full teardown + restart (destroys volumes)"
	@echo "  make infra-validate    Run full infra validation suite"
	@echo ""
	@echo "Developer Tooling:"
	@echo "  make dev-up            Start infra + Kafka UI + Redis UI"
	@echo "  make dev-down          Stop everything including UI tools"
	@echo ""
	@echo "Kafka:"
	@echo "  make kafka-topics      List all topics with partition details"
	@echo "  make kafka-produce T=<topic>   Interactive producer for a topic"
	@echo "  make kafka-consume T=<topic>   Tail messages from beginning"
	@echo "  make kafka-lag         Show consumer group lag"
	@echo ""
	@echo "Redis:"
	@echo "  make redis-cli         Open interactive Redis CLI"
	@echo "  make redis-flush       DANGER: flush all Redis keys (dev only)"
	@echo "  make redis-job J=<id>  Show job status hash for a job ID"
	@echo ""
	@echo "Workers:"
	@echo "  make worker-build      Build all worker Docker images"
	@echo "  make hash-shell        Shell into hash-worker container"
	@echo ""
	@echo "Testing:"
	@echo "  make test-unit         Run unit tests (no infra required)"
	@echo "  make test-integration  Run integration tests (requires infra-up)"
	@echo ""
	@echo "Logs:"
	@echo "  make logs-kafka        Tail Kafka logs"
	@echo "  make logs-minio        Tail MinIO + init logs"
	@echo "  make logs-redis        Tail Redis logs"
	@echo ""

# ── Infrastructure ─────────────────────────────────────────────────────────────

infra-up:
	docker compose up kafka zookeeper minio redis --wait
	@echo "✓ Infra is up. Run 'make infra-validate' to verify."

infra-down:
	docker compose --profile dev down

infra-reset:
	@echo "WARNING: This destroys all Kafka messages, MinIO objects, and Redis keys."
	docker compose --profile dev down -v
	docker compose up kafka zookeeper minio redis --wait
	@echo "✓ Infra reset complete."

infra-validate:
	@bash infra/validate.sh

dev-up:
	docker compose --profile dev up kafka zookeeper minio redis kafka-ui redis-ui --wait
	@echo ""
	@echo "✓ Dev stack is up."
	@echo "  Kafka UI:  http://localhost:8080"
	@echo "  Redis UI:  http://localhost:8081"
	@echo "  MinIO UI:  http://localhost:9001"

dev-down:
	docker compose --profile dev down

# ── Logs ───────────────────────────────────────────────────────────────────────

logs-kafka:
	docker compose logs kafka kafka-init -f --tail=50

logs-minio:
	docker compose logs minio minio-init -f --tail=50

logs-redis:
	docker compose logs redis -f --tail=50

# ── Kafka helpers ──────────────────────────────────────────────────────────────

kafka-topics:
	docker run --rm --network $(NETWORK) \
	  confluentinc/cp-kafka:7.6.1 \
	  kafka-topics --bootstrap-server kafka:9092 --describe

kafka-produce:
ifndef T
	$(error Usage: make kafka-produce T=<topic-name>)
endif
	docker run --rm -it --network $(NETWORK) \
	  confluentinc/cp-kafka:7.6.1 \
	  kafka-console-producer \
	    --broker-list kafka:9092 \
	    --topic $(T) \
	    --property "parse.key=true" \
	    --property "key.separator=:"

kafka-consume:
ifndef T
	$(error Usage: make kafka-consume T=<topic-name>)
endif
	docker run --rm -it --network $(NETWORK) \
	  confluentinc/cp-kafka:7.6.1 \
	  kafka-console-consumer \
	    --bootstrap-server kafka:9092 \
	    --topic $(T) \
	    --from-beginning \
	    --property "print.key=true" \
	    --property "print.timestamp=true"

kafka-lag:
	docker run --rm --network $(NETWORK) \
	  confluentinc/cp-kafka:7.6.1 \
	  kafka-consumer-groups \
	    --bootstrap-server kafka:9092 \
	    --describe \
	    --all-groups

# ── Redis helpers ─────────────────────────────────────────────────────────────

redis-cli:
	docker exec -it nexus-redis redis-cli

redis-flush:
	@echo "WARNING: This deletes ALL Redis keys."
	docker exec nexus-redis redis-cli FLUSHALL

redis-job:
ifndef J
	$(error Usage: make redis-job J=<job-id>)
endif
	@echo "Job status for: $(J)"
	docker exec nexus-redis redis-cli HGETALL "job:$(J):status"

# ── Worker helpers ─────────────────────────────────────────────────────────────

worker-build:
	docker build services/hash-worker -t nexus-hash-worker:dev
	docker build services/ai-worker   -t nexus-ai-worker:dev
	@echo "✓ Worker images built."

hash-shell:
	docker run --rm -it \
	  --network $(NETWORK) \
	  -e KAFKA_BROKERS=kafka:9092 \
	  -e REDIS_HOST=redis \
	  -e MINIO_ENDPOINT=minio:9000 \
	  -e MINIO_ACCESS_KEY=nexus \
	  -e MINIO_SECRET_KEY=nexus-secret-change-in-prod \
	  -v "$(CURDIR)/services/hash-worker:/app:ro" \
	  nexus-hash-worker:dev \
	  bash

# ── Testing ───────────────────────────────────────────────────────────────────

test-unit:
	cd services/hash-worker && \
	  python -m pytest tests/unit/ -v --tb=short

test-integration:
	cd services/hash-worker && \
	  MINIO_ENDPOINT=localhost:9000 \
	  REDIS_HOST=localhost \
	  KAFKA_BROKERS=localhost:9093 \
	  python -m pytest tests/integration/ -v --tb=short

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -name "*.pyc" -delete 2>/dev/null || true
	@echo "✓ Cleaned Python caches."
