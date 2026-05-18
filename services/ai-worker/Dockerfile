# services/ai-worker/Dockerfile
#
# The ai-worker is predominantly I/O bound — waiting on:
#   1. Kafka poll (fast, milliseconds)
#   2. MinIO GET for source code (fast, <1s)
#   3. LLM API call (SLOW — 5–30 seconds per pair)
#
# Because of (3), the worker is async throughout. asyncio lets a single
# process handle multiple in-flight LLM calls concurrently without threads.
# CPU usage is near-zero — the bottleneck is always the LLM API.

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# confluent-kafka requires librdkafka C library + build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime-only: librdkafka shared library (no build tools)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY *.py ./

RUN useradd -m -u 1000 worker
USER worker

# Health check: verify all imports work AND LLM_API_KEY is configured.
# Fast-fail if the key is missing — better than silently processing with no LLM.
HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "\
import confluent_kafka, httpx, minio, redis, networkx; \
import os; \
assert os.environ.get('LLM_API_KEY'), 'LLM_API_KEY not set — AI worker cannot function'; \
print('ok')" || exit 1

CMD ["python", "main.py"]
