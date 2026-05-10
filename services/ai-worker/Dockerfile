# services/ai-worker/Dockerfile
#
# Simpler than hash-worker — no tree-sitter C extensions.
# AI worker is I/O bound (LLM API calls), not CPU bound.

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY *.py ./

RUN useradd -m -u 1000 worker
USER worker

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import confluent_kafka; import httpx; print('ok')" || exit 1

CMD ["python", "main.py"]
