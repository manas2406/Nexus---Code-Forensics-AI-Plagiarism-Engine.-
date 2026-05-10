# services/hash-worker/Dockerfile
#
# Multi-stage build:
# Stage 1 (builder): install all deps including dev tools, compile C extensions
# Stage 2 (runtime): copy only what's needed to run — no pip, no compilers

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build dependencies for C extensions (confluent-kafka, tree-sitter)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first — Docker layer cache optimization
COPY requirements.txt pyproject.toml ./

# Install into an isolated prefix for clean copy to runtime stage
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

# Runtime-only system dependencies (librdkafka for confluent-kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder stage
COPY --from=builder /install /usr/local

# Copy application source
COPY *.py ./

# Non-root user — never run app code as root, even in Docker
RUN useradd -m -u 1000 worker
USER worker

# Healthcheck: verify Python environment is intact
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
  CMD python -c "import confluent_kafka; import tree_sitter; print('ok')" || exit 1

# Entry point — replaced with real main.py in Phase 2
CMD ["python", "main.py"]
