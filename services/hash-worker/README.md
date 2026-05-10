# Nexus Hash Worker

C++ AST parsing, Winnowing fingerprinting, and pairwise Jaccard similarity comparison.

## Local Setup

### Option A: uv (recommended — 10× faster than pip)

```bash
pip install uv
cd services/hash-worker
uv venv .venv
.venv\Scripts\activate       # Linux/Mac: source .venv/bin/activate
uv pip install -e ".[dev]"
```

### Option B: standard pip

```bash
cd services/hash-worker
python -m venv .venv
.venv\Scripts\activate       # Linux/Mac: source .venv/bin/activate
pip install -e ".[dev]"
```

## Running Tests

### Unit tests (no Docker required)

```bash
pytest tests/unit/ -v
```

### Integration tests (requires running infra)

```bash
# First, start infrastructure from repo root:
# docker compose up -d

# Then run with env vars pointing at host-exposed ports:
$env:MINIO_ENDPOINT="localhost:9000"
$env:REDIS_HOST="localhost"
$env:KAFKA_BROKERS="localhost:9093"
pytest tests/integration/ -v
```

## Project Structure

```
services/hash-worker/
├── Dockerfile              # Multi-stage production build
├── pyproject.toml           # Dependencies + tool config
├── requirements.txt         # Flat deps for Docker builds
├── README.md
├── src/
│   ├── main.py              # Kafka consumer loop (Phase 2)
│   ├── ast_engine.py        # Tree-sitter C++ tokenizer
│   ├── winnowing.py         # Winnowing fingerprint generator
│   └── comparator.py        # Pairwise Jaccard similarity
└── tests/
    ├── conftest.py          # Shared fixtures (MinIO, Redis, ZIP factory)
    ├── unit/                # No infra needed
    └── integration/         # Requires docker compose up
```
