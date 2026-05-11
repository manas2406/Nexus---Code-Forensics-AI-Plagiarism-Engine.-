# Nexus — Code Forensics & AI Plagiarism Engine

Nexus is an event-driven code-plagiarism detection platform that ingests student submissions, fingerprints source files using Tree-sitter AST hashing and MinHash (Jaccard) similarity, and escalates suspicious pairs to an LLM-powered forensic analyser that identifies obfuscation techniques and renders a verdict. The system is built as a polyglot monorepo with a Next.js frontend, an Express API gateway, Python worker microservices, and infrastructure powered by Kafka, MinIO, and Redis.

## Quick Start

```bash
# 1. Copy environment variables
cp .env.example .env

# 2. Start all infrastructure services
docker compose up -d

# 3. Install Node.js dependencies
pnpm install

# 4. (Optional) Start with developer UI tools
make dev-up
```

## Developer Tooling

| Tool | URL | Purpose |
|------|-----|---------|
| **Kafka UI** (Redpanda Console) | http://localhost:8080 | Browse topics, inspect messages, monitor consumer lag |
| **Redis Commander** | http://localhost:8081 | Browse keys, inspect job status hashes |
| **MinIO Console** | http://localhost:9001 | Browse buckets, manage objects (user: `nexus`) |

Start with `make dev-up` or `docker compose --profile dev up`.

## Useful Commands

```bash
make help              # List all available commands
make infra-validate    # Run full infrastructure validation (21+ checks)
make kafka-topics      # List all Kafka topics with partition details
make worker-build      # Build hash-worker and ai-worker Docker images
make test-unit         # Run unit tests (no Docker required)
make test-integration  # Run integration tests (requires infra-up)
```

## Project Status

### Phase 0 — Complete ✅
- Perf: 0.2ms per file (100 synthetic files)
- Infra: all 5 services healthy on `docker compose up`
- Tests: 6/6 algorithm tests passing

### Phase 1 — Complete ✅
- Infrastructure validation: 21+ automated checks (`make infra-validate`)
- Developer observability: Kafka UI + Redis UI via `make dev-up`
- Python environment: `pyproject.toml` with pinned deps + dev tools
- Worker Dockerfiles: multi-stage builds (hash-worker ~552MB, ai-worker ~253MB)
- Algorithm pipeline: batch processing, LSH pre-filter, benchmark (N=500)
- Schema contract: `shared/types/index.ts` with MINIO_PATHS, CONSUMER_GROUPS
- Test fixtures: `conftest.py` with MinIO + Redis + ZIP factory fixtures
