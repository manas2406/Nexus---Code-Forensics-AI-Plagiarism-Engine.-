# Nexus — Phase-wise Work Distribution
### 2-Person Team: Dev A (Full-Stack / Infra) · Dev B (Full-Stack + ML)

---

## Team Roles

| Person | Strengths | Primary Ownership |
|--------|-----------|-------------------|
| **Dev A** | Full-stack, infra, backend systems | API gateway, Kafka topology, Docker, frontend |
| **Dev B** | Full-stack + ML | AST engine, winnowing, AI worker, algorithm correctness |

Each phase ends with an **integration checkpoint** — a concrete, testable deliverable both people verify together before moving on.

---

## Phase 0 — Algorithm Lab *(~2–3 days)*
> **Goal:** Prove the core math works before touching any infrastructure.

### Dev B (solo)
- Implement `ast_engine.py` — tree-sitter C++ traversal, structural token extraction, ERROR node handling
- Implement `winnowing.py` — Rabin–Karp rolling hash + Winnowing fingerprint selection
- Implement `comparator.py` — pairwise Jaccard similarity using set intersection
- Write `pytest` suite covering:
  - Variable-renamed pairs (expect ≥ 70% similarity)
  - Loop-restructured pairs
  - Files with syntax errors (must not crash, must return partial tokens)
  - Completely dissimilar files (expect < 20% similarity)
- Measure wall-clock time for N = 100 files

### Dev A (solo)
- Set up the monorepo skeleton (`pnpm workspaces`, folder structure per the spec)
- Write `docker-compose.yml` stubs for all services (images only, no app services yet)
- Write `infra/kafka/init-topics.sh` with correct partition/retention config
- Write `infra/minio/init-buckets.sh`

### ✅ Checkpoint 0
- Dev B's pytest suite passes with correct similarity scores
- Dev A's `docker-compose up kafka zookeeper minio redis` runs clean
- Both review the token output of `ast_engine.py` on a real C++ file together

---

## Phase 1 — Storage & Infrastructure Skeleton *(~1–2 days)*
> **Goal:** All stateful services are up and reachable. Nothing app-level talks to them yet.

### Dev A
- Bring up and validate: Kafka, Zookeeper, MinIO, Redis
- Confirm topic creation via `kafka-topics.sh --list`
- Confirm MinIO bucket `nexus-submissions` is created
- Write `redis.conf` with persistence settings
- Add healthchecks to all infra services in `docker-compose.yml`

### Dev B
- Wrap `ast_engine.py` + `winnowing.py` into a single callable pipeline function: `process_file(path) -> ParseResult + fingerprint`
- Add MinHash + LSH pre-filter using `datasketch` to reduce O(N²) comparisons
- Benchmark the full pipeline on N = 500 simulated files, record throughput

### ✅ Checkpoint 1
- `docker-compose up` brings up all infra services healthy
- Dev B demos the LSH pipeline on 500 files with timing results
- Team agrees on Kafka topic names and message schemas (`events.ts` / `events.proto`)

---

## Phase 2 — Hash Worker (Standalone, No Kafka) *(~3–4 days)*
> **Goal:** The full AST → fingerprint → similarity pipeline runs end-to-end as a self-contained script, reading from MinIO.

### Dev A
- Implement `minio.client.ts` (MinIO SDK wrapper for the API side)
- Write the MinIO streaming ZIP extractor (`stream_zip_entries`) in Python for the worker
- Implement Redis state manager `state.py`: `update_job_status`, TTL handling, pub/sub publish skeleton
- Write the `kafka_client.py` consumer base with manual commit + DLQ logic (no handler wired yet)

### Dev B
- Wire `ast_engine.py` + `winnowing.py` + `comparator.py` into `main.py` as a single pipeline
- Integrate LSH pre-filter before full Jaccard computation
- Implement the suspicious pair threshold logic (configurable, default 0.6)
- Add file-level guards: skip files > 500 KB, handle non-UTF-8 encodings gracefully
- Write integration test: feed a real ZIP from MinIO, assert correct pairs are flagged

### ✅ Checkpoint 2
- Running `python main.py --zip s3://nexus-submissions/test.zip` produces a JSON list of suspicious pairs
- Redis `job:{id}:status` is updated correctly throughout the run
- Code review: Dev A reviews Dev B's algorithm; Dev B reviews Dev A's Kafka/Redis code

---

## Phase 3 — API Gateway + Kafka Integration *(~3–4 days)*
> **Goal:** HTTP upload → MinIO → Kafka event → 202 response. Hash worker consumes and runs the pipeline.

### Dev A
- Scaffold Apollo Server with `uploadSubmissions` mutation
- Stream ZIP upload to MinIO (avoid buffering the full file in memory)
- Implement idempotent Kafka producer (`producer.ts`) and publish `JOB_CREATED` event
- Add GraphQL Subscription schema for `jobStatusUpdated` (resolver wired to Redis Pub/Sub)
- Wire `publishJobEvent` bridge between Redis channel and GraphQL subscriptions

### Dev B
- Wire `kafka_client.py` consumer to the hash pipeline handler
- Consumer group: `hash-workers`
- After analysis, produce each suspicious pair to the `suspicious-pairs` Kafka topic
- Produce `JOB_COMPLETE` event to results topic on finish
- Test retry logic: simulate a transient failure mid-processing, confirm DLQ routing after `MAX_RETRIES`

### ✅ Checkpoint 3
- Full flow: upload a ZIP via GraphQL mutation → Kafka event received → hash worker processes → suspicious pairs on `suspicious-pairs` topic
- Redis job state transitions correctly: `PENDING → PARSING → HASHING → COMPLETE`
- Manual verification: inspect Kafka topic offsets and DLQ for a deliberate bad message

---

## Phase 4 — AI Forensic Worker *(~3–4 days)*
> **Goal:** AI worker consumes suspicious pairs, calls LLM, stores structured forensic reports.

### Dev A
- Scaffold `ai-worker` Dockerfile and `requirements.txt`
- Implement `kafka_client.py` for the AI worker (reuse base from hash worker, different consumer group: `ai-workers`)
- Implement transitive closure deduplication (`networkx`) to reduce LLM calls on large cheating rings
- Store forensic JSON reports in MinIO at `reports/{jobId}/{pairId}.json`
- Publish report reference to Redis on completion

### Dev B
- Implement `forensic_analyst.py`:
  - `asyncio.Semaphore`-based concurrency cap (`LLM_MAX_CONCURRENT`)
  - Exponential backoff with jitter on 429 and 5xx errors
  - JSON response parsing with markdown fence stripping
  - Fallback report on total LLM failure (never drops a pair)
- Implement `rate_limiter.py` token bucket
- Write prompt engineering tests: verify the system prompt produces valid JSON on at least 10 diverse pair inputs
- Tune `temperature`, `max_tokens`, source truncation threshold

### ✅ Checkpoint 4
- End-to-end: suspicious pair from Kafka → LLM call → `ForensicReport` stored in MinIO
- Semaphore correctly limits concurrent calls (verify with 20 simultaneous pairs)
- Fallback report is generated when LLM is deliberately unreachable
- Cost estimate: run against 50 pairs, record token usage

---

## Phase 5 — Frontend *(~4–5 days)*
> **Goal:** Upload UI → live terminal → interactive similarity graph → forensic report modal.

### Dev A
- Scaffold Next.js app with Apollo Client (HTTP + WebSocket split link)
- Implement `UploadZone.tsx` — drag-and-drop, calls `uploadSubmissions` mutation
- Implement `LiveTerminal.tsx` — subscribes to `jobStatusUpdated`, streams status messages
- Connect job detail page (`/jobs/[jobId]`) to subscription

### Dev B
- Implement `ForensicGraph.tsx` — nodes = submitted files, edges = suspicious pairs weighted by similarity score (React Flow or Cytoscape)
- Implement `ReportModal.tsx` — on edge click, fetch and display the LLM forensic JSON from MinIO
- Style the verdict display: color-coded `LIKELY_PLAGIARISM` / `POSSIBLE_COINCIDENCE` / `INCONCLUSIVE`
- Add obfuscation technique badges and evidence snippet diff view

### ✅ Checkpoint 5
- Full demo flow: upload ZIP → watch live terminal → graph appears with edges → click edge → forensic report modal opens
- Test with a real submission batch containing known plagiarism pairs
- UX review together: terminal latency, graph readability, report clarity

---

## Phase 6 — Hardening *(~3–4 days)*
> **Goal:** Production-ready reliability, observability, graceful degradation.

### Dev A
- Dead-letter queue consumer with structured alerting (log + optional webhook)
- Kafka consumer lag monitoring: Prometheus metrics + Grafana dashboard
- Graceful shutdown: SIGTERM handler drains in-flight messages before exit
- Integration test suite: upload 500-file ZIP, assert all events flow end-to-end with correct state transitions
- `docker-compose.dev.yml` with volume mounts and hot reload for both workers

### Dev B
- Tune `K` and `W` constants in `winnowing.py` based on real submission data; document the tradeoff
- Add per-file processing time metrics to the hash worker
- Handle edge cases: empty ZIP, ZIP with only `.h` files, single-file ZIP
- Stress test: N = 5000 files, measure memory usage, confirm no OOM
- Document the LSH threshold sensitivity (false positive rate vs recall tradeoff)

### ✅ Checkpoint 6 — Ship
- All integration tests pass on a 500-file batch
- Consumer lag stays under 30s under peak load
- Dead-letter queue correctly catches poison messages
- Both team members can run the full stack locally in under 5 minutes from a fresh clone

---

## Dependency Map

```
Phase 0 (Dev B: algo)  ──┐
Phase 0 (Dev A: monorepo) ──┤
                           ▼
                     Phase 1 (infra up)
                           │
                     Phase 2 (pipeline + MinIO)
                           │
                     Phase 3 (API + Kafka wired)
                           │
                     Phase 4 (AI worker)
                           │
                     Phase 5 (frontend)
                           │
                     Phase 6 (hardening)
```

Phases 0 Dev A and 0 Dev B are fully parallel — the first real synchronization point is Checkpoint 0.

---

## Shared Responsibilities (every phase)

| Task | Who |
|------|-----|
| Kafka topic schema changes | Both — agree before implementing |
| `shared/types/events.ts` updates | Dev A writes, Dev B reviews |
| Environment variable additions to `.env.example` | Whoever adds the feature |
| Code review before each checkpoint | Cross-review (A reviews B's code, B reviews A's) |
| Docker image size audit | Dev A |
| Algorithm correctness sign-off | Dev B |

---

*Total estimated calendar time: 3–4 weeks for a 2-person team working full days, assuming no major LLM API surprises in Phase 4.*


# Nexus — Dev A: Phase 0 Implementation Plan
### Role: Full-Stack + Infra | Duration: ~2–3 Days

> **Your Phase 0 contract:** By Checkpoint 0, `docker-compose up kafka zookeeper minio redis`
> runs clean with healthchecks passing, and Dev B can clone the repo and immediately start
> running their algorithm tests against a real MinIO instance without touching Docker config.

---

## What You Own in Phase 0

From the team doc, your four deliverables are:

| # | Deliverable | Touches |
|---|-------------|---------|
| 1 | Monorepo skeleton with `pnpm workspaces` | Root config, all folder stubs |
| 2 | `docker-compose.yml` infra stubs (images, no app services yet) | Kafka, Zookeeper, MinIO, Redis |
| 3 | `infra/kafka/init-topics.sh` | Topic creation with correct partitions, retention, size config |
| 4 | `infra/minio/init-buckets.sh` | Bucket creation + lifecycle policy |

Dev B runs the algo lab independently in parallel. Your first real sync is Checkpoint 0, where
you both verify:
- Dev B's pytest suite passes
- Your infra stack boots clean
- You review `ast_engine.py` token output together on a real C++ file

---

## Day-by-Day Breakdown

### Day 1 — Monorepo Skeleton
### Day 2 — Docker Compose + Infra Init Scripts
### Day 3 — Validation, `.env` Contracts, Checkpoint 0 Prep

---

## Day 1: Monorepo Skeleton

### Step 1.1 — Prerequisites

Verify these are installed on your machine before anything else:

```bash
node --version       # Must be >= 20.x LTS
pnpm --version       # Must be >= 9.x  →  npm install -g pnpm
docker --version     # Must be >= 25.x
docker compose version  # Must be >= 2.x (the plugin, not legacy docker-compose)
```

If pnpm is missing:
```bash
npm install -g pnpm@latest
```

---

### Step 1.2 — Initialize the Root Workspace

```bash
mkdir nexus && cd nexus
git init
```

Create the root `package.json`. This is the pnpm workspace root — it holds no app code,
only workspace-level tooling (linting, type-checking scripts, etc.):

```json
// package.json  (root)
{
  "name": "nexus",
  "version": "0.0.1",
  "private": true,
  "engines": {
    "node": ">=20.0.0",
    "pnpm": ">=9.0.0"
  },
  "scripts": {
    "dev": "docker compose -f docker-compose.yml -f docker-compose.dev.yml up",
    "infra:up": "docker compose up kafka zookeeper minio redis --wait",
    "infra:down": "docker compose down -v",
    "infra:reset": "docker compose down -v && docker compose up kafka zookeeper minio redis --wait",
    "lint": "pnpm -r lint",
    "typecheck": "pnpm -r typecheck"
  },
  "devDependencies": {
    "typescript": "^5.4.0",
    "@types/node": "^20.0.0"
  }
}
```

Create `pnpm-workspace.yaml`. This tells pnpm which directories are workspace packages:

```yaml
# pnpm-workspace.yaml
packages:
  - 'apps/*'
  - 'workers/*'
  - 'shared/*'
```

---

### Step 1.3 — Create the Full Folder Skeleton

Run this entire block as one script. It creates every directory and stub file in one shot,
so Dev B can immediately place their Python files in the right location:

```bash
# ── apps ─────────────────────────────────────────────────────────────────────
mkdir -p apps/api-gateway/src/{graphql/{resolvers},kafka,storage,redis,utils}
mkdir -p apps/frontend/src/{app/jobs,components,lib}

# ── workers ──────────────────────────────────────────────────────────────────
mkdir -p workers/hash-worker/src
mkdir -p workers/ai-worker/src

# ── infra ─────────────────────────────────────────────────────────────────────
mkdir -p infra/{kafka,minio,redis}

# ── shared ────────────────────────────────────────────────────────────────────
mkdir -p shared/{types,proto}

# ── root files ────────────────────────────────────────────────────────────────
touch .env.example
touch .gitignore
touch docker-compose.yml
touch docker-compose.dev.yml
touch README.md
```

---

### Step 1.4 — Stub Package Files for Each Workspace App

Each `apps/*` directory needs its own `package.json` to be recognized as a workspace member.
Workers are Python so they don't get package.json — just a placeholder `requirements.txt`.

**API Gateway:**
```json
// apps/api-gateway/package.json
{
  "name": "@nexus/api-gateway",
  "version": "0.0.1",
  "private": true,
  "scripts": {
    "dev": "tsx watch src/index.ts",
    "build": "tsc",
    "start": "node dist/index.js",
    "lint": "eslint src",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@apollo/server": "^4.10.0",
    "graphql": "^16.8.0",
    "graphql-subscriptions": "^2.0.0",
    "graphql-redis-subscriptions": "^2.6.0",
    "graphql-ws": "^5.14.0",
    "kafkajs": "^2.2.4",
    "ioredis": "^5.3.2",
    "minio": "^8.0.0",
    "express": "^4.18.0",
    "@types/express": "^4.17.0",
    "ws": "^8.16.0",
    "multer": "^1.4.5",
    "@types/multer": "^1.4.11",
    "uuid": "^9.0.0",
    "@types/uuid": "^9.0.0",
    "zod": "^3.22.0"
  },
  "devDependencies": {
    "tsx": "^4.7.0",
    "typescript": "^5.4.0",
    "@types/node": "^20.0.0",
    "@types/ws": "^8.5.0"
  }
}
```

**Frontend:**
```json
// apps/frontend/package.json
{
  "name": "@nexus/frontend",
  "version": "0.0.1",
  "private": true,
  "scripts": {
    "dev": "next dev",
    "build": "next build",
    "start": "next start",
    "lint": "next lint",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "next": "^14.2.0",
    "react": "^18.3.0",
    "react-dom": "^18.3.0",
    "@apollo/client": "^3.10.0",
    "graphql": "^16.8.0",
    "graphql-ws": "^5.14.0",
    "reactflow": "^11.11.0",
    "cytoscape": "^3.29.0",
    "tailwindcss": "^3.4.0"
  },
  "devDependencies": {
    "typescript": "^5.4.0",
    "@types/react": "^18.3.0",
    "@types/node": "^20.0.0"
  }
}
```

**Python worker placeholders** (Dev B will flesh these out, but they need to exist for Docker):
```bash
# workers/hash-worker/requirements.txt
confluent-kafka==2.4.0
tree-sitter==0.22.0
tree-sitter-cpp==0.22.0
minio==7.2.7
redis==5.0.4
datasketch==1.6.5
pytest==8.1.0
pytest-asyncio==0.23.0

# workers/ai-worker/requirements.txt
confluent-kafka==2.4.0
httpx==0.27.0
minio==7.2.7
redis==5.0.4
networkx==3.3
```

```bash
# Write those to disk:
cat > workers/hash-worker/requirements.txt << 'EOF'
confluent-kafka==2.4.0
tree-sitter==0.22.0
tree-sitter-cpp==0.22.0
minio==7.2.7
redis==5.0.4
datasketch==1.6.5
pytest==8.1.0
pytest-asyncio==0.23.0
EOF

cat > workers/ai-worker/requirements.txt << 'EOF'
confluent-kafka==2.4.0
httpx==0.27.0
minio==7.2.7
redis==5.0.4
networkx==3.3
EOF
```

---

### Step 1.5 — Shared Types (The Kafka Schema Contract)

This is the most important file you create in Phase 0. Dev B must agree on this before
they write any Kafka-producing code in Phase 2. Lock it in now and flag it at Checkpoint 0.

```typescript
// shared/types/events.ts

/**
 * Nexus Kafka Event Schemas — v1
 *
 * IMPORTANT: All Kafka message payloads are serialized as JSON using these types.
 * Any field addition is backward-compatible. Field removal or rename is a BREAKING CHANGE
 * and requires a schema version bump + consumer migration window.
 *
 * Both the Node.js API gateway and the Python workers must conform to these shapes.
 * The Python equivalents are plain dicts validated with a matching dataclass structure.
 */

// ── Topic: submissions ─────────────────────────────────────────────────────

/**
 * Produced by: api-gateway
 * Consumed by: hash-worker
 * Partition key: jobId (ensures all events for one job land on the same partition)
 */
export interface JobCreatedEvent {
  schemaVersion: 1;
  eventType: 'JOB_CREATED';
  jobId: string;                      // UUID v4
  bucketName: string;                 // MinIO bucket name
  objectKey: string;                  // MinIO object path: submissions/{jobId}.zip
  submittedAt: string;                // ISO 8601 UTC
  fileCount: number;                  // From ZIP manifest; used for progress %
  similarityThreshold: number;        // 0.0–1.0; default 0.7
}

// ── Topic: suspicious-pairs ────────────────────────────────────────────────

/**
 * Produced by: hash-worker
 * Consumed by: ai-worker
 * Partition key: jobId (keeps all pairs from same job on same partition)
 */
export interface SuspiciousPairEvent {
  schemaVersion: 1;
  eventType: 'SUSPICIOUS_PAIR';
  jobId: string;
  pairId: string;                     // UUID v4; unique per pair within the job
  fileAName: string;                  // Original filename from ZIP
  fileBName: string;
  fileAObjectKey: string;             // MinIO key for extracted source: extracted/{jobId}/{fileAName}
  fileBObjectKey: string;
  similarityScore: number;            // Jaccard similarity 0.0–1.0
  fingerprintSizeA: number;           // |fp_A| — useful for debugging false positives
  fingerprintSizeB: number;
  detectedAt: string;                 // ISO 8601 UTC
}

// ── Topic: forensic-results ────────────────────────────────────────────────

/**
 * Produced by: ai-worker
 * Consumed by: api-gateway (for WebSocket fan-out)
 * Partition key: jobId
 */
export interface ForensicResultEvent {
  schemaVersion: 1;
  eventType: 'FORENSIC_RESULT';
  jobId: string;
  pairId: string;
  reportObjectKey: string;            // MinIO key: reports/{jobId}/{pairId}.json
  verdict: 'LIKELY_PLAGIARISM' | 'POSSIBLE_COINCIDENCE' | 'INCONCLUSIVE';
  confidence: number;                 // 0.0–1.0
  obfuscationTechniques: string[];
  completedAt: string;                // ISO 8601 UTC
  llmModelUsed: string;
  tokensUsed: number;
}

// ── Topic: job-lifecycle ───────────────────────────────────────────────────

/**
 * Produced by: hash-worker (transitions) and ai-worker (final complete)
 * Consumed by: api-gateway (for WebSocket + Redis state update)
 */
export interface JobStatusEvent {
  schemaVersion: 1;
  eventType: 'JOB_STATUS';
  jobId: string;
  status: JobStatus;
  progress: number;                   // 0–100 integer
  message: string;                    // Human-readable; shown in LiveTerminal
  timestamp: string;                  // ISO 8601 UTC
  workerInstance?: string;            // Optional: hostname of the worker (debugging)
  metadata?: Record<string, unknown>; // Phase-specific data (e.g., filesProcessed)
}

export type JobStatus =
  | 'PENDING'
  | 'EXTRACTING'       // Downloading + unzipping the ZIP from MinIO
  | 'PARSING'          // Tree-sitter AST parsing
  | 'HASHING'          // Winnowing fingerprints
  | 'COMPARING'        // Jaccard pairwise comparison
  | 'AI_ANALYSIS'      // LLM forensic analysis
  | 'COMPLETE'
  | 'FAILED';

// ── Dead Letter ────────────────────────────────────────────────────────────

/**
 * Shape of messages that land in the dead-letter topic.
 * The original message payload is re-published with these headers attached
 * (not as a separate envelope — the DLQ message IS the original payload,
 * with diagnostic Kafka headers added by the retry handler).
 */
export interface DLQHeaders {
  'original-topic': string;
  'original-partition': string;
  'original-offset': string;
  'failure-reason': string;           // Truncated to 500 chars
  'failed-attempts': string;
  'failed-at': string;                // Unix timestamp
  'schema-version': string;
}

// ── Redis Key Patterns (not Kafka, but kept here as the shared contract) ───

export const REDIS_KEYS = {
  jobStatus: (jobId: string) => `job:${jobId}:status`,          // Hash
  jobEvents: (jobId: string) => `job:${jobId}:events`,          // Pub/Sub channel
  jobPairs: (jobId: string) => `job:${jobId}:pairs`,            // Set of pairIds
  pairReport: (pairId: string) => `pair:${pairId}:report`,      // String (MinIO key ref)
  workerHeartbeat: (hostname: string) => `worker:${hostname}:heartbeat`, // String with TTL
} as const;

export const KAFKA_TOPICS = {
  SUBMISSIONS: 'submissions',
  SUSPICIOUS_PAIRS: 'suspicious-pairs',
  FORENSIC_RESULTS: 'forensic-results',
  JOB_LIFECYCLE: 'job-lifecycle',
  DEAD_LETTER: 'dead-letter',
} as const;
```

---

### Step 1.6 — Root `.gitignore`

```gitignore
# .gitignore

# Node
node_modules/
dist/
.next/
.turbo/

# Python
__pycache__/
*.py[cod]
.venv/
*.egg-info/
.pytest_cache/
.mypy_cache/

# Environment
.env
.env.local
.env.*.local

# Docker volumes (never commit data dirs)
data/

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Build artifacts
*.tsbuildinfo
```

---

### Step 1.7 — Root `.env.example`

This is the single source of truth for environment variable contracts. Every variable
that any service needs must live here. Add a comment explaining the purpose and valid values.

```bash
# .env.example
# Copy to .env and fill in real values. Never commit .env.

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKERS=kafka:9092
# Used by: api-gateway, hash-worker, ai-worker

# ── Redis ─────────────────────────────────────────────────────────────────────
REDIS_HOST=redis
REDIS_PORT=6379
# Used by: api-gateway, hash-worker, ai-worker

# ── MinIO ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT=minio
MINIO_PORT=9000
MINIO_ACCESS_KEY=nexus
MINIO_SECRET_KEY=nexus-secret-change-in-prod
MINIO_USE_SSL=false
MINIO_BUCKET_SUBMISSIONS=nexus-submissions
MINIO_BUCKET_REPORTS=nexus-reports
# Used by: api-gateway, hash-worker, ai-worker

# ── API Gateway ───────────────────────────────────────────────────────────────
API_PORT=4000
API_MAX_UPLOAD_SIZE_MB=500
# Default similarity threshold (can be overridden per job via API)
DEFAULT_SIMILARITY_THRESHOLD=0.70

# ── Frontend ──────────────────────────────────────────────────────────────────
NEXT_PUBLIC_API_URL=http://localhost:4000/graphql
NEXT_PUBLIC_WS_URL=ws://localhost:4000/graphql

# ── AI Worker ─────────────────────────────────────────────────────────────────
LLM_API_KEY=your-openai-or-anthropic-key-here
LLM_API_URL=https://api.openai.com/v1/chat/completions
LLM_MODEL=gpt-4o-mini
LLM_MAX_CONCURRENT=5
# Maximum source code chars sent to LLM per file (cost control)
LLM_MAX_SOURCE_CHARS=4000

# ── Hash Worker ───────────────────────────────────────────────────────────────
# Winnowing constants — see winnowing.py for tuning guidance
WINNOW_K=8
WINNOW_W=6
# Max file size to process (bytes). Files larger than this are skipped.
MAX_FILE_SIZE_BYTES=512000

# ── Observability ─────────────────────────────────────────────────────────────
LOG_LEVEL=info
# Valid values: debug, info, warning, error
```

---

## Day 2: Docker Compose + Infra Init Scripts

### Step 2.1 — The Docker Compose File (Infra Only)

This is your most critical Phase 0 file. **Rule: In Phase 0, only infra services exist.**
`api-gateway`, `frontend`, `hash-worker`, `ai-worker` are NOT added yet — that's Phase 3+.

The Dev B can use this to spin up a real MinIO for their integration test in Phase 2.

```yaml
# docker-compose.yml

version: '3.9'

# ─────────────────────────────────────────────────────────────────────────────
# Shared environment snippets via YAML anchors.
# These are referenced by app services added in later phases.
# ─────────────────────────────────────────────────────────────────────────────
x-infra-env: &infra-env
  KAFKA_BROKERS: kafka:9092
  REDIS_HOST: redis
  REDIS_PORT: "6379"
  MINIO_ENDPOINT: minio
  MINIO_PORT: "9000"
  MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-nexus}
  MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-nexus-secret-change-in-prod}
  MINIO_USE_SSL: "false"
  MINIO_BUCKET_SUBMISSIONS: ${MINIO_BUCKET_SUBMISSIONS:-nexus-submissions}
  MINIO_BUCKET_REPORTS: ${MINIO_BUCKET_REPORTS:-nexus-reports}

services:

  # ── Zookeeper ───────────────────────────────────────────────────────────────
  # Required by Kafka (KRaft mode alternative requires Kafka 3.5+ — staying on
  # Zookeeper for broader compatibility with team's existing tooling).
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    container_name: nexus-zookeeper
    hostname: zookeeper
    networks: [nexus-net]
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
      ZOOKEEPER_SYNC_LIMIT: 2
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-logs:/var/lib/zookeeper/log
    healthcheck:
      test: echo stat | nc localhost 2181 | grep Mode
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
    restart: unless-stopped

  # ── Kafka ───────────────────────────────────────────────────────────────────
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: nexus-kafka
    hostname: kafka
    networks: [nexus-net]
    ports:
      - "9092:9092"      # Internal broker (used by all containers)
      - "9093:9093"      # External broker (used by host-machine tooling like kafkacat)
    depends_on:
      zookeeper:
        condition: service_healthy
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181

      # Two listener configs:
      # PLAINTEXT → internal container-to-container communication
      # EXTERNAL → host machine → container (for local debugging with kafka CLI)
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT

      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_DEFAULT_REPLICATION_FACTOR: 1
      KAFKA_MIN_INSYNC_REPLICAS: 1

      # ── CRITICAL for fault tolerance ─────────────────────────────────────
      # 7-day log retention — ensures Kafka replays messages if a worker crashes
      # and stays down for hours/days. Without this, messages expire and forensic
      # jobs are silently dropped. Never use the default (168h keeps this explicit).
      KAFKA_LOG_RETENTION_HOURS: 168

      # 10MB max message size — students can submit large C++ files.
      # Must be set on BOTH broker AND producer/consumer (already set in kafka_client.py).
      KAFKA_MESSAGE_MAX_BYTES: 10485760
      KAFKA_REPLICA_FETCH_MAX_BYTES: 10485760
      KAFKA_MAX_REQUEST_SIZE: 10485760

      # Disable auto topic creation — all topics are explicitly created by init-topics.sh.
      # This prevents typos in topic names from silently creating phantom topics.
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"

      # Performance tuning
      KAFKA_NUM_PARTITIONS: 1          # Default; init-topics.sh overrides per topic
      KAFKA_LOG_SEGMENT_BYTES: 1073741824   # 1GB segments
      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 300000

    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      # Try to list topics — proves broker is up and ZK connection is live
      test: kafka-topics --bootstrap-server localhost:9092 --list
      interval: 15s
      timeout: 10s
      retries: 8
      start_period: 30s
    restart: unless-stopped

  # ── Kafka Init ──────────────────────────────────────────────────────────────
  # One-shot container that creates topics after Kafka is healthy.
  # Uses `restart: no` so it doesn't loop. If topic creation fails,
  # check `docker compose logs kafka-init`.
  kafka-init:
    image: confluentinc/cp-kafka:7.6.1
    container_name: nexus-kafka-init
    networks: [nexus-net]
    depends_on:
      kafka:
        condition: service_healthy
    volumes:
      - ./infra/kafka/init-topics.sh:/init-topics.sh:ro
    command: bash /init-topics.sh
    environment:
      KAFKA_BROKER: kafka:9092
    restart: "no"

  # ── Redis ───────────────────────────────────────────────────────────────────
  redis:
    image: redis:7.2-alpine
    container_name: nexus-redis
    hostname: redis
    networks: [nexus-net]
    ports:
      - "6379:6379"
    command: redis-server /usr/local/etc/redis/redis.conf
    volumes:
      - redis-data:/data
      - ./infra/redis/redis.conf:/usr/local/etc/redis/redis.conf:ro
    healthcheck:
      test: redis-cli ping | grep PONG
      interval: 5s
      timeout: 3s
      retries: 5
      start_period: 5s
    restart: unless-stopped

  # ── MinIO ───────────────────────────────────────────────────────────────────
  minio:
    image: minio/minio:RELEASE.2024-06-04T19-20-08Z
    container_name: nexus-minio
    hostname: minio
    networks: [nexus-net]
    ports:
      - "9000:9000"      # S3-compatible API
      - "9001:9001"      # MinIO Web Console (browse at http://localhost:9001)
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY:-nexus}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY:-nexus-secret-change-in-prod}
      # Optional: telemetry off for air-gapped environments
      MINIO_UPDATE: "off"
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: mc ready local
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 10s
    restart: unless-stopped

  # ── MinIO Init ──────────────────────────────────────────────────────────────
  minio-init:
    image: minio/mc:RELEASE.2024-06-03T11-52-49Z
    container_name: nexus-minio-init
    networks: [nexus-net]
    depends_on:
      minio:
        condition: service_healthy
    volumes:
      - ./infra/minio/init-buckets.sh:/init-buckets.sh:ro
    entrypoint: sh /init-buckets.sh
    environment:
      MINIO_ENDPOINT: http://minio:9000
      MINIO_ACCESS_KEY: ${MINIO_ACCESS_KEY:-nexus}
      MINIO_SECRET_KEY: ${MINIO_SECRET_KEY:-nexus-secret-change-in-prod}
    restart: "no"

# ─────────────────────────────────────────────────────────────────────────────
# Networks
# ─────────────────────────────────────────────────────────────────────────────
networks:
  nexus-net:
    driver: bridge
    # Explicit subnet avoids conflicts with VPN ranges
    ipam:
      config:
        - subnet: 172.28.0.0/16

# ─────────────────────────────────────────────────────────────────────────────
# Named Volumes
# Never use bind mounts for stateful data — named volumes survive container
# recreation and are managed by Docker's storage driver.
# ─────────────────────────────────────────────────────────────────────────────
volumes:
  zookeeper-data:
    driver: local
  zookeeper-logs:
    driver: local
  kafka-data:
    driver: local
  redis-data:
    driver: local
  minio-data:
    driver: local
```

---

### Step 2.2 — Dev Override File

```yaml
# docker-compose.dev.yml
# Usage: docker compose -f docker-compose.yml -f docker-compose.dev.yml up
#
# This file:
# - Adds hot-reload volume mounts for app services (added in Phase 3+)
# - Exposes extra debug ports
# - Sets LOG_LEVEL=debug globally

version: '3.9'

services:
  kafka:
    environment:
      KAFKA_LOG4J_ROOT_LOGLEVEL: WARN      # Reduce Kafka noise in dev

  # When app services are added in Phase 3+, add their dev overrides here:
  # api-gateway:
  #   volumes:
  #     - ./apps/api-gateway/src:/app/src:ro
  #   environment:
  #     LOG_LEVEL: debug
  #     NODE_ENV: development
  #
  # hash-worker:
  #   volumes:
  #     - ./workers/hash-worker/src:/app/src:ro
  #   environment:
  #     LOG_LEVEL: debug
```

---

### Step 2.3 — Kafka Init Topics Script

This is the most technically complex infra file. Every parameter here has a specific
reason. Read the comments — Dev B will ask you why the partition counts are what they are.

```bash
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
```

Make it executable:
```bash
chmod +x infra/kafka/init-topics.sh
```

---

### Step 2.4 — MinIO Init Buckets Script

```bash
#!/usr/bin/env sh
# infra/minio/init-buckets.sh
#
# Creates MinIO buckets and lifecycle policies.
# Run context: inside nexus-minio-init container, after MinIO is healthy.
#
# Uses the `mc` (MinIO Client) CLI — already in the minio/mc Docker image.

set -eu

ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
ACCESS_KEY="${MINIO_ACCESS_KEY:-nexus}"
SECRET_KEY="${MINIO_SECRET_KEY:-nexus-secret-change-in-prod}"
ALIAS="nexus-local"

echo "══════════════════════════════════════════════════════"
echo "Nexus MinIO Bucket Initialization"
echo "Endpoint: ${ENDPOINT}"
echo "══════════════════════════════════════════════════════"

# ── Configure mc alias ──────────────────────────────────────────────────────
echo ""
echo "→ Configuring mc alias..."
mc alias set "${ALIAS}" "${ENDPOINT}" "${ACCESS_KEY}" "${SECRET_KEY}" --api S3v4
echo "  ✓ Alias configured"

# ── Helper: create bucket if it doesn't exist ────────────────────────────────
create_bucket() {
  local BUCKET="$1"
  echo ""
  echo "→ Creating bucket: ${ALIAS}/${BUCKET}"

  if mc ls "${ALIAS}/${BUCKET}" &>/dev/null; then
    echo "  ⊙ Already exists — skipping"
  else
    mc mb "${ALIAS}/${BUCKET}"
    echo "  ✓ Created"
  fi
}

# ── nexus-submissions ────────────────────────────────────────────────────────
# Stores: raw ZIP files uploaded by instructors
# Path pattern: submissions/{jobId}.zip
# Lifecycle: expire after 30 days (ZIPs are large; results are what matter)
create_bucket "nexus-submissions"

cat > /tmp/submissions-lifecycle.json << 'LIFECYCLE_EOF'
{
  "Rules": [
    {
      "ID": "expire-raw-submissions",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "submissions/"
      },
      "Expiration": {
        "Days": 30
      }
    }
  ]
}
LIFECYCLE_EOF

mc ilm import "${ALIAS}/nexus-submissions" < /tmp/submissions-lifecycle.json
echo "  ✓ Lifecycle policy applied (30-day expiry on submissions/)"

# ── nexus-reports ────────────────────────────────────────────────────────────
# Stores: LLM forensic JSON reports + extracted C++ source files
# Path patterns:
#   reports/{jobId}/{pairId}.json   ← LLM forensic report
#   extracted/{jobId}/{filename}    ← Individual C++ files (for AI worker retrieval)
#
# Lifecycle:
#   reports/ → 90 days (audit record; instructors need these for grade disputes)
#   extracted/ → 7 days (temp files; only needed during active analysis)
create_bucket "nexus-reports"

cat > /tmp/reports-lifecycle.json << 'LIFECYCLE_EOF'
{
  "Rules": [
    {
      "ID": "expire-extracted-sources",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "extracted/"
      },
      "Expiration": {
        "Days": 7
      }
    },
    {
      "ID": "expire-forensic-reports",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "reports/"
      },
      "Expiration": {
        "Days": 90
      }
    }
  ]
}
LIFECYCLE_EOF

mc ilm import "${ALIAS}/nexus-reports" < /tmp/reports-lifecycle.json
echo "  ✓ Lifecycle policy applied (7-day expiry on extracted/, 90-day on reports/)"

# ── Verification ─────────────────────────────────────────────────────────────
echo ""
echo "── Verification ───────────────────────────────────────"
echo "Buckets:"
mc ls "${ALIAS}"

echo ""
echo "nexus-submissions lifecycle:"
mc ilm ls "${ALIAS}/nexus-submissions"

echo ""
echo "nexus-reports lifecycle:"
mc ilm ls "${ALIAS}/nexus-reports"

echo ""
echo "══════════════════════════════════════════════════════"
echo "✓ All Nexus MinIO buckets initialized successfully"
echo "   Console: http://localhost:9001"
echo "   User: ${ACCESS_KEY}"
echo "══════════════════════════════════════════════════════"
```

Make it executable:
```bash
chmod +x infra/minio/init-buckets.sh
```

---

### Step 2.5 — Redis Configuration

```conf
# infra/redis/redis.conf
#
# Minimal production-grade Redis config for Nexus.
# Key design decisions documented inline.

# ── Network ──────────────────────────────────────────────────────────────────
bind 0.0.0.0
port 6379
protected-mode no    # Running inside Docker network; external access is controlled by Docker

# ── Persistence ──────────────────────────────────────────────────────────────
# AOF (Append-Only File) gives per-second durability.
# If Redis restarts, it replays the AOF to reconstruct state.
# Without this, job statuses are lost on Redis restart — the frontend
# would show stale or missing data for in-progress jobs.
appendonly yes
appendfilename "nexus.aof"
appendfsync everysec    # Flush to disk every second (balance between safety and performance)
no-appendfsync-on-rewrite yes

# RDB snapshot as a backup (in addition to AOF):
# Save if at least 1 key changed in the last 3600 seconds
save 3600 1
save 300 100
save 60 10000

# ── Memory ───────────────────────────────────────────────────────────────────
# Cap Redis memory. For N=5000 files, job state hashes are tiny (~1KB each).
# 512MB is extremely conservative — most job state fits in < 50MB.
# If Redis exceeds this, it evicts LRU keys — which we want for Pub/Sub
# channels (ephemeral) but NOT for job status hashes.
maxmemory 512mb

# Policy: only evict keys with a TTL set, preserving permanent keys (job statuses).
# Our job statuses have explicit 24h TTLs set in state.py.
maxmemory-policy volatile-lru

# ── Pub/Sub ───────────────────────────────────────────────────────────────────
# No config changes needed for Pub/Sub — Redis supports it natively.
# Client output buffer limit for subscribers:
# If a subscriber is too slow to consume, Redis will disconnect it after:
# - Hard limit: 256MB buffer, OR
# - Soft limit: 64MB buffer held for > 60 seconds
client-output-buffer-limit pubsub 256mb 64mb 60

# ── Logging ──────────────────────────────────────────────────────────────────
loglevel notice
logfile ""    # Log to stdout → captured by Docker

# ── Timeouts ─────────────────────────────────────────────────────────────────
timeout 0      # Never close idle connections (workers maintain long-lived connections)
tcp-keepalive 300
```

---

## Day 3: Validation & Checkpoint 0 Prep

### Step 3.1 — First Boot Validation

```bash
# From the repo root:

# 1. Start infra only (the Phase 0 target)
docker compose up kafka zookeeper minio redis --wait

# Expected: all 4 services show (healthy) in docker compose ps
docker compose ps

# 2. Verify Kafka topics were created by kafka-init
docker compose logs kafka-init

# You should see:
# ✓ All Nexus topics initialized successfully

# 3. Verify topics exist
docker run --rm --network nexus_nexus-net \
  confluentinc/cp-kafka:7.6.1 \
  kafka-topics --bootstrap-server kafka:9092 --list

# Expected output (5 topics):
# dead-letter
# forensic-results
# job-lifecycle
# submissions
# suspicious-pairs

# 4. Verify topic configs (spot-check submissions)
docker run --rm --network nexus_nexus-net \
  confluentinc/cp-kafka:7.6.1 \
  kafka-topics --bootstrap-server kafka:9092 --describe --topic submissions

# Expected: Leader: 1, Partitions: 4, Replication: 1
# Config: retention.ms=604800000, max.message.bytes=10485760

# 5. Verify MinIO buckets
docker compose logs minio-init

# You should see:
# ✓ All Nexus MinIO buckets initialized successfully

# 6. Redis health check
docker exec nexus-redis redis-cli ping
# Expected: PONG

docker exec nexus-redis redis-cli info server | grep redis_version
# Expected: redis_version:7.2.x
```

---

### Step 3.2 — Smoke Test: Produce and Consume a Message

Before handing off to Dev B, prove Kafka works end-to-end with a manual message:

```bash
# Terminal 1: Start a consumer on the submissions topic
docker run --rm -it --network nexus_nexus-net \
  confluentinc/cp-kafka:7.6.1 \
  kafka-console-consumer \
    --bootstrap-server kafka:9092 \
    --topic submissions \
    --from-beginning

# Terminal 2: Produce a test message
docker run --rm -it --network nexus_nexus-net \
  confluentinc/cp-kafka:7.6.1 \
  kafka-console-producer \
    --broker-list kafka:9092 \
    --topic submissions \
    --property "parse.key=true" \
    --property "key.separator=:"

# Type this in Terminal 2, then press Enter:
# test-job-id:{"schemaVersion":1,"eventType":"JOB_CREATED","jobId":"test-job-id","bucketName":"nexus-submissions","objectKey":"submissions/test-job-id.zip","submittedAt":"2024-01-01T00:00:00Z","fileCount":5,"similarityThreshold":0.7}

# Terminal 1 should immediately print the message.
# Ctrl+C both terminals when done.
```

---

### Step 3.3 — Smoke Test: MinIO Upload

Give Dev B a working MinIO test they can run from their Python environment:

```python
# Save as: infra/minio/test_minio_connection.py
# Run from host (not inside Docker): python infra/minio/test_minio_connection.py
# Requires: pip install minio

from minio import Minio
from minio.error import S3Error
import io

client = Minio(
    "localhost:9000",
    access_key="nexus",
    secret_key="nexus-secret-change-in-prod",
    secure=False,
)

# Upload a test object
test_data = b"Hello from Nexus test"
client.put_object(
    bucket_name="nexus-submissions",
    object_name="test/hello.txt",
    data=io.BytesIO(test_data),
    length=len(test_data),
    content_type="text/plain",
)
print("✓ Upload succeeded")

# Download and verify
response = client.get_object("nexus-submissions", "test/hello.txt")
assert response.read() == test_data
print("✓ Download and verify succeeded")

# Clean up
client.remove_object("nexus-submissions", "test/hello.txt")
print("✓ Cleanup done")
print("\nMinIO is fully operational for Dev B.")
```

```bash
# Run it:
pip install minio
python infra/minio/test_minio_connection.py
```

---

### Step 3.4 — Install Dependencies

```bash
# From repo root — install all Node workspace dependencies at once
pnpm install

# Verify workspace resolution
pnpm -r exec node --version    # Should print node version for each workspace
```

---

### Step 3.5 — Checkpoint 0 Handoff Checklist

Work through this with Dev B before declaring Phase 0 done:

```
Dev A Deliverables:
□ pnpm install completes without errors from a clean clone
□ docker compose up kafka zookeeper minio redis --wait → all (healthy)
□ docker compose logs kafka-init shows all 5 topics created successfully
□ docker compose logs minio-init shows both buckets created with lifecycle policies
□ Kafka smoke test: produce → consume round-trip works
□ MinIO smoke test: test_minio_connection.py passes
□ shared/types/events.ts is committed and reviewed by Dev B
□ .env.example is complete with every variable needed through Phase 4

Joint Review:
□ Walk through events.ts together — Dev B must agree on:
    - SuspiciousPairEvent fields (especially fileAObjectKey/fileBObjectKey path format)
    - JobStatus state machine (PENDING → EXTRACTING → PARSING → HASHING → COMPARING → AI_ANALYSIS → COMPLETE)
    - REDIS_KEYS patterns
    - KAFKA_TOPICS constants
□ Dev B demos ast_engine.py token output on a real C++ file
□ Both read the token stream and agree the structural tokens look correct
□ Agree on the MinIO path format for extracted C++ files:
    - Submissions ZIP: submissions/{jobId}.zip
    - Extracted files:  extracted/{jobId}/{originalFilename}
    - Forensic reports: reports/{jobId}/{pairId}.json
```

---

## Final Repo State After Phase 0

```
nexus/
├── .env.example                          ✓
├── .gitignore                            ✓
├── package.json                          ✓
├── pnpm-workspace.yaml                   ✓
├── docker-compose.yml                    ✓  (infra only)
├── docker-compose.dev.yml                ✓  (dev overrides stub)
│
├── apps/
│   ├── api-gateway/
│   │   ├── package.json                  ✓
│   │   └── src/                          (empty — Phase 3)
│   └── frontend/
│       ├── package.json                  ✓
│       └── src/                          (empty — Phase 5)
│
├── workers/
│   ├── hash-worker/
│   │   ├── requirements.txt              ✓
│   │   └── src/                          (Dev B filling this — Phase 0/1/2)
│   └── ai-worker/
│       ├── requirements.txt              ✓
│       └── src/                          (empty — Phase 4)
│
├── infra/
│   ├── kafka/
│   │   ├── init-topics.sh                ✓  (executable)
│   │   └── test_kafka_connection.sh      ✓
│   ├── minio/
│   │   ├── init-buckets.sh               ✓  (executable)
│   │   └── test_minio_connection.py      ✓
│   └── redis/
│       └── redis.conf                    ✓
│
└── shared/
    └── types/
        └── events.ts                     ✓  (THE schema contract — reviewed by both)
```

---

## Common Pitfalls to Avoid in Phase 0

**Docker network naming:** Docker Compose prefixes the network name with the project directory
name. If your folder is `nexus/`, the network is `nexus_nexus-net`. If it's something else,
the smoke test `--network` flag needs updating. Run `docker network ls` to check.

**Kafka `advertised.listeners` split-brain:** The two-listener setup (PLAINTEXT for container,
EXTERNAL for host) is essential. Without it, a producer on the host machine connects to
`kafka:9092`, which it can't resolve → `UNKNOWN_HOST`. Dev B's Python tests may run from the
host (not inside Docker), so they need `localhost:9093`.

**MinIO `mc` vs `aws s3`:** MinIO's `mc` CLI uses `mc` syntax, not `aws`. The test script
uses the Python `minio` SDK which speaks S3 protocol — this is the correct abstraction for
both the API gateway and the workers.

**`pnpm install` must run from root:** Running it from inside `apps/api-gateway/` installs
a flat `node_modules` instead of using the workspace hoist. Always run from repo root.

**Zookeeper startup race:** Kafka's healthcheck (`kafka-topics --list`) can pass before
Zookeeper is fully ready, causing the `kafka-init` script to fail. The `start_period: 30s`
on the Kafka healthcheck absorbs this. If `kafka-init` fails, `docker compose restart kafka-init`
is safe because the script is idempotent (`--if-not-exists`).
```

---

# Nexus — Dev A: Phase 4 Implementation Plan
### Role: Full-Stack + Infra | Duration: ~3–4 Days

> **Phase 4 contract:** By Checkpoint 4, the AI worker has a fully operational
> Kafka consumer loop (consumer group: `ai-workers`) that pulls `SuspiciousPairEvent`
> messages from `suspicious-pairs`, runs Dev B's `forensic_analyst.py` on each pair,
> deduplicates cheating rings via transitive closure before firing LLM calls, stores
> every `ForensicReport` as a JSON object in MinIO at `reports/{jobId}/{pairId}.json`,
> and publishes the report reference to Redis so the frontend (Phase 5) can fetch it.

## Files Delivered in Phase 4

| File | Status |
|------|--------|
| `services/ai-worker/forensic_analyst.py` | ✅ Interface contract stub |
| `services/ai-worker/kafka_client.py` | ✅ Consumer loop (ai-workers group, batch accumulation) |
| `services/ai-worker/deduplicator.py` | ✅ Transitive closure via networkx |
| `services/ai-worker/report_store.py` | ✅ MinIO write + Redis publish |
| `services/ai-worker/main.py` | ✅ Pipeline orchestrator |
| `services/ai-worker/requirements.txt` | ✅ Updated: networkx, python-dotenv |
| `services/ai-worker/pyproject.toml` | ✅ Updated: networkx, python-dotenv |
| `services/ai-worker/Dockerfile` | ✅ LLM_API_KEY health check |
| `services/ai-worker/tests/unit/test_deduplicator.py` | ✅ 12 tests, all pass |
| `services/ai-worker/tests/unit/test_report_store.py` | ✅ 11 tests, all pass |
| `services/ai-worker/tests/integration/test_pipeline_no_llm.py` | ✅ Full pipeline, stub LLM |
| `docker-compose.yml` | ✅ ai-worker service activated |
| `docker-compose.dev.yml` | ✅ ai-worker hot-reload override |
| `devstudy.md` | ✅ Component reference (gitignored) |

## Interface Contract — `forensic_analyst.py`

```python
async def analyze_pair(pair: SuspiciousPair) -> ForensicReport:
    # Dev B implements this.
    # NEVER raises — returns fallback ForensicReport on any failure.
    # Concurrency controlled internally via asyncio.Semaphore (LLM_MAX_CONCURRENT).
    # Source code strings provided by Dev A (fetched from MinIO before calling).
```

**Contract agreements:**
- Return type: `ForensicReport` dataclass (Dev A calls `dataclasses.asdict()` to serialize)
- Exception behavior: Never raises. Fallback report with `verdict="INCONCLUSIVE"` on total failure.
- Concurrency: `analyze_pair()` manages its own `asyncio.Semaphore` — Dev A uses `asyncio.gather()` externally.
- Source code: Dev A fetches from MinIO, truncates to `LLM_MAX_SOURCE_CHARS`, passes as strings.
- `verdict="SEE_REPRESENTATIVE"`: Synthetic reports for non-representative ring members — frontend must follow `representativeReportKey`.

## Key Design Decisions

### Batch Accumulation (kafka_client.py)
Pairs for one job arrive in a burst from the hash-worker. We wait `AI_BATCH_WINDOW_SECONDS=2.0`
after the last pair for a jobId before flushing. This ensures the deduplicator sees ALL pairs
for a job simultaneously, enabling transitive closure deduplication.

### Transitive Closure (deduplicator.py)
N=200 students copying → C(200,2)=19,900 pairs → 19,900 LLM calls = $199.
With transitive closure: 1 LLM call + 19,899 synthetic reports = $0.01.
NetworkX `connected_components()` runs in O(V+E) — negligible cost.

### MinIO vs Redis Failure Modes (report_store.py)
- MinIO write: **mandatory** — S3Error propagates, kafka_client retries the batch.
- Redis write: **best-effort** — broad `except Exception` catches all connection failures,
  logs warning, continues. Report is in MinIO regardless. API gateway polls MinIO directly.

### asyncio Bridge (main.py)
Kafka consumer is synchronous (confluent-kafka). Async handler is called via `asyncio.run()`.
`asyncio.gather()` fires all representative LLM calls concurrently. `return_exceptions=True`
prevents one failing LLM task from aborting the entire batch.

## Checkpoint 4 Verification Steps

```
□ pytest services/ai-worker/tests/unit/ → 23/23 passing
□ docker build services/ai-worker → successful
□ docker compose up ai-worker → healthcheck passes (LLM_API_KEY must be set)
□ Integration: suspicious pair from Kafka → ForensicReport in MinIO
□ Ring test: 3 pairs in a ring → 1 LLM call + 2 synthetic reports
□ Fallback: LLM unreachable → report stored with verdict=INCONCLUSIVE
□ DLQ: MinIO failure after MAX_RETRIES → message in dead-letter topic
□ Status: job Redis hash transitions COMPLETE → AI_ANALYSIS → COMPLETE
```