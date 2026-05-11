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
  fileAObjectKey: string;             // MinIO key: extracted/{jobId}/{fileAName}
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

/**
 * JobStatus State Machine — transition ownership
 *
 * State flow:
 *   PENDING → EXTRACTING → PARSING → HASHING → COMPARING → AI_ANALYSIS → COMPLETE
 *                                                                      ↘ FAILED
 *
 * Transition ownership:
 *   PENDING      → set by api-gateway when JOB_CREATED is published
 *   EXTRACTING   → set by hash-worker when ZIP download from MinIO starts
 *   PARSING      → set by hash-worker when tree-sitter AST loop starts
 *   HASHING      → set by hash-worker when Winnowing fingerprinting starts
 *   COMPARING    → set by hash-worker when pairwise Jaccard comparison starts
 *   AI_ANALYSIS  → set by ai-worker when first LLM call fires
 *   COMPLETE     → set by ai-worker after last ForensicResult is stored
 *   FAILED       → set by either worker on unrecoverable error
 *
 * TODO: confirm with Dev B — verify these transitions match their
 *       hash-worker implementation before Phase 2 code is written.
 */
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
 * The original message payload is re-published with these headers attached.
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

// ── MinIO Object Path Conventions ─────────────────────────────────────────
//
// These are the agreed path formats used by all services. Any change here
// requires updates in api-gateway, hash-worker, and ai-worker.
//
// TODO: confirm with Dev B — verify hash-worker writes to these exact paths.

export const MINIO_PATHS = {
  /** Raw ZIP uploads from the API gateway */
  submission: (jobId: string) => `submissions/${jobId}.zip`,
  /** Extracted source files (one per file in the ZIP) */
  extractedFile: (jobId: string, filename: string) => `extracted/${jobId}/${filename}`,
  /** AI forensic reports (one per suspicious pair) */
  forensicReport: (jobId: string, pairId: string) => `reports/${jobId}/${pairId}.json`,
} as const;

// ── Redis Key Patterns (not Kafka, but kept here as the shared contract) ───

export const REDIS_KEYS = {
  jobStatus: (jobId: string) => `job:${jobId}:status`,
  jobEvents: (jobId: string) => `job:${jobId}:events`,
  jobPairs: (jobId: string) => `job:${jobId}:pairs`,
  pairReport: (pairId: string) => `pair:${pairId}:report`,
  workerHeartbeat: (hostname: string) => `worker:${hostname}:heartbeat`,
} as const;

// ── Kafka Topic Constants ─────────────────────────────────────────────────
//
// Python workers must use string literals that EXACTLY match these values.
// These strings match the topics created by infra/kafka/init-topics.sh.

export const KAFKA_TOPICS = {
  SUBMISSIONS: 'submissions',
  SUSPICIOUS_PAIRS: 'suspicious-pairs',
  FORENSIC_RESULTS: 'forensic-results',
  JOB_LIFECYCLE: 'job-lifecycle',
  DEAD_LETTER: 'dead-letter',
} as const;

// ── Kafka Consumer Group Names ────────────────────────────────────────────
//
// Each worker type uses a dedicated consumer group for partition balancing.
// Scaling workers (docker compose up --scale hash-worker=4) auto-balances
// partitions across group members.
//
// TODO: confirm with Dev B — verify these group names are used in
//       confluent-kafka consumer config.

export const CONSUMER_GROUPS = {
  HASH_WORKERS: 'hash-workers',
  AI_WORKERS: 'ai-workers',
} as const;

// ── Kafka Partition Key Convention ────────────────────────────────────────
//
// All messages use jobId (plain UUID string, no prefix) as the partition key.
// This ensures all messages for a single job land on the same partition,
// enabling ordered processing per job.
//
// In Python (confluent-kafka): producer.produce(key=job_id.encode('utf-8'))
// In TypeScript (kafkajs):     producer.send({ messages: [{ key: jobId, ... }] })

// ── Similarity Score Convention ───────────────────────────────────────────
//
// All similarity scores are in the range [0.0, 1.0] (NOT 0–100).
// This applies to:
//   - SuspiciousPairEvent.similarityScore
//   - ForensicResultEvent.confidence
//   - JobCreatedEvent.similarityThreshold
//   - comparator.py jaccard() return value
