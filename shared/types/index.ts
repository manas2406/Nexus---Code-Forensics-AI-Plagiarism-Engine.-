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

// ── Redis Key Patterns (not Kafka, but kept here as the shared contract) ───

export const REDIS_KEYS = {
  jobStatus: (jobId: string) => `job:${jobId}:status`,
  jobEvents: (jobId: string) => `job:${jobId}:events`,
  jobPairs: (jobId: string) => `job:${jobId}:pairs`,
  pairReport: (pairId: string) => `pair:${pairId}:report`,
  workerHeartbeat: (hostname: string) => `worker:${hostname}:heartbeat`,
} as const;

export const KAFKA_TOPICS = {
  SUBMISSIONS: 'submissions',
  SUSPICIOUS_PAIRS: 'suspicious-pairs',
  FORENSIC_RESULTS: 'forensic-results',
  JOB_LIFECYCLE: 'job-lifecycle',
  DEAD_LETTER: 'dead-letter',
} as const;
