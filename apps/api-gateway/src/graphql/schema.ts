/**
 * GraphQL SDL schema for Nexus API Gateway.
 *
 * Types mirror shared/types/index.ts exactly.
 * Changes here MUST be reflected in the TypeScript event interfaces.
 */

export const typeDefs = /* GraphQL */ `
  # ── Scalars ──────────────────────────────────────────────────────────────────

  """ISO 8601 UTC datetime string. Example: "2024-01-15T10:30:00Z"."""
  scalar DateTime

  """0.0–1.0 similarity ratio. Example: 0.8473."""
  scalar SimilarityScore


  # ── Enums ─────────────────────────────────────────────────────────────────────

  """
  Job status state machine.
  Transitions are monotonically forward — a job never goes backward.
  """
  enum JobStatus {
    PENDING
    EXTRACTING
    PARSING
    HASHING
    COMPARING
    AI_ANALYSIS
    COMPLETE
    FAILED
  }


  # ── Types ─────────────────────────────────────────────────────────────────────

  type Job {
    jobId: ID!
    status: JobStatus!
    progress: Int!
    message: String!
    updatedAt: DateTime
    suspiciousPairCount: Int
  }

  """
  A pair of submissions flagged as structurally similar.
  Populated after the hash worker completes COMPARING stage.
  """
  type SuspiciousPair {
    pairId: ID!
    jobId: ID!
    fileAName: String
    fileBName: String
    similarityScore: SimilarityScore
    forensicReportKey: String
  }

  """
  Real-time status event streamed to subscribed clients.
  Published once per status transition by the hash or AI worker.
  """
  type JobStatusEvent {
    jobId: ID!
    status: JobStatus!
    progress: Int!
    message: String!
    timestamp: DateTime!
    suspiciousPairs: [SuspiciousPair!]
  }


  # ── Input Types ───────────────────────────────────────────────────────────────

  input UploadSubmissionsInput {
    """
    Similarity threshold for flagging pairs. Default: 0.70.
    Range: 0.0 (flag everything) to 1.0 (flag only identical files).
    """
    similarityThreshold: Float = 0.70

    """
    Optional label for the job (e.g., "CS101 Assignment 3").
    """
    label: String
  }


  # ── Mutations ─────────────────────────────────────────────────────────────────

  type Mutation {
    """
    Upload a ZIP file of C++ code submissions for plagiarism analysis.

    The ZIP file is passed as a multipart form field named "file".
    This mutation:
    1. Streams the ZIP to MinIO (never buffered in API memory)
    2. Writes PENDING status to Redis
    3. Produces a JOB_CREATED event to Kafka
    4. Returns immediately with the jobId — processing is asynchronous

    Use jobStatusUpdated subscription to track progress.
    """
    uploadSubmissions(input: UploadSubmissionsInput): Job!
  }


  # ── Queries ───────────────────────────────────────────────────────────────────

  type Query {
    """
    Retrieve the current status of a job.
    Returns null if the job does not exist or has expired (TTL: 24h).
    """
    jobStatus(jobId: ID!): Job

    """
    Retrieve suspicious pairs for a completed job.
    Returns empty list if job is not yet complete.
    """
    suspiciousPairs(jobId: ID!): [SuspiciousPair!]!
  }


  # ── Subscriptions ─────────────────────────────────────────────────────────────

  type Subscription {
    """
    Subscribe to real-time status updates for a specific job.

    Events are published by hash and AI workers via Redis Pub/Sub.
    The subscription completes when status reaches COMPLETE or FAILED.
    """
    jobStatusUpdated(jobId: ID!): JobStatusEvent!
  }
`;
