/**
 * Idempotent Kafka producer for Nexus API Gateway.
 *
 * idempotent: true → exactly-once delivery per session.
 * Without this, a network retry could produce duplicate JOB_CREATED events,
 * causing the hash worker to process the same ZIP twice.
 */

import {
  Kafka,
  type Producer,
  CompressionTypes,
  type RecordMetadata,
  logLevel,
} from 'kafkajs';

// ── Kafka client singleton ──────────────────────────────────────────────────

const kafka = new Kafka({
  clientId: 'nexus-api-gateway',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9093').split(','),
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 8,
    maxRetryTime: 30_000,
    multiplier: 2,
  },
});

let _producer: Producer | null = null;

export async function getProducer(): Promise<Producer> {
  if (_producer) return _producer;

  _producer = kafka.producer({
    idempotent: true,
    maxInFlightRequests: 1,
  });

  await _producer.connect();
  console.log('[kafka] Producer connected');

  return _producer;
}

export async function disconnectProducer(): Promise<void> {
  if (_producer) {
    await _producer.disconnect();
    _producer = null;
    console.log('[kafka] Producer disconnected');
  }
}

// ── Topic name constants ────────────────────────────────────────────────────

export const TOPICS = {
  SUBMISSIONS:      'submissions',
  SUSPICIOUS_PAIRS: 'suspicious-pairs',
  FORENSIC_RESULTS: 'forensic-results',
  JOB_LIFECYCLE:    'job-lifecycle',
  DEAD_LETTER:      'dead-letter',
} as const;

// ── Typed event interface ───────────────────────────────────────────────────

export interface JobCreatedEvent {
  schemaVersion:       1;
  eventType:           'JOB_CREATED';
  jobId:               string;
  bucketName:          string;
  objectKey:           string;
  /** Alias of objectKey — required by hash-worker handler.py (payload.get("submissionZipKey")) */
  submissionZipKey:    string;
  submittedAt:         string;
  fileCount:           number;
  similarityThreshold: number;
  label?:              string;
}

// ── Producer helper ─────────────────────────────────────────────────────────

export async function publishJobCreated(
  event: JobCreatedEvent,
): Promise<RecordMetadata> {
  const producer = await getProducer();

  const results = await producer.send({
    topic: TOPICS.SUBMISSIONS,
    messages: [
      {
        key: event.jobId,
        value: JSON.stringify(event),
        headers: {
          'event-type':     'JOB_CREATED',
          'schema-version': '1',
          'content-type':   'application/json',
          'produced-by':    'nexus-api-gateway',
          'produced-at':    new Date().toISOString(),
        },
      },
    ],
  });

  const meta = results[0];
  console.log(
    `[kafka] JOB_CREATED produced | job=${event.jobId} | partition=${meta.partition} | offset=${meta.offset}`,
  );

  return meta;
}
