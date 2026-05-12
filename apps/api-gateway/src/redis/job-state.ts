/**
 * API Gateway's Redis job state writes.
 *
 * The API gateway only writes the initial PENDING state.
 * All subsequent transitions (EXTRACTING → PARSING → ... → COMPLETE)
 * are written by the hash worker and AI worker.
 */

import { getRedisClient, REDIS_KEYS, JOB_STATUS_TTL } from './client.js';

export interface JobStatusRecord {
  job_id:     string;
  status:     string;
  progress:   string;
  message:    string;
  updated_at: string;
}

export async function initJobStatus(
  jobId: string,
  label?: string,
): Promise<void> {
  const redis = getRedisClient();
  const key   = REDIS_KEYS.jobStatus(jobId);
  const now   = new Date().toISOString();

  // Write initial PENDING state BEFORE Kafka produce.
  // Guarantees: any subscriber polling immediately sees PENDING, not 404.
  await redis.hset(key, {
    job_id:     jobId,
    status:     'PENDING',
    progress:   '0',
    message:    label ? `Job "${label}" queued` : 'Job queued for processing',
    updated_at: now,
  });

  await redis.expire(key, JOB_STATUS_TTL);

  // Publish initial PENDING event to Pub/Sub for any active subscription
  const payload = JSON.stringify({
    jobId,
    status:    'PENDING',
    progress:  0,
    message:   label ? `Job "${label}" queued` : 'Job queued for processing',
    timestamp: now,
  });
  await redis.publish(REDIS_KEYS.jobEvents(jobId), payload);

  console.log(`[redis] Job initialized | job=${jobId} | status=PENDING`);
}

export async function getJobStatusRecord(
  jobId: string,
): Promise<JobStatusRecord | null> {
  const redis = getRedisClient();
  const raw   = await redis.hgetall(REDIS_KEYS.jobStatus(jobId));

  if (!raw || Object.keys(raw).length === 0) return null;

  return raw as unknown as JobStatusRecord;
}

export async function getJobPairs(jobId: string): Promise<string[]> {
  const redis = getRedisClient();
  return redis.lrange(REDIS_KEYS.jobPairs(jobId), 0, -1);
}
