/**
 * Redis client singleton (ioredis).
 *
 * This client is used for regular commands (GET, SET, HSET, etc.).
 * A SEPARATE client is used for Pub/Sub (see subscriber.ts) because
 * ioredis puts a client in subscribe mode permanently once SUBSCRIBE
 * is called — regular commands are blocked on that connection.
 */

import { Redis } from 'ioredis';

let _client: Redis | null = null;

export function getRedisClient(): Redis {
  if (_client) return _client;

  _client = new Redis({
    host:           process.env.REDIS_HOST ?? 'localhost',
    port:           parseInt(process.env.REDIS_PORT ?? '6379', 10),
    maxRetriesPerRequest: 3,
    retryStrategy: (times: number) => {
      if (times > 10) return null;
      return Math.min(times * 200, 3000);
    },
  });

  _client.on('connect',       () => console.log('[redis] Connected'));
  _client.on('error',         (err: Error) => console.error('[redis] Error:', err.message));
  _client.on('reconnecting',  () => console.warn('[redis] Reconnecting...'));

  return _client;
}

// ── Key helpers (mirror of shared/types/index.ts REDIS_KEYS) ────────────────

export const REDIS_KEYS = {
  jobStatus:  (jobId: string) => `job:${jobId}:status`,
  jobEvents:  (jobId: string) => `job:${jobId}:events`,
  jobPairs:   (jobId: string) => `job:${jobId}:pairs`,
} as const;

export const JOB_STATUS_TTL = 86_400;  // 24 hours
