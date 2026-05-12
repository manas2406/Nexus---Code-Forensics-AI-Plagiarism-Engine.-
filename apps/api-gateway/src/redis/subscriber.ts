/**
 * Redis Pub/Sub → GraphQL Subscriptions bridge.
 */

import { Redis } from 'ioredis';
import { getPubSub, JOB_STATUS_UPDATED } from './pubsub.js';

let _subscriber: Redis | null = null;

export async function startRedisSubscriber(): Promise<void> {
  if (_subscriber) return;

  _subscriber = new Redis({
    host: process.env.REDIS_HOST ?? 'localhost',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
  });

  _subscriber.on('error', (err: Error) => {
    console.error('[redis-sub] Error:', err.message);
  });

  await _subscriber.psubscribe('job:*:events');

  _subscriber.on('pmessage', (_pattern: string, channel: string, message: string) => {
    const parts = channel.split(':');
    if (parts.length < 3) return;

    let payload: Record<string, unknown>;
    try {
      payload = JSON.parse(message);
    } catch {
      console.error('[redis-sub] Invalid JSON on channel:', channel, message);
      return;
    }

    const pubsub = getPubSub();
    pubsub.publish(JOB_STATUS_UPDATED, {
      jobStatusUpdated: {
        jobId:     payload['jobId'],
        status:    payload['status'],
        progress:  payload['progress'],
        message:   payload['message'],
        timestamp: payload['timestamp'],
      },
    });

    console.log(
      `[redis-sub] Bridged event | job=${payload['jobId']} | status=${payload['status']}`,
    );
  });

  console.log('[redis-sub] Subscribed to pattern: job:*:events');
}

export function stopRedisSubscriber(): void {
  _subscriber?.disconnect();
  _subscriber = null;
}
