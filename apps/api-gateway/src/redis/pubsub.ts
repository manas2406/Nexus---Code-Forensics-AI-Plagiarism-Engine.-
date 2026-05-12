/**
 * graphql-redis-subscriptions PubSub instance.
 */

import { RedisPubSub } from 'graphql-redis-subscriptions';
import { Redis } from 'ioredis';

export const JOB_STATUS_UPDATED = 'JOB_STATUS_UPDATED';

let _pubsub: RedisPubSub | null = null;

export function getPubSub(): RedisPubSub {
  if (_pubsub) return _pubsub;

  const options = {
    host: process.env.REDIS_HOST ?? 'localhost',
    port: parseInt(process.env.REDIS_PORT ?? '6379', 10),
  };

  _pubsub = new RedisPubSub({
    publisher:  new Redis(options) as any,
    subscriber: new Redis(options) as any,
  });

  return _pubsub;
}
