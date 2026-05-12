/**
 * GraphQL context — services injected into every resolver.
 */

import type { Request } from 'express';
import type * as Minio from 'minio';
import type { Producer } from 'kafkajs';
import type { Redis } from 'ioredis';

import { getRedisClient } from '../redis/client.js';
import { getMinioClient } from '../storage/minio.client.js';
import { getProducer } from '../kafka/producer.js';

export interface NexusContext {
  redis: Redis;
  minio: Minio.Client;
  kafkaProducer: Producer;
  req?: Request;
}

export async function buildContext(reqOrCtx: Request | unknown): Promise<NexusContext> {
  return {
    redis:         getRedisClient(),
    minio:         getMinioClient(),
    kafkaProducer: await getProducer(),
    req:           reqOrCtx as Request | undefined,
  };
}
