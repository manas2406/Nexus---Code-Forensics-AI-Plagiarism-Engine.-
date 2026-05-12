/**
 * Root resolvers for the Nexus API Gateway.
 *
 * - Mutation.uploadSubmissions: ZIP → MinIO → Redis PENDING → Kafka JOB_CREATED → 202
 * - Query.jobStatus: Redis HGETALL
 * - Query.suspiciousPairs: Redis LRANGE pair IDs
 * - Subscription.jobStatusUpdated: withFilter(jobId) on Redis Pub/Sub events
 */

import { withFilter } from 'graphql-subscriptions';
import { GraphQLError } from 'graphql';
import { v4 as uuidv4 } from 'uuid';
import { z } from 'zod';

import { streamZipToMinio, UploadError } from '../../storage/upload.handler.js';
import { publishJobCreated, TOPICS } from '../../kafka/producer.js';
import { initJobStatus, getJobStatusRecord, getJobPairs } from '../../redis/job-state.js';
import { REDIS_KEYS } from '../../redis/client.js';
import { getPubSub, JOB_STATUS_UPDATED } from '../../redis/pubsub.js';
import type { NexusContext } from '../context.js';

// ── Input validation ────────────────────────────────────────────────────────

const UploadInputSchema = z.object({
  similarityThreshold: z.number().min(0).max(1).default(0.70),
  label: z.string().max(200).optional(),
});


export const resolvers = {

  // ── Mutation ──────────────────────────────────────────────────────────────

  Mutation: {
    uploadSubmissions: async (
      _parent: unknown,
      { input }: { input?: { similarityThreshold?: number; label?: string } },
      context: NexusContext,
    ) => {
      // ── 1. Validate input ───────────────────────────────────────────────
      const parsed = UploadInputSchema.safeParse(input ?? {});
      if (!parsed.success) {
        throw new GraphQLError('Invalid input', {
          extensions: {
            code: 'BAD_USER_INPUT',
            details: parsed.error.flatten(),
          },
        });
      }
      const { similarityThreshold, label } = parsed.data;

      // ── 2. Get the raw request for multipart parsing ──────────────────
      const req = context.req;
      if (!req) {
        throw new GraphQLError('No request context available', {
          extensions: { code: 'INTERNAL_SERVER_ERROR' },
        });
      }

      // The upload handler is invoked from the multipart middleware.
      // Check if the file was already parsed and attached to the request.
      const uploadResult = (req as any).__nexusUpload as
        | { objectKey: string; sizeBytes: number; jobId: string }
        | undefined;

      if (!uploadResult) {
        throw new GraphQLError(
          'uploadSubmissions requires a multipart/form-data request with a "file" field. ' +
          'Send the ZIP file as a form field named "file".',
          { extensions: { code: 'BAD_USER_INPUT' } },
        );
      }

      const jobId = uploadResult.jobId;

      // ── 3. Initialize Redis state BEFORE Kafka produce ────────────────
      // Guarantees: any subscriber polling immediately sees PENDING, not 404
      try {
        await initJobStatus(jobId, label);
      } catch (err) {
        throw new GraphQLError('Failed to initialize job state', {
          extensions: { code: 'INTERNAL_SERVER_ERROR' },
        });
      }

      // ── 4. Produce JOB_CREATED event to Kafka ─────────────────────────
      try {
        await publishJobCreated({
          schemaVersion:       1,
          eventType:           'JOB_CREATED',
          jobId,
          bucketName:          'nexus-submissions',
          objectKey:           uploadResult.objectKey,
          submittedAt:         new Date().toISOString(),
          fileCount:           -1,    // Unknown until worker unzips
          similarityThreshold,
          label,
        });
      } catch (err) {
        // Kafka failed AFTER MinIO success + Redis PENDING.
        // Mark FAILED in Redis so the client doesn't wait forever.
        await context.redis.hset(REDIS_KEYS.jobStatus(jobId), {
          status:     'FAILED',
          progress:   '0',
          message:    'Job created but event broker unavailable. Please retry.',
          updated_at: new Date().toISOString(),
        });

        console.error(`[mutation] Kafka produce failed | job=${jobId}:`, err);
        throw new GraphQLError(
          'Job created but could not be queued for processing. Please retry.',
          { extensions: { code: 'BROKER_ERROR' } },
        );
      }

      console.log(
        `[mutation] Upload complete | job=${jobId} | key=${uploadResult.objectKey} | size=${uploadResult.sizeBytes}`,
      );

      // ── 5. Return 202-equivalent response ─────────────────────────────
      return {
        jobId,
        status:    'PENDING',
        progress:  0,
        message:   label ? `Job "${label}" queued` : 'Job queued for processing',
        updatedAt: new Date().toISOString(),
      };
    },
  },


  // ── Query ─────────────────────────────────────────────────────────────────

  Query: {
    jobStatus: async (
      _parent: unknown,
      { jobId }: { jobId: string },
      _context: NexusContext,
    ) => {
      const record = await getJobStatusRecord(jobId);
      if (!record) return null;

      return {
        jobId:    record.job_id,
        status:   record.status,
        progress: parseInt(record.progress, 10),
        message:  record.message,
        updatedAt: record.updated_at,
      };
    },

    suspiciousPairs: async (
      _parent: unknown,
      { jobId }: { jobId: string },
      _context: NexusContext,
    ) => {
      const pairIds = await getJobPairs(jobId);
      return pairIds.map((pairId) => ({ pairId, jobId }));
    },
  },


  // ── Subscription ──────────────────────────────────────────────────────────

  Subscription: {
    jobStatusUpdated: {
      /**
       * withFilter ensures each subscriber ONLY receives events for their
       * specific jobId. Without this filter, subscriber A watching job-001
       * would receive events for job-002 — a correctness bug + data leak.
       */
      subscribe: withFilter(
        () => getPubSub().asyncIterator<{ jobStatusUpdated: unknown }>(JOB_STATUS_UPDATED),
        (
          payload: { jobStatusUpdated: { jobId: string } },
          variables: { jobId: string },
        ) => {
          return payload.jobStatusUpdated.jobId === variables.jobId;
        },
      ),

      resolve: (payload: { jobStatusUpdated: unknown }) => {
        return payload.jobStatusUpdated;
      },
    },
  },
};
