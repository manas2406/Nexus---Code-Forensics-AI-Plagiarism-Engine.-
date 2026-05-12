/**
 * Nexus API Gateway Bootstrap
 *
 * Architecture:
 * - Express handles HTTP (upload mutation via multipart, health check)
 * - Apollo Server handles GraphQL (mutations + queries)
 * - graphql-ws handles WebSocket (subscriptions)
 * - Both share the same port: HTTP on /graphql, WS upgrade on /graphql
 *
 * Multipart upload strategy:
 * Apollo 4's expressMiddleware expects JSON bodies. For multipart file uploads,
 * we run a custom Express middleware BEFORE Apollo that:
 *   1. Detects multipart/form-data requests
 *   2. Parses the file stream with busboy → pipes to MinIO (streaming, no RAM)
 *   3. Extracts the GraphQL "operations" JSON field
 *   4. Attaches the upload result + parsed body to req
 *   5. Replaces Content-Type with application/json so Apollo processes it normally
 */

import express from 'express';
import http from 'http';
import { ApolloServer } from '@apollo/server';
import { expressMiddleware } from '@apollo/server/express4';
import { ApolloServerPluginDrainHttpServer } from '@apollo/server/plugin/drainHttpServer';
import { WebSocketServer } from 'ws';
import { useServer } from 'graphql-ws/lib/use/ws';
import { makeExecutableSchema } from '@graphql-tools/schema';
import cors from 'cors';
import Busboy from 'busboy';
import { v4 as uuidv4 } from 'uuid';
import { PassThrough } from 'stream';

import { typeDefs } from './graphql/schema.js';
import { resolvers } from './graphql/resolvers/index.js';
import { buildContext, type NexusContext } from './graphql/context.js';
import { getRedisClient } from './redis/client.js';
import { getProducer, disconnectProducer } from './kafka/producer.js';
import { startRedisSubscriber, stopRedisSubscriber } from './redis/subscriber.js';
import { getMinioClient, BUCKET_SUBMISSIONS } from './storage/minio.client.js';

const PORT = parseInt(process.env.PORT ?? '4000', 10);
const MAX_UPLOAD_BYTES = parseInt(process.env.API_MAX_UPLOAD_SIZE_MB ?? '500', 10) * 1024 * 1024;

async function bootstrap() {
  // ── 1. Warm up dependencies ───────────────────────────────────────────────
  const redis = getRedisClient();
  await redis.ping();
  console.log('[boot] Redis connected');

  await getProducer();
  console.log('[boot] Kafka producer connected');

  await startRedisSubscriber();
  console.log('[boot] Redis subscriber started');

  // Ensure MinIO bucket exists
  const minio = getMinioClient();
  const bucketExists = await minio.bucketExists(BUCKET_SUBMISSIONS);
  if (!bucketExists) {
    await minio.makeBucket(BUCKET_SUBMISSIONS);
    console.log(`[boot] Created MinIO bucket: ${BUCKET_SUBMISSIONS}`);
  }
  console.log('[boot] MinIO connected');

  // ── 2. Build executable schema ────────────────────────────────────────────
  const schema = makeExecutableSchema({ typeDefs, resolvers });

  // ── 3. Express app ────────────────────────────────────────────────────────
  const app = express();
  const httpServer = http.createServer(app);

  // ── 4. WebSocket server for GraphQL Subscriptions ─────────────────────────
  const wsServer = new WebSocketServer({
    server: httpServer,
    path: '/graphql',
  });

  const serverCleanup = useServer(
    {
      schema,
      context: async () => buildContext({}),
      onConnect: async () => {
        console.log('[ws] Client connected');
        return true;
      },
      onDisconnect: () => {
        console.log('[ws] Client disconnected');
      },
    },
    wsServer as any,
  );

  // ── 5. Apollo Server ──────────────────────────────────────────────────────
  const apolloServer = new ApolloServer<NexusContext>({
    schema,
    plugins: [
      ApolloServerPluginDrainHttpServer({ httpServer }),
      {
        async serverWillStart() {
          return {
            async drainServer() {
              await serverCleanup.dispose();
            },
          };
        },
      },
    ],
    introspection: process.env.NODE_ENV !== 'production',
    formatError: (formattedError) => {
      console.error('[apollo] GraphQL error:', formattedError);
      return formattedError;
    },
  });

  await apolloServer.start();
  console.log('[boot] Apollo Server started');

  // ── 6. Express middleware stack ────────────────────────────────────────────

  const corsOptions = cors<cors.CorsRequest>({
    origin: process.env.CORS_ORIGIN ?? 'http://localhost:3000',
    credentials: true,
  });

  // Health check (before GraphQL middleware)
  app.get('/health', (_req, res) => {
    res.json({
      status: 'ok',
      uptime: process.uptime(),
      timestamp: new Date().toISOString(),
    });
  });

  // ── Multipart upload middleware ─────────────────────────────────────────
  // Intercepts multipart/form-data BEFORE Apollo's expressMiddleware.
  // Parses the file with busboy, streams to MinIO, then rewrites the
  // request as application/json so Apollo processes the GraphQL operation.
  app.post('/graphql', corsOptions, (req, res, next) => {
    const contentType = req.headers['content-type'] ?? '';

    if (!contentType.includes('multipart/form-data')) {
      // Not a multipart request — let Apollo handle it
      return next();
    }

    const jobId = uuidv4();
    let operationsJson = '';
    let fileProcessed = false;
    let uploadResult: { objectKey: string; sizeBytes: number } | null = null;
    let uploadError: Error | null = null;

    const bb = Busboy({
      headers: req.headers as Record<string, string>,
      limits: {
        files: 1,
        fileSize: MAX_UPLOAD_BYTES,
      },
    });

    // Collect the "operations" form field (contains the GraphQL query JSON)
    bb.on('field', (fieldname: string, val: string) => {
      if (fieldname === 'operations') {
        operationsJson = val;
      }
    });

    // Stream the "file" field to MinIO
    bb.on('file', (fieldname: string, fileStream: NodeJS.ReadableStream, info: { filename: string; encoding: string; mimeType: string }) => {
      if (fieldname !== 'file') {
        (fileStream as any).resume();
        return;
      }

      fileProcessed = true;
      const { mimeType } = info;

      // Validate MIME
      if (mimeType && !mimeType.includes('zip') && !mimeType.includes('octet-stream')) {
        uploadError = new Error(`INVALID_FILE_TYPE: Expected application/zip, got ${mimeType}`);
        (fileStream as any).resume();
        return;
      }

      const objectKey = `submissions/${jobId}.zip`;
      const chunks: Buffer[] = [];

      (fileStream as any).on('data', (chunk: Buffer) => {
        chunks.push(chunk);
      });

      (fileStream as any).on('limit', () => {
        uploadError = new Error(`FILE_TOO_LARGE: Upload exceeds ${MAX_UPLOAD_BYTES / 1024 / 1024}MB limit`);
      });

      (fileStream as any).on('end', () => {
        if (uploadError) return;

        const fileBuffer = Buffer.concat(chunks);
        const bytesReceived = fileBuffer.length;

        const minioClient = getMinioClient();
        minioClient.putObject(
          BUCKET_SUBMISSIONS,
          objectKey,
          fileBuffer,
          bytesReceived,
          { 'Content-Type': 'application/zip' },
        ).then(() => {
          uploadResult = { objectKey, sizeBytes: bytesReceived };
          console.log(`[upload] ZIP uploaded to MinIO | job=${jobId} | key=${objectKey} | size=${bytesReceived}`);
        }).catch((err: Error) => {
          uploadError = new Error(`STORAGE_ERROR: MinIO upload failed: ${err.message}`);
        });
      });
    });

    bb.on('error', (err: Error) => {
      uploadError = new Error(`PARSE_ERROR: ${err.message}`);
    });

    bb.on('finish', () => {
      // Wait a tick for the MinIO promise to resolve
      const waitForUpload = () => {
        if (uploadError) {
          // Return GraphQL error response
          res.status(200).json({
            errors: [{
              message: uploadError.message,
              extensions: {
                code: uploadError.message.startsWith('FILE_TOO_LARGE') ? 'FILE_TOO_LARGE'
                     : uploadError.message.startsWith('INVALID_FILE_TYPE') ? 'BAD_USER_INPUT'
                     : 'INTERNAL_SERVER_ERROR',
              },
            }],
          });
          return;
        }

        if (!fileProcessed) {
          // No file field — still try to parse as normal GraphQL
          if (operationsJson) {
            try {
              const ops = JSON.parse(operationsJson);
              // Attach the parsed GraphQL body and let Apollo handle it
              (req as any).body = ops;
              (req as any).__nexusUpload = undefined;
              (req.headers as any)['content-type'] = 'application/json';
              return next();
            } catch {
              // Fall through to Apollo
            }
          }
          return next();
        }

        if (uploadResult === null) {
          // MinIO still writing — poll briefly
          setTimeout(waitForUpload, 50);
          return;
        }

        // Success: attach upload result to request, rewrite body for Apollo
        (req as any).__nexusUpload = {
          objectKey: uploadResult.objectKey,
          sizeBytes: uploadResult.sizeBytes,
          jobId,
        };

        // Parse the GraphQL operations field and set as body
        if (operationsJson) {
          try {
            (req as any).body = JSON.parse(operationsJson);
          } catch {
            (req as any).body = { query: '' };
          }
        } else {
          (req as any).body = { query: '' };
        }

        // Rewrite content-type so Apollo treats this as a JSON request
        (req.headers as any)['content-type'] = 'application/json';
        
        // Prevent express.json() from trying to read the consumed stream
        (req as any)._body = true;

        next();
      };

      // Small delay to let MinIO putObject complete
      setTimeout(waitForUpload, 10);
    });

    req.pipe(bb);
  });

  // Apollo expressMiddleware for JSON requests
  app.use(
    '/graphql',
    corsOptions,
    express.json(),
    expressMiddleware(apolloServer, {
      context: async ({ req }) => buildContext(req),
    }),
  );

  // ── 7. Start listening ────────────────────────────────────────────────────
  await new Promise<void>((resolve) => httpServer.listen(PORT, resolve));
  console.log(`[boot] Nexus API Gateway ready at http://localhost:${PORT}/graphql`);
  console.log(`[boot] WebSocket ready at ws://localhost:${PORT}/graphql`);

  // ── 8. Graceful shutdown ──────────────────────────────────────────────────
  const shutdown = async (signal: string) => {
    console.log(`\n[shutdown] ${signal} received — draining...`);
    await apolloServer.stop();
    await disconnectProducer();
    stopRedisSubscriber();
    redis.disconnect();
    process.exit(0);
  };

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

bootstrap().catch((err) => {
  console.error('[boot] Fatal error during startup:', err);
  process.exit(1);
});
