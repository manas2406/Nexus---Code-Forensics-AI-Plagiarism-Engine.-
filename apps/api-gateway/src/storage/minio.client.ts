/**
 * MinIO client singleton.
 *
 * Provides a configured MinIO SDK client for object storage operations.
 * Used by the upload handler (streaming ZIPs) and resolvers (reading results).
 */

import * as Minio from 'minio';

let _client: Minio.Client | null = null;

export function getMinioClient(): Minio.Client {
  if (_client) return _client;

  _client = new Minio.Client({
    endPoint:  process.env.MINIO_ENDPOINT  ?? 'localhost',
    port:      parseInt(process.env.MINIO_PORT ?? '9000', 10),
    accessKey: process.env.MINIO_ROOT_USER  ?? process.env.MINIO_ACCESS_KEY ?? 'nexus',
    secretKey: process.env.MINIO_ROOT_PASSWORD ?? process.env.MINIO_SECRET_KEY ?? 'nexus-secret-change-in-prod',
    useSSL:    process.env.MINIO_USE_SSL === 'true',
  });

  return _client;
}

export const BUCKET_SUBMISSIONS = process.env.MINIO_BUCKET_SUBMISSIONS ?? 'nexus-submissions';
export const BUCKET_REPORTS     = process.env.MINIO_BUCKET_REPORTS     ?? 'nexus-reports';
