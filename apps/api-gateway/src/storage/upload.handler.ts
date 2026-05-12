/**
 * Streaming ZIP upload to MinIO.
 *
 * Architecture:
 *   HTTP multipart stream
 *     → busboy parser
 *       → file field stream (Readable)
 *         → PassThrough (byte counting)
 *           → MinIO putObject (streaming, S3 multipart internally)
 *             → object stored in nexus-submissions/submissions/{jobId}.zip
 *
 * Why busboy? Apollo Server's expressMiddleware expects JSON bodies.
 * For multipart file uploads, we parse the raw request stream with busboy
 * BEFORE Apollo sees it. This means the file is never buffered in Node.js
 * heap — bytes flow directly from the client to MinIO.
 *
 * The critical constraint: MinIO putObject with size=-1 uses S3 multipart
 * upload internally (5MB chunks per S3 spec). This is correct for unknown
 * Content-Length.
 */

import Busboy from 'busboy';
import type { IncomingMessage } from 'http';
import { PassThrough } from 'stream';
import { getMinioClient, BUCKET_SUBMISSIONS } from './minio.client.js';

const MAX_UPLOAD_BYTES = parseInt(process.env.API_MAX_UPLOAD_SIZE_MB ?? '500', 10) * 1024 * 1024;

export interface UploadResult {
  objectKey: string;     // MinIO key: submissions/{jobId}.zip
  sizeBytes: number;     // Actual bytes received
}

export class UploadError extends Error {
  constructor(
    public readonly code: string,
    message: string,
  ) {
    super(message);
    this.name = 'UploadError';
  }
}

export async function streamZipToMinio(
  req: IncomingMessage,
  jobId: string,
): Promise<UploadResult> {
  return new Promise((resolve, reject) => {
    const objectKey = `submissions/${jobId}.zip`;
    const minio     = getMinioClient();

    let fileFieldFound = false;

    const bb = Busboy({
      headers: req.headers as Record<string, string>,
      limits: {
        files: 1,
        fileSize: MAX_UPLOAD_BYTES,
      },
    });

    bb.on('file', (fieldname: string, fileStream: NodeJS.ReadableStream, info: { filename: string; encoding: string; mimeType: string }) => {
      if (fieldname !== 'file') {
        (fileStream as any).resume();
        return;
      }

      fileFieldFound = true;
      const { mimeType } = info;

      // Validate MIME type
      if (mimeType && !mimeType.includes('zip') && !mimeType.includes('octet-stream')) {
        const err = new UploadError(
          'INVALID_FILE_TYPE',
          `Expected application/zip, got ${mimeType}`,
        );
        (fileStream as any).destroy(err);
        reject(err);
        return;
      }

      let bytesReceived = 0;
      const countingStream = new PassThrough();

      countingStream.on('data', (chunk: Buffer) => {
        bytesReceived += chunk.length;
      });

      (fileStream as any).on('limit', () => {
        const err = new UploadError(
          'FILE_TOO_LARGE',
          `Upload exceeds ${MAX_UPLOAD_BYTES / 1024 / 1024}MB limit`,
        );
        countingStream.destroy(err);
        reject(err);
      });

      (fileStream as NodeJS.ReadableStream).pipe(countingStream);

      // MinIO putObject with size=-1 → S3 multipart upload (no RAM buffer)
      minio.putObject(
        BUCKET_SUBMISSIONS,
        objectKey,
        countingStream,
        -1,
        { 'Content-Type': 'application/zip' },
      ).then(() => {
        resolve({ objectKey, sizeBytes: bytesReceived });
      }).catch((err: Error) => {
        reject(new UploadError('STORAGE_ERROR', `MinIO upload failed: ${err.message}`));
      });
    });

    bb.on('filesLimit', () => {
      reject(new UploadError('INVALID_REQUEST', 'Too many file fields'));
    });

    bb.on('finish', () => {
      if (!fileFieldFound) {
        reject(new UploadError(
          'MISSING_FILE',
          'No file field found in multipart request. Expected field name: "file"',
        ));
      }
    });

    bb.on('error', (err: Error) => {
      reject(new UploadError('PARSE_ERROR', `Multipart parse error: ${err.message}`));
    });

    req.pipe(bb);
  });
}
