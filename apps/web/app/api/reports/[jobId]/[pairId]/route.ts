import { NextRequest, NextResponse } from 'next/server';
import { Client } from 'minio';

const minioClient = new Client({
  endPoint: process.env.MINIO_ENDPOINT || 'localhost',
  port: parseInt(process.env.MINIO_PORT || '9000', 10),
  useSSL: process.env.MINIO_USE_SSL === 'true',
  accessKey: process.env.MINIO_ACCESS_KEY || 'minioadmin',
  secretKey: process.env.MINIO_SECRET_KEY || 'minioadmin'
});

export async function GET(
  req: NextRequest,
  { params }: { params: { jobId: string; pairId: string } }
) {
  const { jobId, pairId } = params;
  const bucketName = process.env.MINIO_BUCKET || 'nexus-reports';
  const objectName = `reports/${jobId}/${pairId}.json`;

  try {
    const dataStream = await minioClient.getObject(bucketName, objectName);
    
    const chunks: Buffer[] = [];
    for await (const chunk of dataStream) {
      chunks.push(Buffer.from(chunk));
    }
    
    const jsonString = Buffer.concat(chunks).toString('utf-8');
    
    return new NextResponse(jsonString, {
      status: 200,
      headers: {
        'Content-Type': 'application/json'
      }
    });
  } catch (error: any) {
    if (error.code === 'NoSuchKey') {
      return new NextResponse(JSON.stringify({ error: 'Report not found' }), {
        status: 404,
        headers: { 'Content-Type': 'application/json' }
      });
    }
    console.error('Error fetching report from MinIO:', error);
    return new NextResponse(JSON.stringify({ error: 'Internal Server Error' }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}
