import express from 'express';
import type { JobStatus } from '@nexus/types';

const app = express();
const PORT = process.env['PORT'] ?? 4000;

// Demonstrate that shared types resolve correctly
const currentStatus: JobStatus = 'PENDING';

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', currentStatus });
});

app.listen(PORT, () => {
  console.log(`[api-gateway] listening on http://localhost:${String(PORT)}`);
});
