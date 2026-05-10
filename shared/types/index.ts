export type JobStatus =
  | 'PENDING'
  | 'PARSING'
  | 'HASHING'
  | 'ANALYSING'
  | 'COMPLETE'
  | 'FAILED';

export interface JobCreatedEvent {
  jobId: string;
  submissionZipKey: string; // MinIO object key
  createdAt: string; // ISO 8601
}

export interface JobStatusUpdatedEvent {
  jobId: string;
  status: JobStatus;
  updatedAt: string;
  detail?: string;
}

export interface SuspiciousPairEvent {
  jobId: string;
  pairId: string;
  fileA: string;
  fileB: string;
  similarity: number; // 0.0–1.0 Jaccard
}

export interface ForensicReport {
  pairId: string;
  verdict: 'LIKELY_PLAGIARISM' | 'POSSIBLE_COINCIDENCE' | 'INCONCLUSIVE';
  confidence: number; // 0.0–1.0
  obfuscationTechniques: string[];
  evidenceSummary: string;
  rawLlmResponse: string;
}
