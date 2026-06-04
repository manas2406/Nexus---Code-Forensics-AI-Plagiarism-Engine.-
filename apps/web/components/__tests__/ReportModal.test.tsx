import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import ReportModal from '../ReportModal';
import { expect, test, vi, beforeEach } from 'vitest';

const mockPair = {
  pairId: 'p1',
  fileA: 'file1.ts',
  fileB: 'file2.ts',
  similarity: 0.9,
};

const mockReport = {
  pairId: 'p1',
  jobId: 'job-1',
  verdict: 'LIKELY_PLAGIARISM',
  confidence: 0.95,
  obfuscation_techniques: ['Variable Renaming'],
  evidence_summary: 'Files are highly similar.',
  file_a_diff: 'const a = 1;',
  file_b_diff: 'const b = 1;',
};

beforeEach(() => {
  global.fetch = vi.fn();
});

test('test_renders_null_when_pair_is_null', () => {
  const { container } = render(<ReportModal pair={null} jobId="job-1" onClose={() => {}} />);
  expect(container.firstChild).toBeNull();
});

test('test_shows_loading_state', async () => {
  (global.fetch as any).mockImplementation(() => new Promise(() => {})); // Never resolves
  
  render(<ReportModal pair={mockPair} jobId="job-1" onClose={() => {}} />);
  
  // Checking for spinner by role or class. We used border-b-2 border-indigo-500 for spinner.
  expect(screen.getByText('↔')).toBeInTheDocument(); // Header renders
  // We can't easily query the spinner without a role, let's query the container
  const spinner = document.querySelector('.animate-spin');
  expect(spinner).toBeInTheDocument();
});

test('test_shows_verdict_badge', async () => {
  (global.fetch as any).mockResolvedValue({
    ok: true,
    json: async () => mockReport,
  });

  render(<ReportModal pair={mockPair} jobId="job-1" onClose={() => {}} />);
  
  await waitFor(() => {
    expect(screen.getByText('Likely Plagiarism')).toBeInTheDocument();
  });
});

test('test_shows_fallback_warning', async () => {
  (global.fetch as any).mockResolvedValue({
    ok: true,
    json: async () => ({ ...mockReport, is_fallback: true }),
  });

  render(<ReportModal pair={mockPair} jobId="job-1" onClose={() => {}} />);
  
  await waitFor(() => {
    expect(screen.getByText('Fallback report — LLM was unreachable')).toBeInTheDocument();
  });
});

test('test_escape_key_closes_modal', async () => {
  (global.fetch as any).mockImplementation(() => new Promise(() => {})); // Never resolves
  const onClose = vi.fn();
  render(<ReportModal pair={mockPair} jobId="job-1" onClose={onClose} />);
  
  fireEvent.keyDown(window, { key: 'Escape', code: 'Escape' });
  expect(onClose).toHaveBeenCalled();
});

test('test_retry_button_on_error', async () => {
  (global.fetch as any).mockRejectedValueOnce(new Error('Network error'));
  
  render(<ReportModal pair={mockPair} jobId="job-1" onClose={() => {}} />);
  
  await waitFor(() => {
    expect(screen.getByText('Network error')).toBeInTheDocument();
    expect(screen.getByText('Retry')).toBeInTheDocument();
  });
  
  // Setup next fetch to succeed
  (global.fetch as any).mockResolvedValueOnce({
    ok: true,
    json: async () => mockReport,
  });
  
  fireEvent.click(screen.getByText('Retry'));
  
  await waitFor(() => {
    expect(screen.getByText('Likely Plagiarism')).toBeInTheDocument();
  });
  
  expect(global.fetch).toHaveBeenCalledTimes(2);
});
