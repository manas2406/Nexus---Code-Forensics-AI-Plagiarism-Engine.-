"use client";

import React, { useEffect, useState, useCallback } from 'react';
import { SuspiciousPair, ForensicReport } from '@nexus/types';
import VerdictBadge from './VerdictBadge';

interface ReportModalProps {
  pair: SuspiciousPair | null;
  jobId: string;
  onClose: () => void;
}

export default function ReportModal({ pair, jobId, onClose }: ReportModalProps) {
  const [report, setReport] = useState<ForensicReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchReport = useCallback(() => {
    if (!pair) return;
    setLoading(true);
    setError(null);
    fetch(`/api/reports/${jobId}/${pair.pairId}`)
      .then(r => {
        if (!r.ok) {
          throw new Error('Failed to load report');
        }
        return r.json();
      })
      .then(setReport)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false));
  }, [jobId, pair]);

  useEffect(() => {
    if (pair) {
      fetchReport();
    } else {
      setReport(null);
      setError(null);
      setLoading(false);
    }
  }, [pair, fetchReport]);

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        onClose();
      }
    };
    if (pair) {
      window.addEventListener('keydown', handleKeyDown);
    }
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [pair, onClose]);

  if (!pair) return null;

  const truncateFilename = (filename: string) => {
    const parts = filename.split('/');
    return parts[parts.length - 1];
  };

  const getConfidenceColor = (verdict?: string) => {
    if (verdict === 'LIKELY_PLAGIARISM') return 'bg-red-500';
    if (verdict === 'POSSIBLE_COINCIDENCE') return 'bg-amber-500';
    return 'bg-slate-500';
  };

  return (
    <div 
      className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-[rgba(0,0,0,0.7)]"
      onClick={onClose}
    >
      <div 
        className="bg-slate-900 border border-slate-700 rounded-lg shadow-xl w-full max-w-[860px] max-h-[90vh] flex flex-col overflow-hidden text-slate-200"
        onClick={e => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-slate-700 bg-slate-800">
          <div className="flex items-center gap-2 font-mono text-sm">
            <span className="text-slate-300">{truncateFilename(pair.fileA)}</span>
            <span className="text-slate-500">↔</span>
            <span className="text-slate-300">{truncateFilename(pair.fileB)}</span>
            <span className="ml-4 px-2 py-1 bg-slate-700 rounded text-slate-300">
              {Math.round(pair.similarity * 100)}% Match
            </span>
          </div>
          <button 
            onClick={onClose}
            className="text-slate-400 hover:text-white text-xl leading-none px-2"
          >
            &times;
          </button>
        </div>

        {/* Body */}
        <div className="p-6 overflow-y-auto flex-1">
          {loading && (
            <div className="flex justify-center items-center h-40">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div>
            </div>
          )}

          {error && !loading && (
            <div className="flex flex-col items-center justify-center h-40 space-y-4">
              <p className="text-red-400">{error}</p>
              <button 
                onClick={fetchReport}
                className="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded transition-colors"
              >
                Retry
              </button>
            </div>
          )}

          {report && !loading && !error && (
            <div className="space-y-8">
              {/* Verdict */}
              <div className="flex flex-col items-center space-y-4">
                <VerdictBadge verdict={report.verdict} size="lg" />
              </div>

              {/* Confidence */}
              <div className="space-y-2">
                <div className="flex justify-between text-sm">
                  <span className="text-slate-400">Confidence</span>
                  <span>{Math.round(report.confidence * 100)}%</span>
                </div>
                <div className="w-full bg-slate-700 h-2 rounded-full overflow-hidden">
                  <div 
                    className={`h-full ${getConfidenceColor(report.verdict)}`} 
                    style={{ width: `${report.confidence * 100}%` }}
                  ></div>
                </div>
              </div>

              {/* Techniques */}
              <div className="space-y-2">
                <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Techniques detected</h3>
                <div className="flex flex-wrap gap-2">
                  {report.obfuscation_techniques && report.obfuscation_techniques.length > 0 ? (
                    report.obfuscation_techniques.map((tech, i) => (
                      <span key={i} className="px-3 py-1 bg-slate-800 border border-slate-600 rounded-full text-sm">
                        {tech}
                      </span>
                    ))
                  ) : (
                    <span className="text-slate-500 italic">None detected</span>
                  )}
                </div>
              </div>

              {/* Analysis */}
              <div className="space-y-2">
                <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Analysis</h3>
                <p className="text-slate-300 leading-relaxed whitespace-pre-wrap">
                  {report.evidence_summary}
                </p>
              </div>

              {/* Diff View */}
              <div className="space-y-2">
                <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider">Code Comparison</h3>
                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-slate-950 rounded border border-slate-700 p-4 overflow-x-auto">
                    <div className="text-xs text-slate-500 mb-2 font-mono border-b border-slate-800 pb-2">
                      {truncateFilename(pair.fileA)}
                    </div>
                    {report.file_a_diff ? (
                      <pre className="text-sm font-mono text-slate-300"><code>{report.file_a_diff}</code></pre>
                    ) : (
                      <div className="text-slate-500 italic text-sm">Source not available</div>
                    )}
                  </div>
                  <div className="bg-slate-950 rounded border border-slate-700 p-4 overflow-x-auto">
                    <div className="text-xs text-slate-500 mb-2 font-mono border-b border-slate-800 pb-2">
                      {truncateFilename(pair.fileB)}
                    </div>
                    {report.file_b_diff ? (
                      <pre className="text-sm font-mono text-slate-300"><code>{report.file_b_diff}</code></pre>
                    ) : (
                      <div className="text-slate-500 italic text-sm">Source not available</div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Footer */}
        {report?.is_fallback && (
          <div className="p-3 bg-amber-900/50 border-t border-amber-700 text-amber-200 text-sm text-center">
            Fallback report — LLM was unreachable
          </div>
        )}
      </div>
    </div>
  );
}
