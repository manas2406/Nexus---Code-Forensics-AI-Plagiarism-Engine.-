"use client";

import React, { useState } from 'react';
import { gql } from '@apollo/client';
import { useQuery } from '@apollo/client/react';
import ForensicGraph from '../../../components/ForensicGraph';
import ReportModal from '../../../components/ReportModal';
import { SuspiciousPair } from '@nexus/types';

const GET_SUSPICIOUS_PAIRS = gql`
  query GetSuspiciousPairs($jobId: String!) {
    suspiciousPairs(jobId: $jobId) {
      pairId
      fileA
      fileB
      similarity
    }
  }
`;

export default function JobPage({ params }: { params: { jobId: string } }) {
  const { jobId } = params;
  const [selectedPair, setSelectedPair] = useState<SuspiciousPair | null>(null);

  // We handle loading/error inside or pass it down, but the prompt says 
  // pairs={data?.suspiciousPairs ?? []} which means we default to empty array
  const { data, loading, error } = useQuery<{ suspiciousPairs: SuspiciousPair[] }>(GET_SUSPICIOUS_PAIRS, {
    variables: { jobId },
    skip: !jobId, // just in case
  });

  return (
    <div className="flex flex-col h-screen">
      {/* Dev A's components above */}
      <div className="flex-1 min-h-[600px] relative">
        <ForensicGraph
          jobId={jobId}
          pairs={data?.suspiciousPairs ?? []}
          onEdgeClick={setSelectedPair}
        />
        <ReportModal
          pair={selectedPair}
          jobId={jobId}
          onClose={() => setSelectedPair(null)}
        />
      </div>
    </div>
  );
}
