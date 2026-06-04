import React from 'react';

export interface VerdictBadgeProps {
  verdict: 'LIKELY_PLAGIARISM' | 'POSSIBLE_COINCIDENCE' | 'INCONCLUSIVE';
  size?: 'sm' | 'lg';
}

export default function VerdictBadge({ verdict, size = 'sm' }: VerdictBadgeProps) {
  const sizeClasses = size === 'sm' ? 'px-2 py-1 text-xs' : 'px-4 py-2 text-lg';
  let colorClasses = '';
  let label = '';

  switch (verdict) {
    case 'LIKELY_PLAGIARISM':
      colorClasses = 'bg-red-500 text-white';
      label = 'Likely Plagiarism';
      break;
    case 'POSSIBLE_COINCIDENCE':
      colorClasses = 'bg-amber-500 text-gray-900';
      label = 'Possible Coincidence';
      break;
    case 'INCONCLUSIVE':
      colorClasses = 'bg-slate-600 text-white';
      label = 'Inconclusive';
      break;
  }

  return (
    <span className={`inline-flex items-center justify-center font-bold rounded-full ${sizeClasses} ${colorClasses}`}>
      {label}
    </span>
  );
}
