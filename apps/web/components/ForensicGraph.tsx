"use client";

import React, { useMemo } from 'react';
import ReactFlow, { Node, Edge, Background, Controls } from 'reactflow';
import 'reactflow/dist/style.css';
import { SuspiciousPair } from '@nexus/types';

interface ForensicGraphProps {
  jobId: string;
  pairs: SuspiciousPair[];
  onEdgeClick: (pair: SuspiciousPair) => void;
}

const truncateFilename = (filename: string) => {
  const parts = filename.split('/');
  const base = parts[parts.length - 1];
  if (base.length > 20) {
    return base.substring(0, 17) + '...';
  }
  return base;
};

const getEdgeColor = (similarity: number) => {
  if (similarity >= 0.85) return '#ef4444'; // red
  if (similarity >= 0.70) return '#f59e0b'; // amber
  if (similarity >= 0.60) return '#eab308'; // yellow
  return '#94a3b8'; // default slate-400
};

export default function ForensicGraph({ jobId, pairs, onEdgeClick }: ForensicGraphProps) {
  const { nodes, edges } = useMemo(() => {
    const nodeMap = new Set<string>();
    pairs.forEach(p => {
      nodeMap.add(p.fileA);
      nodeMap.add(p.fileB);
    });

    const outNodes: Node[] = Array.from(nodeMap).map((file, index) => {
      const x = (index % 5) * 200;
      const y = Math.floor(index / 5) * 150;
      
      return {
        id: file,
        position: { x, y },
        data: { label: truncateFilename(file) },
        style: {
          background: '#1e293b',
          borderColor: '#334155',
          color: '#f8fafc',
          fontFamily: 'monospace',
          borderWidth: 2,
          borderRadius: 8,
          padding: 10,
        },
      };
    });

    const outEdges: Edge[] = pairs.map((p) => {
      return {
        id: p.pairId,
        source: p.fileA,
        target: p.fileB,
        style: {
          strokeWidth: 1 + p.similarity * 8,
          stroke: getEdgeColor(p.similarity),
        },
        type: 'straight',
      };
    });

    return { nodes: outNodes, edges: outEdges };
  }, [pairs]);

  const handleEdgeClick = (event: React.MouseEvent, edge: Edge) => {
    const pair = pairs.find(p => p.pairId === edge.id);
    if (pair) {
      onEdgeClick(pair);
    }
  };

  if (pairs.length === 0) {
    return (
      <div className="w-full h-full flex items-center justify-center bg-slate-900 border border-slate-700 min-h-[400px]">
        <p className="text-slate-400">No suspicious pairs detected</p>
      </div>
    );
  }

  return (
    <div className="w-full h-full min-h-[600px] border border-slate-700 bg-slate-900 relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onEdgeClick={handleEdgeClick}
        fitView
      >
        <Background color="#334155" gap={16} />
        <Controls />
      </ReactFlow>
      {/* Inject custom CSS for selected node border as tailwind doesn't apply cleanly to react flow internal class without important or specific targeting */}
      <style dangerouslySetInnerHTML={{__html: `
        .react-flow__node.selected {
          border-color: #6366f1 !important;
        }
      `}} />
    </div>
  );
}
