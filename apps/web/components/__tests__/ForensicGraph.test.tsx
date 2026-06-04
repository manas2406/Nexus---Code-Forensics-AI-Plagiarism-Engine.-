import { render, screen, fireEvent } from '@testing-library/react';
import { expect, test, vi } from 'vitest';

// Mock reactflow because jsdom cannot render SVG-based canvas layouts
vi.mock('reactflow', () => {
  const React = require('react');

  function MockReactFlow(props: {
    nodes: Array<{ id: string; data: { label: string }; style?: Record<string, unknown> }>;
    edges: Array<{ id: string; source: string; target: string; style?: Record<string, unknown> }>;
    onEdgeClick?: (event: React.MouseEvent, edge: { id: string }) => void;
    fitView?: boolean;
    children?: React.ReactNode;
  }) {
    return React.createElement('div', { 'data-testid': 'react-flow' }, [
      // Render nodes
      ...props.nodes.map((node: { id: string; data: { label: string }; style?: Record<string, unknown> }) =>
        React.createElement('div', {
          key: node.id,
          className: 'react-flow__node',
          'data-testid': `node-${node.id}`,
        }, node.data.label)
      ),
      // Render edges
      ...props.edges.map((edge: { id: string; source: string; target: string; style?: Record<string, unknown> }) =>
        React.createElement('div', {
          key: edge.id,
          className: 'react-flow__edge',
          'data-testid': `edge-${edge.id}`,
          'data-stroke': (edge.style as Record<string, unknown>)?.stroke,
          'data-stroke-width': (edge.style as Record<string, unknown>)?.strokeWidth,
          onClick: (e: React.MouseEvent) => props.onEdgeClick?.(e, edge),
        }, `${edge.source} → ${edge.target}`)
      ),
    ]);
  }

  return {
    __esModule: true,
    default: MockReactFlow,
    Background: () => null,
    Controls: () => null,
    useNodesState: vi.fn(),
    useEdgesState: vi.fn(),
  };
});

// Import after mock is set up
import ForensicGraph from '../ForensicGraph';

test('test_renders_empty_state', () => {
  render(<ForensicGraph jobId="job-1" pairs={[]} onEdgeClick={() => {}} />);
  expect(screen.getByText('No suspicious pairs detected')).toBeInTheDocument();
});

test('test_renders_correct_node_count', () => {
  const pairs = [
    { pairId: 'p1', fileA: 'file1.ts', fileB: 'file2.ts', similarity: 0.8 },
    { pairId: 'p2', fileA: 'file2.ts', fileB: 'file3.ts', similarity: 0.9 },
    { pairId: 'p3', fileA: 'file3.ts', fileB: 'file4.ts', similarity: 0.7 },
  ];
  const { container } = render(
    <ForensicGraph jobId="job-1" pairs={pairs} onEdgeClick={() => {}} />
  );

  const nodes = container.querySelectorAll('.react-flow__node');
  expect(nodes.length).toBe(4);
});

test('test_edge_click_calls_callback', () => {
  const onEdgeClick = vi.fn();
  const pairs = [
    { pairId: 'p1', fileA: 'file1.ts', fileB: 'file2.ts', similarity: 0.8 },
  ];

  render(
    <ForensicGraph jobId="job-1" pairs={pairs} onEdgeClick={onEdgeClick} />
  );

  const edge = screen.getByTestId('edge-p1');
  fireEvent.click(edge);

  expect(onEdgeClick).toHaveBeenCalledWith(pairs[0]);
});

test('test_high_similarity_edge_is_red', () => {
  const pairs = [
    { pairId: 'p1', fileA: 'file1.ts', fileB: 'file2.ts', similarity: 0.9 },
  ];

  render(
    <ForensicGraph jobId="job-1" pairs={pairs} onEdgeClick={() => {}} />
  );

  const edge = screen.getByTestId('edge-p1');
  expect(edge.getAttribute('data-stroke')).toBe('#ef4444');
});
