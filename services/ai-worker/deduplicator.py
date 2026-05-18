# services/ai-worker/deduplicator.py
"""
Transitive Closure Deduplication via NetworkX.

Given a list of SuspiciousPairEvent payloads for one job, this module:
1. Builds an undirected similarity graph (nodes = files, edges = pairs)
2. Finds connected components (cheating rings)
3. For each component, selects ONE representative pair (highest similarity)
4. Returns: (representative_pairs, ring_map)

ring_map: maps every pair_id to the representative pair_id for its ring.
This allows generating synthetic reports for non-representative pairs.

Complexity: O(V + E) — linear in files + pairs. Negligible vs LLM call time.

Why this matters:
  N=200 students all copy the same solution → C(200,2)=19,900 suspicious pairs.
  Without deduplication: 19,900 LLM calls × $0.01 = $199 per job.
  With deduplication: 1 LLM call + 19,899 synthetic reports. Cost: $0.01.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

import networkx as nx

logger = logging.getLogger("nexus.deduplicator")


@dataclass
class DeduplicationResult:
    """Result of the transitive closure deduplication pass."""

    # Pairs that need actual LLM analysis — one per connected component (cheating ring)
    representative_pairs: list[dict[str, Any]]

    # Maps pair_id → representative pair_id for the same ring.
    # For representative pairs: ring_map[pair_id] == pair_id (maps to itself)
    ring_map: dict[str, str]

    # Full component info for reporting
    # component_id → list of pair_ids in that component
    components: dict[str, list[str]]

    @property
    def llm_call_count(self) -> int:
        return len(self.representative_pairs)

    @property
    def total_pairs(self) -> int:
        return len(self.ring_map)

    @property
    def reduction_count(self) -> int:
        return self.total_pairs - self.llm_call_count

    @property
    def reduction_pct(self) -> float:
        if self.total_pairs == 0:
            return 0.0
        return (self.reduction_count / self.total_pairs) * 100


def deduplicate_pairs(
    pair_payloads: list[dict[str, Any]],
) -> DeduplicationResult:
    """
    Deduplicate suspicious pairs by finding cheating rings via connected components.

    Args:
        pair_payloads: List of SuspiciousPairEvent dicts from Kafka.
                       Each must have: pairId, fileAName, fileBName, similarityScore.

    Returns:
        DeduplicationResult with representative pairs and ring membership map.

    Example:
        Input pairs: (alice,bob)=0.91, (alice,charlie)=0.87, (bob,charlie)=0.89
        Graph: alice -- bob -- charlie -- alice (fully connected ring)
        Connected component: {alice, bob, charlie}
        Representative pair: (alice,bob) with score=0.91 (highest)
        ring_map: {
            "pair-alice-bob":     "pair-alice-bob",      # representative → itself
            "pair-alice-charlie": "pair-alice-bob",      # non-rep → rep
            "pair-bob-charlie":   "pair-alice-bob",      # non-rep → rep
        }
        LLM calls: 1 (down from 3)
    """
    if not pair_payloads:
        return DeduplicationResult(
            representative_pairs=[],
            ring_map={},
            components={},
        )

    # ── Build similarity graph ────────────────────────────────────────────────
    G = nx.Graph()

    # Index payloads by pair_id for fast lookup
    pair_index: dict[str, dict[str, Any]] = {}

    for payload in pair_payloads:
        pair_id = payload["pairId"]
        file_a  = payload["fileAName"]
        file_b  = payload["fileBName"]
        score   = float(payload["similarityScore"])

        pair_index[pair_id] = payload

        G.add_node(file_a)
        G.add_node(file_b)

        # Add edge weighted by similarity. If two files appear in multiple pairs
        # (defensive: shouldn't happen, but keep highest-score edge).
        if G.has_edge(file_a, file_b):
            if score > G[file_a][file_b]["weight"]:
                G[file_a][file_b]["weight"]  = score
                G[file_a][file_b]["pair_id"] = pair_id
        else:
            G.add_edge(file_a, file_b, weight=score, pair_id=pair_id)

    logger.info(
        "Similarity graph built | nodes=%d (files) | edges=%d (pairs)",
        G.number_of_nodes(), G.number_of_edges(),
    )

    # ── Find connected components (cheating rings) ────────────────────────────
    ring_map:             dict[str, str]         = {}
    components:           dict[str, list[str]]   = {}
    representative_pairs: list[dict[str, Any]]   = []
    seen_pair_ids: set[str] = set()

    for i, component_nodes in enumerate(nx.connected_components(G)):
        component_id = f"ring-{i}"

        # Find all pairs whose BOTH files are within this component
        component_pairs: list[dict[str, Any]] = []
        for payload in pair_payloads:
            if (payload["fileAName"] in component_nodes
                    and payload["fileBName"] in component_nodes):
                component_pairs.append(payload)

        if not component_pairs:
            continue

        # Select representative: highest similarity score.
        # Ties broken alphabetically by fileAName for determinism.
        representative = max(
            component_pairs,
            key=lambda p: (float(p["similarityScore"]), p["fileAName"]),
        )
        rep_pair_id = representative["pairId"]

        representative_pairs.append(representative)
        seen_pair_ids.add(rep_pair_id)
        components[component_id] = [p["pairId"] for p in component_pairs]

        # Map every pair in this component → representative pair_id
        for p in component_pairs:
            ring_map[p["pairId"]] = rep_pair_id
            seen_pair_ids.add(p["pairId"])

        if len(component_pairs) > 1:
            logger.info(
                "Ring %s: %d files, %d pairs → 1 LLM call "
                "(representative: %s↔%s, score=%.3f)",
                component_id,
                len(component_nodes),
                len(component_pairs),
                representative["fileAName"],
                representative["fileBName"],
                float(representative["similarityScore"]),
            )

    # Pairs that weren't assigned to any component (isolated — maps to itself)
    for payload in pair_payloads:
        pid = payload["pairId"]
        if pid not in ring_map:
            ring_map[pid] = pid
            representative_pairs.append(payload)

    result = DeduplicationResult(
        representative_pairs=representative_pairs,
        ring_map=ring_map,
        components=components,
    )

    logger.info(
        "Deduplication complete | total=%d | llm_calls=%d | reduced=%d (%.1f%%)",
        result.total_pairs,
        result.llm_call_count,
        result.reduction_count,
        result.reduction_pct,
    )

    return result


def build_synthetic_report(
    pair_payload: dict[str, Any],
    representative_report_key: str,
) -> dict[str, Any]:
    """
    Build a ForensicReport dict for a non-representative pair.

    Instead of calling the LLM, we reference the representative pair's report.
    The frontend can follow the reference to show the same forensic details.

    Args:
        pair_payload:              The SuspiciousPairEvent for the non-representative pair.
        representative_report_key: MinIO key of the representative's report.

    Returns:
        A dict matching the ForensicReport JSON schema, with a reference to the
        representative report and a note explaining the deduplication.
    """
    return {
        "pairId":                   pair_payload["pairId"],
        "similarityScore":          float(pair_payload["similarityScore"]),
        "obfuscationTechniques":    [],
        "evidence":                 [],
        "verdict":                  "SEE_REPRESENTATIVE",   # Special verdict for UI
        "confidence":               0.0,
        "analystNotes": (
            "This pair is part of a detected cheating ring. "
            "Forensic analysis was performed on the highest-similarity representative pair. "
            "See the representative report for obfuscation technique details."
        ),
        "rawLlmResponse":           "",
        "isRepresentative":         False,
        "representativeReportKey":  representative_report_key,
        "schemaVersion":            1,
        "generatedAt":              time.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
