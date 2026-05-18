"""
Unit tests for deduplicator.py — transitive closure deduplication.

All tests are pure-unit (no I/O, no external dependencies).
Run with: pytest tests/unit/test_deduplicator.py -v
"""
import sys
import os

# Add ai-worker root to path so imports work whether run from repo root or service dir
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest
from deduplicator import build_synthetic_report, deduplicate_pairs


# ── Fixtures ─────────────────────────────────────────────────────────────────

def make_pair(
    pair_id: str,
    file_a: str,
    file_b: str,
    score: float,
    job_id: str = "test-job",
) -> dict:
    return {
        "pairId":          pair_id,
        "fileAName":       file_a,
        "fileBName":       file_b,
        "similarityScore": score,
        "jobId":           job_id,
        "fileAObjectKey":  f"extracted/{job_id}/{file_a}",
        "fileBObjectKey":  f"extracted/{job_id}/{file_b}",
    }


# ── TestDeduplicatePairs ──────────────────────────────────────────────────────

class TestDeduplicatePairs:

    def test_empty_input_returns_empty_result(self):
        """Edge case: empty batch produces empty deduplication result."""
        result = deduplicate_pairs([])
        assert result.representative_pairs == []
        assert result.ring_map == {}
        assert result.llm_call_count == 0
        assert result.total_pairs == 0
        assert result.reduction_count == 0
        assert result.reduction_pct == 0.0

    def test_single_pair_is_its_own_representative(self):
        """Single pair → 1 LLM call, maps to itself."""
        pairs = [make_pair("p1", "alice.cpp", "bob.cpp", 0.85)]
        result = deduplicate_pairs(pairs)

        assert result.llm_call_count == 1
        assert result.total_pairs == 1
        assert result.ring_map["p1"] == "p1"
        assert result.reduction_count == 0
        assert result.representative_pairs[0]["pairId"] == "p1"

    def test_two_independent_pairs_each_get_llm_call(self):
        """Two disjoint pairs (no shared files) → 2 LLM calls."""
        pairs = [
            make_pair("p1", "alice.cpp",   "bob.cpp",  0.85),
            make_pair("p2", "charlie.cpp", "dave.cpp", 0.82),
        ]
        result = deduplicate_pairs(pairs)

        assert result.llm_call_count == 2
        assert result.total_pairs == 2
        assert result.ring_map["p1"] == "p1"
        assert result.ring_map["p2"] == "p2"
        assert result.reduction_count == 0

    def test_transitive_ring_collapsed_to_one_representative(self):
        """
        alice↔bob=0.91, alice↔charlie=0.87, bob↔charlie=0.89
        All three share files → one connected component.
        Representative: p-ab (highest score 0.91).
        """
        pairs = [
            make_pair("p-ab", "alice.cpp",   "bob.cpp",     0.91),
            make_pair("p-ac", "alice.cpp",   "charlie.cpp", 0.87),
            make_pair("p-bc", "bob.cpp",     "charlie.cpp", 0.89),
        ]
        result = deduplicate_pairs(pairs)

        assert result.llm_call_count == 1, \
            f"Expected 1 LLM call, got {result.llm_call_count}"

        assert result.ring_map["p-ab"] == "p-ab"   # representative
        assert result.ring_map["p-ac"] == "p-ab"   # → representative
        assert result.ring_map["p-bc"] == "p-ab"   # → representative

        rep = result.representative_pairs[0]
        assert rep["pairId"] == "p-ab"
        assert float(rep["similarityScore"]) == 0.91

    def test_massive_cheating_ring_collapses_to_one_call(self):
        """
        N=10 students all copy same source → C(10,2)=45 pairs → 1 LLM call.
        """
        students = [f"student_{chr(65 + i)}.cpp" for i in range(10)]
        pairs = []
        pair_num = 0
        for i, s_a in enumerate(students):
            for s_b in students[i + 1:]:
                pairs.append(make_pair(
                    f"p{pair_num}", s_a, s_b,
                    0.90 - pair_num * 0.001,
                ))
                pair_num += 1

        result = deduplicate_pairs(pairs)

        assert result.total_pairs == 45
        assert result.llm_call_count == 1
        assert result.reduction_count == 44
        assert result.reduction_pct == pytest.approx(97.78, rel=0.01)

    def test_mixed_independent_pairs_and_ring(self):
        """
        Ring: alice↔bob↔charlie (3 pairs → 1 call)
        Independent: dave↔eve (1 pair → 1 call)
        Total: 2 LLM calls
        """
        pairs = [
            make_pair("r1", "alice.cpp",   "bob.cpp",     0.91),
            make_pair("r2", "alice.cpp",   "charlie.cpp", 0.87),
            make_pair("r3", "bob.cpp",     "charlie.cpp", 0.89),
            make_pair("i1", "dave.cpp",    "eve.cpp",     0.75),
        ]
        result = deduplicate_pairs(pairs)

        assert result.llm_call_count == 2
        assert result.total_pairs == 4
        assert result.reduction_count == 2

        ring_rep = result.ring_map["r1"]
        assert result.ring_map["r2"] == ring_rep
        assert result.ring_map["r3"] == ring_rep
        assert result.ring_map["i1"] == "i1"

    def test_representative_selection_is_deterministic(self):
        """Same input always produces the same representative (stable sort)."""
        pairs = [
            make_pair("p1", "alice.cpp", "bob.cpp",     0.85),
            make_pair("p2", "alice.cpp", "charlie.cpp", 0.85),   # same score
        ]
        result1 = deduplicate_pairs(list(pairs))
        result2 = deduplicate_pairs(list(pairs))

        rep1 = result1.representative_pairs[0]["pairId"]
        rep2 = result2.representative_pairs[0]["pairId"]
        assert rep1 == rep2

    def test_higher_score_pair_always_wins_representative(self):
        """When scores differ, the highest-score pair must be the representative."""
        pairs = [
            make_pair("low",  "alice.cpp", "bob.cpp",     0.71),
            make_pair("high", "alice.cpp", "charlie.cpp", 0.95),
            make_pair("mid",  "bob.cpp",   "charlie.cpp", 0.83),
        ]
        result = deduplicate_pairs(pairs)

        assert result.llm_call_count == 1
        assert result.representative_pairs[0]["pairId"] == "high"
        assert result.ring_map["low"]  == "high"
        assert result.ring_map["high"] == "high"
        assert result.ring_map["mid"]  == "high"

    def test_ring_map_covers_all_input_pairs(self):
        """Every input pair_id must appear exactly once in ring_map."""
        pairs = [
            make_pair("p1", "a.cpp", "b.cpp", 0.9),
            make_pair("p2", "b.cpp", "c.cpp", 0.8),
            make_pair("p3", "d.cpp", "e.cpp", 0.7),
        ]
        result = deduplicate_pairs(pairs)

        all_pair_ids = {p["pairId"] for p in pairs}
        assert set(result.ring_map.keys()) == all_pair_ids


# ── TestBuildSyntheticReport ──────────────────────────────────────────────────

class TestBuildSyntheticReport:

    def test_synthetic_report_has_correct_structure(self):
        pair   = make_pair("p-non-rep", "frank.cpp", "grace.cpp", 0.80)
        report = build_synthetic_report(pair, "reports/job-1/p-rep.json")

        assert report["pairId"]                  == "p-non-rep"
        assert report["similarityScore"]         == 0.80
        assert report["verdict"]                 == "SEE_REPRESENTATIVE"
        assert report["isRepresentative"]        is False
        assert report["representativeReportKey"] == "reports/job-1/p-rep.json"
        assert "cheating ring" in report["analystNotes"].lower()
        assert report["obfuscationTechniques"]   == []
        assert report["evidence"]                == []

    def test_synthetic_report_has_schema_version(self):
        pair   = make_pair("p1", "a.cpp", "b.cpp", 0.7)
        report = build_synthetic_report(pair, "reports/job/rep.json")
        assert report["schemaVersion"] == 1

    def test_synthetic_report_has_generated_at(self):
        pair   = make_pair("p1", "a.cpp", "b.cpp", 0.7)
        report = build_synthetic_report(pair, "reports/job/rep.json")
        assert "generatedAt" in report
        assert "T" in report["generatedAt"]   # ISO 8601 timestamp

    def test_synthetic_report_zero_confidence(self):
        """Synthetic reports must have 0 confidence (no LLM call was made)."""
        pair   = make_pair("p1", "a.cpp", "b.cpp", 0.7)
        report = build_synthetic_report(pair, "rep.json")
        assert report["confidence"] == 0.0
        assert report["rawLlmResponse"] == ""
