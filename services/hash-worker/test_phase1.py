"""Phase 1 test suite for batch processing, LSH indexing, and candidate comparison.

Covers:
- Batch processing (valid, oversized, non-UTF-8, never-raises)
- LSH index (similar pairs, no false negatives on clones)
- Candidate comparison (threshold filtering, deterministic pair IDs)

All Phase 0 tests in test_algo.py are unmodified and must still pass.
"""

from __future__ import annotations

import pytest

from ast_engine import extract_tokens
from winnowing import get_fingerprints
from pipeline import process_file, process_batch, BatchResult
from lsh_index import LSHIndex, CandidatePair
from comparator import jaccard, compare_candidates, SuspiciousPair, _make_pair_id


# ─── Test data ────────────────────────────────────────────────────────

VALID_CPP_TEMPLATE = """\
int func_{n}() {{
    int x_{n} = {n};
    for (int i = 0; i < {n}; i++) {{ x_{n} += i; }}
    return x_{n};
}}
"""

# Base file and its "plagiarised" variant (renamed variables)
CLONE_BASE = """\
int solve(int n) {
    int result = 0;
    for (int i = 0; i < n; i++) {
        result += i * i;
    }
    return result;
}
"""

CLONE_RENAMED = """\
int solve(int count) {
    int total = 0;
    for (int idx = 0; idx < count; idx++) {
        total += idx * idx;
    }
    return total;
}
"""

COMPLETELY_DIFFERENT = """\
#include <stdio.h>
int main() {
    printf("Hello, world!\\n");
    return 0;
}
"""


def _make_valid_files(n: int) -> dict[str, str]:
    """Generate n valid C++ source files."""
    return {
        f"file_{i}.cpp": VALID_CPP_TEMPLATE.format(n=i)
        for i in range(n)
    }


# ─── Batch processing tests ──────────────────────────────────────────

class TestBatchProcessesAllValidFiles:
    """Feed 10 valid C++ strings, assert processed == 10, skipped == []."""

    def test_batch_processes_all_valid_files(self) -> None:
        files = _make_valid_files(10)
        result = process_batch(files)
        assert result.processed == 10, (
            f"Expected 10 processed, got {result.processed}"
        )
        assert result.skipped == [], (
            f"Expected no skipped files, got {result.skipped}"
        )
        assert result.total_files == 10
        assert len(result.fingerprints) == 10
        assert len(result.parse_results) == 10


class TestBatchSkipsOversizedFile:
    """Include one file whose len() exceeds max_bytes, assert skipped."""

    def test_batch_skips_oversized_file(self) -> None:
        files = _make_valid_files(9)
        # Add an oversized file (exceeds default 500_000 bytes)
        files["huge.cpp"] = "int x = 0;\n" * 100_000  # ~1.1MB
        result = process_batch(files, max_bytes=500_000)
        assert "huge.cpp" in result.skipped, (
            "Oversized file should be in skipped list"
        )
        assert result.processed == 9
        assert result.total_files == 10


class TestBatchSkipsNonUtf8:
    """Pass a string with surrogate characters that fail encode('utf-8')."""

    def test_batch_skips_non_utf8(self) -> None:
        files = _make_valid_files(9)
        # A string containing a lone surrogate — cannot be encoded to UTF-8
        files["bad_encoding.cpp"] = "int x = \ud800;"
        result = process_batch(files)
        assert "bad_encoding.cpp" in result.skipped, (
            "Non-UTF-8 file should be in skipped list"
        )
        assert result.processed == 9


class TestBatchNeverRaises:
    """Pass valid, oversized, and broken files — assert no exception."""

    def test_batch_never_raises(self) -> None:
        files = _make_valid_files(5)
        files["huge.cpp"] = "x" * 600_000           # oversized
        files["bad.cpp"] = "int main( { return"     # syntax error (still parseable)
        files["bad_enc.cpp"] = "int x = \ud800;"    # encoding error

        result = process_batch(files, max_bytes=500_000)
        assert result.processed + len(result.skipped) == result.total_files, (
            "processed + skipped must equal total_files"
        )
        # No exception was raised — test passes by reaching this line


# ─── LSH index tests ─────────────────────────────────────────────────

class TestLSHFindsSimilarPairs:
    """Build LSH index from 5 file pairs with similarity >= 0.7."""

    def test_lsh_finds_similar_pairs(self) -> None:
        # Create 5 pairs of similar files (same base, renamed vars)
        fingerprint_map: dict[str, set[int]] = {}
        expected_pairs: set[frozenset[str]] = set()

        for i in range(5):
            base_name = f"base_{i}.cpp"
            clone_name = f"clone_{i}.cpp"

            base_code = CLONE_BASE.replace("solve", f"solve_{i}")
            clone_code = CLONE_RENAMED.replace("solve", f"solve_{i}")

            fp_base = get_fingerprints(extract_tokens(base_code))
            fp_clone = get_fingerprints(extract_tokens(clone_code))

            # Verify similarity is actually high
            sim = jaccard(fp_base, fp_clone)
            assert sim >= 0.7, (
                f"Pair {i} similarity {sim:.4f} should be >= 0.7"
            )

            fingerprint_map[base_name] = fp_base
            fingerprint_map[clone_name] = fp_clone
            expected_pairs.add(frozenset((base_name, clone_name)))

        # Also add some unrelated files so LSH has noise
        for i in range(5):
            noise_name = f"noise_{i}.cpp"
            noise_code = COMPLETELY_DIFFERENT.replace("Hello", f"Hello_{i}")
            fingerprint_map[noise_name] = get_fingerprints(
                extract_tokens(noise_code)
            )

        lsh = LSHIndex(threshold=0.5, num_perm=128)
        candidates = lsh.get_all_candidates(fingerprint_map)

        found_pairs = {
            frozenset((c.file_a, c.file_b)) for c in candidates
        }

        for pair in expected_pairs:
            assert pair in found_pairs, (
                f"Expected similar pair {pair} not found in LSH candidates"
            )


class TestLSHNoFalseNegativesOnClones:
    """Two identical files must always appear as a candidate pair."""

    def test_lsh_no_false_negatives_on_clones(self) -> None:
        fp = get_fingerprints(extract_tokens(CLONE_BASE))
        fingerprint_map = {
            "original.cpp": fp,
            "clone.cpp": fp,  # identical fingerprints
        }

        lsh = LSHIndex(threshold=0.9, num_perm=128)
        candidates = lsh.get_all_candidates(fingerprint_map)

        pair_keys = {frozenset((c.file_a, c.file_b)) for c in candidates}
        expected = frozenset(("original.cpp", "clone.cpp"))
        assert expected in pair_keys, (
            "Identical files must always appear as a candidate pair"
        )
        # Verify the estimated similarity is 1.0
        for c in candidates:
            if frozenset((c.file_a, c.file_b)) == expected:
                assert c.estimated_similarity == 1.0


# ─── Comparator tests ────────────────────────────────────────────────

class TestCompareCandidatesFiltersThreshold:
    """Feed 10 candidate pairs with known similarities, threshold = 0.6."""

    def test_compare_candidates_filters_threshold(self) -> None:
        # Build fingerprint sets with known Jaccard similarities
        fingerprint_map: dict[str, set[int]] = {}
        candidates: list[CandidatePair] = []

        for i in range(10):
            file_a = f"a_{i}.cpp"
            file_b = f"b_{i}.cpp"

            # Create sets where Jaccard = overlap / (2*size - overlap)
            # For pairs 0-4: high similarity (>= 0.6)
            # For pairs 5-9: low similarity (< 0.6)
            if i < 5:
                # High overlap: 8 shared out of 10 = Jaccard 0.8
                base = set(range(100 * i, 100 * i + 10))
                fp_a = base
                fp_b = set(list(base)[:8]) | {100 * i + 100, 100 * i + 101}
            else:
                # Low overlap: 1 shared out of 10+9 = Jaccard ~0.053
                fp_a = set(range(100 * i, 100 * i + 10))
                fp_b = {100 * i} | set(range(100 * i + 20, 100 * i + 29))

            fingerprint_map[file_a] = fp_a
            fingerprint_map[file_b] = fp_b
            candidates.append(CandidatePair(
                file_a=file_a,
                file_b=file_b,
                estimated_similarity=0.0,  # doesn't matter here
            ))

        suspicious = compare_candidates(
            candidates, fingerprint_map, threshold=0.6
        )

        assert len(suspicious) == 5, (
            f"Expected 5 suspicious pairs above threshold, got {len(suspicious)}"
        )
        for sp in suspicious:
            assert sp.similarity >= 0.6


class TestPairIdIsDeterministic:
    """pair_id must be the same regardless of argument order."""

    def test_pair_id_is_deterministic(self) -> None:
        id_ab = _make_pair_id("x.cpp", "y.cpp")
        id_ba = _make_pair_id("y.cpp", "x.cpp")
        assert id_ab == id_ba, (
            f"pair_id should be order-independent: {id_ab} != {id_ba}"
        )
        # Also verify length
        assert len(id_ab) == 12
