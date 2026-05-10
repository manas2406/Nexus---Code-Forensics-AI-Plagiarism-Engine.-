"""Jaccard similarity over fingerprint sets.

Pure Python, no external dependencies.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass


def jaccard(fp_a: set[int], fp_b: set[int]) -> float:
    """Compute the Jaccard similarity coefficient between two fingerprint sets.

    .. math::
        J(A, B) = \\frac{|A \\cap B|}{|A \\cup B|}

    Args:
        fp_a: First set of integer fingerprints.
        fp_b: Second set of integer fingerprints.

    Returns:
        A float in ``[0.0, 1.0]``.  Returns ``0.0`` if both sets are empty.
    """
    if not fp_a and not fp_b:
        return 0.0

    intersection = len(fp_a & fp_b)
    union = len(fp_a | fp_b)

    return intersection / union


@dataclass
class SuspiciousPair:
    """A pair of files with similarity above the detection threshold."""

    file_a: str
    file_b: str
    similarity: float  # exact Jaccard
    pair_id: str       # deterministic: sha256(sorted(file_a, file_b))[:12]


def _make_pair_id(file_a: str, file_b: str) -> str:
    """Generate a deterministic pair ID from two filenames.

    Same two files always produce the same ID regardless of argument order.
    """
    return hashlib.sha256(
        "|".join(sorted([file_a, file_b])).encode()
    ).hexdigest()[:12]


def compare_candidates(
    candidates: list["CandidatePair"],  # noqa: F821 — forward ref to lsh_index
    fingerprint_map: dict[str, set[int]],
    threshold: float = 0.6,
) -> list[SuspiciousPair]:
    """Run exact Jaccard on each candidate pair.

    Args:
        candidates: Candidate pairs from LSH pre-filtering.
        fingerprint_map: Mapping of filename → fingerprint set.
        threshold: Minimum exact Jaccard to include in results.

    Returns:
        Only pairs where exact Jaccard >= *threshold*.
    """
    suspicious: list[SuspiciousPair] = []

    for candidate in candidates:
        fp_a = fingerprint_map.get(candidate.file_a, set())
        fp_b = fingerprint_map.get(candidate.file_b, set())
        sim = jaccard(fp_a, fp_b)

        if sim >= threshold:
            suspicious.append(SuspiciousPair(
                file_a=candidate.file_a,
                file_b=candidate.file_b,
                similarity=sim,
                pair_id=_make_pair_id(candidate.file_a, candidate.file_b),
            ))

    return suspicious

