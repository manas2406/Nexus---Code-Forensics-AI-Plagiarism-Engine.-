"""Jaccard similarity over fingerprint sets.

Pure Python, no external dependencies.
"""

from __future__ import annotations


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
