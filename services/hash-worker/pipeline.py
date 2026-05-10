"""End-to-end pipeline: source → tokens → fingerprints.

Wires :mod:`ast_engine`, :mod:`winnowing`, and :mod:`comparator` into a
single callable for use in Phase 1 worker integration.
"""

from __future__ import annotations

from dataclasses import dataclass

from ast_engine import extract_tokens_with_errors
from winnowing import get_fingerprints


@dataclass
class ParseResult:
    """Result of processing a single source file through the full pipeline."""

    tokens: list[str]
    fingerprints: set[int]
    token_count: int
    fingerprint_count: int
    had_errors: bool  # True if any ERROR nodes were encountered


def process_file(source_code: str, k: int = 5, w: int = 4) -> ParseResult:
    """Full pipeline: source → tokens → fingerprints.

    Args:
        source_code: Raw C++ source code as a string.
        k: K-gram size for winnowing.
        w: Window size for winnowing.

    Returns:
        A :class:`ParseResult` containing tokens, fingerprints, counts,
        and whether any parse errors were encountered.
    """
    tokens, had_errors = extract_tokens_with_errors(source_code)
    fingerprints = get_fingerprints(tokens, k=k, w=w)

    return ParseResult(
        tokens=tokens,
        fingerprints=fingerprints,
        token_count=len(tokens),
        fingerprint_count=len(fingerprints),
        had_errors=had_errors,
    )
