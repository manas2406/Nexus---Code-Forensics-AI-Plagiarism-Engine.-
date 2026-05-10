"""End-to-end pipeline: source → tokens → fingerprints.

Wires :mod:`ast_engine`, :mod:`winnowing`, and :mod:`comparator` into a
single callable for use in Phase 1 worker integration.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass, field
from typing import Callable

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


@dataclass
class BatchResult:
    """Result of batch-processing multiple source files."""

    total_files: int
    processed: int
    skipped: list[str]                          # filenames skipped
    fingerprints: dict[str, set[int]]           # filename → fingerprint set
    parse_results: dict[str, ParseResult]       # filename → full ParseResult
    elapsed_seconds: float


def process_batch(
    files: dict[str, str],
    k: int = 5,
    w: int = 4,
    max_bytes: int = 500_000,
    on_progress: Callable[[str], None] | None = None,
) -> BatchResult:
    """Process a batch of C++ source files through the full pipeline.

    - Skips files whose byte length exceeds *max_bytes* (logs a warning).
    - Skips files with non-UTF-8 content (logs a warning).
    - Calls *on_progress(filename)* after each file if provided.
    - Never raises — all errors are caught and the file is added to *skipped*.

    Args:
        files: Mapping of filename → source code string.
        k: K-gram size for winnowing.
        w: Window size for winnowing.
        max_bytes: Maximum allowed byte length per file.
        on_progress: Optional callback invoked after each successfully processed file.

    Returns:
        A :class:`BatchResult` with aggregated results.
    """
    skipped: list[str] = []
    fingerprints: dict[str, set[int]] = {}
    parse_results: dict[str, ParseResult] = {}
    processed = 0

    start = time.perf_counter()

    for filename, source_code in files.items():
        try:
            # Check byte size
            raw_bytes = source_code.encode("utf-8")
            if len(raw_bytes) > max_bytes:
                print(
                    f"[pipeline] SKIP {filename}: exceeds {max_bytes} bytes",
                    file=sys.stderr,
                )
                skipped.append(filename)
                continue
        except (UnicodeEncodeError, UnicodeDecodeError, AttributeError) as exc:
            print(
                f"[pipeline] SKIP {filename}: encoding error — {exc}",
                file=sys.stderr,
            )
            skipped.append(filename)
            continue

        try:
            result = process_file(source_code, k=k, w=w)
            parse_results[filename] = result
            fingerprints[filename] = result.fingerprints
            processed += 1
            if on_progress is not None:
                on_progress(filename)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[pipeline] SKIP {filename}: processing error — {exc}",
                file=sys.stderr,
            )
            skipped.append(filename)

    elapsed = time.perf_counter() - start

    return BatchResult(
        total_files=len(files),
        processed=processed,
        skipped=skipped,
        fingerprints=fingerprints,
        parse_results=parse_results,
        elapsed_seconds=elapsed,
    )
