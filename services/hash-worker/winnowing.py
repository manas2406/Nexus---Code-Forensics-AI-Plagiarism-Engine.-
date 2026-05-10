"""Winnowing algorithm for robust document fingerprinting.

Produces a position-independent set of integer fingerprints from a token
list, suitable for Jaccard similarity comparison.
"""

from __future__ import annotations


def _poly_hash(s: str, base: int = 31, mod: int = 2**61 - 1) -> int:
    """Deterministic polynomial rolling hash.

    Unlike Python's built-in ``hash()``, this function is **not**
    randomised across processes (``PYTHONHASHSEED`` independent).

    Args:
        s: The string to hash.
        base: Polynomial base.
        mod: Hash modulus (Mersenne prime for good distribution).

    Returns:
        A non-negative integer hash.
    """
    h = 0
    for ch in s:
        h = (h * base + ord(ch)) % mod
    return h


def get_fingerprints(tokens: list[str], k: int = 5, w: int = 4) -> set[int]:
    """Compute Winnowing fingerprints from a token list.

    Steps:
        1. Build k-grams (sliding window of size *k*, joined by ``|``).
        2. Hash each k-gram with :func:`_poly_hash`.
        3. Slide a window of size *w* over the hashes.  In each window,
           select the **minimum** hash; ties broken by **rightmost**
           occurrence.
        4. Deduplicate: only record a fingerprint when it is selected at
           a **new** position.

    Args:
        tokens: List of token strings (e.g. from :func:`ast_engine.extract_tokens`).
        k: K-gram size (number of tokens per gram).
        w: Winnowing window size.

    Returns:
        A set of integer fingerprints.  Empty if *tokens* is too short
        to form even one k-gram.
    """
    if not tokens or len(tokens) < k:
        return set()

    # Step 1: build k-grams
    kgrams: list[str] = []
    for i in range(len(tokens) - k + 1):
        kgrams.append("|".join(tokens[i : i + k]))

    # Step 2: hash each k-gram
    hashes: list[int] = [_poly_hash(gram) for gram in kgrams]

    if not hashes:
        return set()

    # Step 3 & 4: winnowing — slide window, pick min (rightmost on tie)
    fingerprints: set[int] = set()
    prev_selected_pos: int = -1

    for i in range(len(hashes) - w + 1):
        window = hashes[i : i + w]

        # Find minimum value, rightmost occurrence on tie
        min_val = window[0]
        min_pos = i  # absolute position
        for j in range(1, len(window)):
            if window[j] <= min_val:
                min_val = window[j]
                min_pos = i + j

        # Only add if this is a new selection position
        if min_pos != prev_selected_pos:
            fingerprints.add(min_val)
            prev_selected_pos = min_pos

    # If fewer k-grams than w, we still need to pick from what we have
    if len(hashes) < w:
        # Find minimum hash (rightmost on tie) across all available hashes
        min_val = hashes[0]
        for j in range(1, len(hashes)):
            if hashes[j] <= min_val:
                min_val = hashes[j]
        fingerprints.add(min_val)

    return fingerprints
