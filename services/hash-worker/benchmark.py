"""
Nexus Phase 1 Benchmark — N = 500 synthetic C++ files.

Usage: python benchmark.py [--n 500] [--threshold 0.6] [--num-perm 128]

Tradeoff notes:
  - num_perm=128: Good balance between accuracy and speed.  Increasing to
    256 improves recall by ~2% but doubles MinHash build time.
  - threshold=0.5 for LSH: Intentionally lower than the final comparison
    threshold (0.6) to ensure high recall — LSH is a pre-filter, not a
    final classifier.  False positives are cheap (filtered by exact Jaccard);
    false negatives are expensive (missed plagiarism).
  - k=5, w=4: Winnowing parameters from Phase 0 — tuned for C++ AST tokens.
"""

from __future__ import annotations

import argparse
import random
import string
import time

from pipeline import process_batch
from lsh_index import LSHIndex
from comparator import compare_candidates


def generate_cpp(seed: int, variant: int = 0) -> tuple[str, str]:
    """Generate a synthetic C++ file.

    Args:
        seed: Random seed for reproducibility.
        variant:
            0 = original
            1 = renamed variables (high similarity to variant 0)
            2 = completely different (low similarity to variant 0)

    Returns:
        Tuple of (filename, source_code).
    """
    rng = random.Random(seed)

    if variant == 0:
        vars_ = [
            "".join(rng.choices(string.ascii_lowercase, k=5)) for _ in range(5)
        ]
        limit = rng.randint(10, 100)
        init_val = rng.randint(0, 50)
        filename = f"orig_{seed:04d}.cpp"
        source = f"""\
int compute_{seed}(int n) {{
    int {vars_[0]} = {init_val};
    for (int {vars_[1]} = 0; {vars_[1]} < {limit}; {vars_[1]}++) {{
        {vars_[0]} += {vars_[1]} * {vars_[1]};
        if ({vars_[1]} % 2 == 0) {{
            {vars_[0]} -= {vars_[1]};
        }}
    }}
    int {vars_[2]} = {vars_[0]} + n;
    for (int {vars_[3]} = 0; {vars_[3]} < n; {vars_[3]}++) {{
        {vars_[2]} += {vars_[3]};
    }}
    return {vars_[2]};
}}
"""
        return filename, source

    elif variant == 1:
        # Same structure, different variable names
        vars_ = [
            "".join(rng.choices(string.ascii_lowercase, k=5)) for _ in range(5)
        ]
        limit = rng.randint(10, 100)
        init_val = rng.randint(0, 50)
        # Re-generate with different names but same structure
        new_vars = [f"var_{chr(ord('a') + i)}_{seed}" for i in range(5)]
        filename = f"clone_{seed:04d}.cpp"
        source = f"""\
int compute_{seed}(int n) {{
    int {new_vars[0]} = {init_val};
    for (int {new_vars[1]} = 0; {new_vars[1]} < {limit}; {new_vars[1]}++) {{
        {new_vars[0]} += {new_vars[1]} * {new_vars[1]};
        if ({new_vars[1]} % 2 == 0) {{
            {new_vars[0]} -= {new_vars[1]};
        }}
    }}
    int {new_vars[2]} = {new_vars[0]} + n;
    for (int {new_vars[3]} = 0; {new_vars[3]} < n; {new_vars[3]}++) {{
        {new_vars[2]} += {new_vars[3]};
    }}
    return {new_vars[2]};
}}
"""
        return filename, source

    else:
        # Generate structurally diverse files based on seed
        rng2 = random.Random(seed + 10_000)
        
        # A list of diverse structural snippets
        snippets = [
            "for (int i = 0; i < 10; i++) { int x = i * i; }",
            "int y = 0; while (y < 5) { y++; }",
            "if (true) { int z = 1; } else { int z = 2; }",
            "try { int a = 1; } catch (...) { int b = 2; }",
            "switch (1) { case 1: break; default: break; }",
            "struct Point { int x; int y; };",
            "enum Color { RED, GREEN, BLUE };",
            "template <typename T> T max(T a, T b) { return a > b ? a : b; }",
            "int* p = new int[10]; delete[] p;",
            "auto lambda = [](int x) { return x + 1; };"
        ]
        
        # Pick a random subset and shuffle
        k = rng2.randint(2, len(snippets))
        chosen = rng2.sample(snippets, k)
        
        filename = f"diff_{seed:04d}.cpp"
        source = f"""\
#include <iostream>

{chr(10).join(chosen)}

void run_{seed}() {{
    // extra function
}}
"""
        return filename, source


def main() -> None:
    parser = argparse.ArgumentParser(description="Nexus Phase 1 Benchmark")
    parser.add_argument("--n", type=int, default=500, help="Number of files")
    parser.add_argument("--threshold", type=float, default=0.6,
                        help="Exact Jaccard threshold")
    parser.add_argument("--num-perm", type=int, default=128,
                        help="MinHash permutations")
    args = parser.parse_args()

    n = args.n
    threshold = args.threshold
    num_perm = args.num_perm
    num_planted = 20

    print(f"Generating {n} synthetic C++ files ({num_planted} planted pairs)...")

    # 1. Generate N files with ~20 plagiarism pairs
    files: dict[str, str] = {}
    planted_pairs: set[frozenset[str]] = set()

    # First: generate planted pairs (variant 0 + variant 1 of same seed)
    for i in range(num_planted):
        orig_name, orig_code = generate_cpp(seed=i, variant=0)
        clone_name, clone_code = generate_cpp(seed=i, variant=1)
        files[orig_name] = orig_code
        files[clone_name] = clone_code
        planted_pairs.add(frozenset((orig_name, clone_name)))

    # Fill remaining with unique different files
    remaining = n - len(files)
    for i in range(remaining):
        seed = num_planted + i
        name, code = generate_cpp(seed=seed, variant=2)
        files[name] = code

    print(f"  Total files: {len(files)}")
    print(f"  Planted pairs: {len(planted_pairs)}")

    # 2. process_batch()
    print("\nRunning batch processing...")
    batch = process_batch(files)

    # 3. LSH pre-filter
    print("Running LSH pre-filter...")
    lsh = LSHIndex(threshold=threshold - 0.1, num_perm=num_perm)
    candidates = lsh.get_all_candidates(batch.fingerprints)

    # 4. Exact comparison
    print("Running exact Jaccard comparison...")
    suspicious = compare_candidates(candidates, batch.fingerprints,
                                    threshold=threshold)

    # 5. Count detected planted pairs
    suspicious_pairs = {
        frozenset((sp.file_a, sp.file_b)) for sp in suspicious
    }
    detected = sum(1 for pp in planted_pairs if pp in suspicious_pairs)

    total_possible_pairs = n * (n - 1) // 2
    avoided = total_possible_pairs - len(candidates)

    print(f"""
    === Nexus Phase 1 Benchmark ===
    Files processed     : {batch.processed}
    Files skipped       : {len(batch.skipped)}
    Batch time          : {batch.elapsed_seconds:.3f}s
    Avg per file        : {batch.elapsed_seconds / max(batch.processed, 1) * 1000:.1f}ms

    Candidate pairs     : {len(candidates)}   (after LSH pre-filter)
    Suspicious pairs    : {len(suspicious)}   (after exact Jaccard >= {threshold})
    Known planted pairs : {num_planted}
    Detected planted    : {detected}          (planted pairs found in suspicious)
    Recall              : {detected / max(num_planted, 1) * 100:.1f}%

    O(N²) comparisons avoided: {avoided} / {total_possible_pairs}
    """)


if __name__ == "__main__":
    main()
