"""MinHash + LSH pre-filtering for candidate pair detection.

Uses the `datasketch` library to build a Locality-Sensitive Hashing index
that reduces pairwise comparisons from O(N²) to near-linear.  Candidate
pairs are then verified with exact Jaccard in :mod:`comparator`.
"""

from __future__ import annotations

from dataclasses import dataclass

from datasketch import MinHash, MinHashLSH

from comparator import jaccard


@dataclass
class CandidatePair:
    """A candidate pair identified by MinHash + LSH pre-filtering."""

    file_a: str
    file_b: str
    estimated_similarity: float  # exact Jaccard on fingerprint sets


class LSHIndex:
    """MinHash + LSH index for fast approximate nearest-neighbour search."""

    def __init__(self, threshold: float = 0.5, num_perm: int = 128) -> None:
        """Initialise the LSH index.

        Args:
            threshold: Minimum Jaccard similarity for a pair to be a candidate.
            num_perm: Number of MinHash permutations (higher = more accurate, slower).
        """
        self._threshold = threshold
        self._num_perm = num_perm
        self._lsh = MinHashLSH(threshold=threshold, num_perm=num_perm)
        self._minhashes: dict[str, MinHash] = {}
        self._fingerprints: dict[str, set[int]] = {}

    def _build_minhash(self, fingerprints: set[int]) -> MinHash:
        """Build a MinHash signature from a set of integer fingerprints."""
        mh = MinHash(num_perm=self._num_perm)
        for fp in fingerprints:
            mh.update(fp.to_bytes(8, "big"))
        return mh

    def add(self, filename: str, fingerprints: set[int]) -> None:
        """Add a file's fingerprints to the index.

        Args:
            filename: Unique identifier for the file.
            fingerprints: Set of integer fingerprints from winnowing.
        """
        mh = self._build_minhash(fingerprints)
        self._minhashes[filename] = mh
        self._fingerprints[filename] = fingerprints
        self._lsh.insert(filename, mh)

    def query(self, filename: str, fingerprints: set[int]) -> list[CandidatePair]:
        """Return candidate pairs for a file above the similarity threshold.

        Args:
            filename: Identifier for the query file.
            fingerprints: Fingerprint set of the query file.

        Returns:
            List of :class:`CandidatePair` objects for this file.
        """
        mh = self._build_minhash(fingerprints)
        results = self._lsh.query(mh)
        pairs: list[CandidatePair] = []
        for match in results:
            if match == filename:
                continue
            if match in self._fingerprints:
                sim = jaccard(fingerprints, self._fingerprints[match])
            else:
                sim = 0.0
            pairs.append(CandidatePair(
                file_a=filename,
                file_b=match,
                estimated_similarity=sim,
            ))
        return pairs

    def get_all_candidates(
        self,
        fingerprint_map: dict[str, set[int]],
    ) -> list[CandidatePair]:
        """Build the full index and return all candidate pairs.

        Inserts all files, then queries each to find candidate pairs.
        Deduplicates — (A, B) and (B, A) are the same pair.

        Args:
            fingerprint_map: Mapping of filename → fingerprint set.

        Returns:
            Deduplicated list of :class:`CandidatePair` objects.
        """
        # Re-initialise index to avoid duplicates from prior add() calls
        self._lsh = MinHashLSH(threshold=self._threshold, num_perm=self._num_perm)
        self._minhashes.clear()
        self._fingerprints.clear()

        # Insert all files
        for filename, fps in fingerprint_map.items():
            self.add(filename, fps)

        # Query each file and deduplicate pairs
        seen: set[frozenset[str]] = set()
        candidates: list[CandidatePair] = []

        for filename, fps in fingerprint_map.items():
            mh = self._minhashes[filename]
            results = self._lsh.query(mh)
            for match in results:
                if match == filename:
                    continue
                pair_key = frozenset((filename, match))
                if pair_key in seen:
                    continue
                seen.add(pair_key)
                sim = jaccard(fps, fingerprint_map[match])
                candidates.append(CandidatePair(
                    file_a=filename,
                    file_b=match,
                    estimated_similarity=sim,
                ))

        return candidates
