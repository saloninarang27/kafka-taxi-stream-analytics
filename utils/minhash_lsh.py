"""
utils/minhash_lsh.py – MinHash + LSH banding for Jaccard similarity.

Stage 3 uses this to find similar pickup zones based on their
destination (drop-off zone) sets.
"""

import hashlib
import itertools
import math
import struct
from collections import defaultdict


# ──────────────────────────────────────────────────────────────────────────────
# MinHash
# ──────────────────────────────────────────────────────────────────────────────

class MinHash:
    """
    Generates a MinHash signature of length `num_hashes` for a set of integers.

    Uses the universal hash family:
        h_i(x) = (a_i * x + b_i) mod PRIME  (mod MAX_HASH for signature value)
    """

    PRIME = (1 << 61) - 1        # Mersenne prime
    MAX_HASH = (1 << 32)

    def __init__(self, num_hashes: int = 100, seed: int = 42):
        self.num_hashes = num_hashes
        rng_state = seed
        self._a = []
        self._b = []
        for _ in range(num_hashes):
            rng_state = (rng_state * 1664525 + 1013904223) & 0xFFFFFFFF
            self._a.append(rng_state | 1)
            rng_state = (rng_state * 1664525 + 1013904223) & 0xFFFFFFFF
            self._b.append(rng_state)

    def signature(self, items: set) -> list:
        """Compute MinHash signature for a set of integer or string items."""
        sig = [self.MAX_HASH] * self.num_hashes
        for item in items:
            if isinstance(item, str):
                h = struct.unpack("<I", hashlib.md5(item.encode()).digest()[:4])[0]
            else:
                h = int(item)
            for i in range(self.num_hashes):
                v = (self._a[i] * h + self._b[i]) % self.PRIME % self.MAX_HASH
                if v < sig[i]:
                    sig[i] = v
        return sig


# ──────────────────────────────────────────────────────────────────────────────
# LSH banding
# ──────────────────────────────────────────────────────────────────────────────

class LSH:
    """
    Locality-Sensitive Hashing using the banding technique.

    Parameters
    ----------
    num_hashes    : int   Total signature length (must equal bands * rows_per_band)
    bands         : int   Number of bands
    rows_per_band : int   Rows per band
    """

    def __init__(self, num_hashes: int = 100, bands: int = 20, rows_per_band: int = 5):
        assert num_hashes == bands * rows_per_band, (
            f"num_hashes ({num_hashes}) must equal bands*rows ({bands*rows_per_band})"
        )
        self.num_hashes = num_hashes
        self.bands = bands
        self.rows_per_band = rows_per_band
        # bucket_tables[band] → dict of bucket_key → list of zone_ids
        self._tables: list[dict] = [defaultdict(list) for _ in range(bands)]

    def add(self, zone_id, signature: list) -> None:
        """Insert a zone with its MinHash signature into the LSH buckets."""
        for b in range(self.bands):
            start = b * self.rows_per_band
            band_sig = tuple(signature[start: start + self.rows_per_band])
            key = (b, band_sig)
            self._tables[b][band_sig].append(zone_id)

    def get_candidates(self) -> set:
        """Return all candidate pairs (zones sharing at least one bucket)."""
        candidates = set()
        for b in range(self.bands):
            for bucket in self._tables[b].values():
                if len(bucket) >= 2:
                    for pair in itertools.combinations(sorted(bucket), 2):
                        candidates.add(pair)
        return candidates

    def reset(self):
        self._tables = [defaultdict(list) for _ in range(self.bands)]


# ──────────────────────────────────────────────────────────────────────────────
# Jaccard utilities
# ──────────────────────────────────────────────────────────────────────────────

def jaccard(set_a: set, set_b: set) -> float:
    if not set_a and not set_b:
        return 1.0
    inter = len(set_a & set_b)
    union = len(set_a | set_b)
    return inter / union if union else 0.0


def all_pairs_jaccard(zone_sets: dict, threshold: float = 0.50) -> set:
    """
    Brute-force all-pairs Jaccard for evaluation only.
    Returns set of (zone_a, zone_b) pairs with Jaccard >= threshold.
    """
    zones = sorted(zone_sets.keys())
    true_pairs = set()
    for i, a in enumerate(zones):
        for b in zones[i + 1:]:
            if jaccard(zone_sets[a], zone_sets[b]) >= threshold:
                true_pairs.add((a, b))
    return true_pairs
