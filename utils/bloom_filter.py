"""
utils/bloom_filter.py – Bloom Filter using bitarray + multiple hash functions.
Reports false-positive and false-negative rates against ground truth.
"""

import math
import hashlib
import struct


class BloomFilter:
    """
    Space-efficient probabilistic set membership test.

    Parameters
    ----------
    expected_items : int   n  (expected number of distinct insertions)
    fp_rate        : float p  (target false-positive rate, e.g. 0.01)
    """

    def __init__(self, expected_items: int = 200_000, fp_rate: float = 0.01):
        self.n = expected_items
        self.p = fp_rate

        # Optimal bit-array size  m = -n * ln(p) / ln(2)^2
        self.m = max(1, int(-expected_items * math.log(fp_rate) / (math.log(2) ** 2)))
        # Optimal hash count      k = (m/n) * ln(2)
        self.k = max(1, int((self.m / expected_items) * math.log(2)))

        self._bits = bytearray((self.m + 7) // 8)  # bit array
        self._count = 0  # items inserted

        # Stats for evaluation
        self.fp_events = 0   # false positives seen
        self.fn_events = 0   # false negatives (always 0 for Bloom)
        self.tp_events = 0   # true positives (real duplicates caught)
        self.tn_events = 0   # true negatives (correctly identified as new)
        self.total_queries = 0

    # ──────────────────────────────────────────────────────────────────────────

    def _hash_positions(self, item: str):
        """Generate k independent bit positions for `item`."""
        positions = []
        for i in range(self.k):
            raw = hashlib.md5(f"{i}:{item}".encode()).digest()
            val = struct.unpack("<Q", raw[:8])[0]
            positions.append(val % self.m)
        return positions

    def add(self, item: str) -> None:
        for pos in self._hash_positions(item):
            self._bits[pos >> 3] |= (1 << (pos & 7))
        self._count += 1

    def __contains__(self, item: str) -> bool:
        for pos in self._hash_positions(item):
            if not (self._bits[pos >> 3] & (1 << (pos & 7))):
                return False
        return True

    def test_and_add(self, item: str, is_real_duplicate: bool):
        """
        Check membership, record statistics against ground truth, then insert.
        Returns True if the filter says 'seen before'.
        """
        seen = item in self
        self.total_queries += 1

        if seen and is_real_duplicate:
            self.tp_events += 1
        elif seen and not is_real_duplicate:
            self.fp_events += 1       # false positive
        elif not seen and is_real_duplicate:
            self.fn_events += 1       # false negative (impossible in theory)
        else:
            self.tn_events += 1

        if not is_real_duplicate:
            self.add(item)            # only add genuine first occurrences
        return seen

    # ──────────────────────────────────────────────────────────────────────────

    @property
    def fp_rate_observed(self):
        denom = self.fp_events + self.tn_events
        return self.fp_events / denom if denom else 0.0

    @property
    def fn_rate_observed(self):
        denom = self.fn_events + self.tp_events
        return self.fn_events / denom if denom else 0.0

    def info(self) -> dict:
        return {
            "inserted_ids": self._count,
            "bit_array_size_bits": self.m,
            "num_hash_functions": self.k,
            "fp_rate_theoretical": self.p,
            "fp_rate_observed": round(self.fp_rate_observed, 6),
            "fn_rate_observed": round(self.fn_rate_observed, 6),
            "total_queries": self.total_queries,
        }
