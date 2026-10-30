"""
utils/hll.py – HyperLogLog sketch for counting distinct elements.

Uses the HyperLogLog++ algorithm (Flajolet et al. 2007 / Heule et al. 2013).
No external library required.
"""

import hashlib
import math
import struct


class HyperLogLog:
    """
    HyperLogLog cardinality estimator.

    Parameters
    ----------
    precision : int  b   Number of index bits (4 <= b <= 16).
                         Number of registers m = 2^b.
                         Standard error ≈ 1.04 / sqrt(m).
    """

    def __init__(self, precision: int = 12):
        if not 4 <= precision <= 16:
            raise ValueError("precision must be between 4 and 16")
        self.b = precision
        self.m = 1 << precision            # number of registers
        self._registers = bytearray(self.m)
        self._alpha = self._get_alpha(self.m)
        self._exact = set()                # kept only for evaluation baseline

    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _get_alpha(m: int) -> float:
        if m == 16:   return 0.673
        if m == 32:   return 0.697
        if m == 64:   return 0.709
        return 0.7213 / (1.0 + 1.079 / m)

    def _hash64(self, item: str) -> int:
        raw = hashlib.sha256(item.encode()).digest()
        return struct.unpack("<Q", raw[:8])[0]

    def add(self, item: str) -> None:
        self._exact.add(item)             # exact baseline (evaluation only)
        h = self._hash64(item)
        # top-b bits → register index
        idx = h >> (64 - self.b)
        # remaining bits → leading-zero count + 1
        w = h & ((1 << (64 - self.b)) - 1)
        leading_zeros = (64 - self.b) - w.bit_length() + 1
        self._registers[idx] = max(self._registers[idx], leading_zeros)

    def estimate(self) -> int:
        """Return HLL cardinality estimate."""
        m = self.m
        Z = sum(2.0 ** (-r) for r in self._registers)
        raw = self._alpha * m * m / Z

        # Small-range correction
        if raw <= 2.5 * m:
            V = self._registers.count(0)
            if V > 0:
                raw = m * math.log(m / V)

        # Large-range correction (not needed for typical sizes < 2^32/30)
        return int(raw)

    def exact_count(self) -> int:
        return len(self._exact)

    def error_pct(self) -> float:
        exact = self.exact_count()
        if exact == 0:
            return 0.0
        return abs(self.estimate() - exact) / exact * 100

    def memory_sketch_bytes(self) -> int:
        """Approximate memory used by the HLL registers."""
        return self.m  # 1 byte per register

    def memory_exact_bytes(self) -> int:
        """Approximate memory used by the exact set (rough estimate)."""
        return sum(len(s.encode()) + 50 for s in self._exact)

    def reset(self):
        self._registers = bytearray(self.m)
        self._exact.clear()
