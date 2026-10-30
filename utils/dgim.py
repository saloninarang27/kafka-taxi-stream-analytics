"""
utils/dgim.py  – Datar-Gionis-Indyk-Motwani (DGIM) algorithm.

Maintains a summary of a binary stream to answer: "how many 1s in the
last k bits?" with at most 50% relative error.

Implementation follows the textbook (Mining of Massive Datasets, Ch. 4).
"""

import math
from collections import deque


class Bucket:
    """A DGIM bucket: a power-of-two count ending at a specific timestamp."""
    __slots__ = ("size", "timestamp")

    def __init__(self, size: int, timestamp: int):
        self.size = size
        self.timestamp = timestamp

    def __repr__(self):
        return f"Bucket(size={self.size}, ts={self.timestamp})"


class DGIM:
    """
    DGIM summary for the most recent `max_window` bits.

    Parameters
    ----------
    max_window : int
        Maximum window size N.  We discard buckets whose timestamp is
        older than max_window positions ago.
    """

    def __init__(self, max_window: int = 10_000):
        self.N = max_window
        self._current_ts: int = 0       # monotonically increasing bit counter
        # One deque per power-of-two size.  index i → size 2**i.
        # Each deque holds at most 2 Buckets.
        self._buckets: list[deque] = []  # indexed by log2(size)

    # ──────────────────────────────────────────────────────────────────────────
    # Public interface
    # ──────────────────────────────────────────────────────────────────────────

    def add(self, bit: int) -> None:
        """Process one bit (0 or 1)."""
        self._current_ts += 1
        self._expire()

        if bit == 1:
            # Create a new bucket of size 1
            b = Bucket(size=1, timestamp=self._current_ts)
            self._merge_bucket(b, level=0)

    def query(self, k: int) -> int:
        """
        Estimate the number of 1s in the most recent k bits.
        Returns an integer estimate.
        """
        if k > self.N:
            k = self.N

        threshold = self._current_ts - k  # bits with ts <= threshold are outside window

        count = 0
        last_size = None

        for level in range(len(self._buckets)):
            for bucket in self._buckets[level]:
                if bucket.timestamp > threshold:
                    count += bucket.size
                    last_size = bucket.size
                else:
                    # This bucket straddles the boundary – add half
                    pass  # handled after the loop (only the oldest bucket in window)

        # DGIM approximation: the oldest bucket that falls partially inside
        # the window contributes only half of its size.
        # We already counted it fully above; subtract half now.
        if last_size is not None:
            # Find the oldest bucket still in window
            oldest_ts = None
            oldest_size = None
            for level in range(len(self._buckets)):
                for bucket in self._buckets[level]:
                    if bucket.timestamp > threshold:
                        if oldest_ts is None or bucket.timestamp < oldest_ts:
                            oldest_ts = bucket.timestamp
                            oldest_size = bucket.size
            if oldest_size is not None:
                count = count - oldest_size + oldest_size // 2

        return max(0, count)

    def bucket_count(self) -> int:
        """Total number of DGIM buckets currently stored."""
        return sum(len(dq) for dq in self._buckets)

    def query_exact(self, bit_stream_recent: list) -> int:
        """
        Exact answer for evaluation purposes.
        bit_stream_recent should be the last-k elements of the actual stream.
        """
        return sum(bit_stream_recent)

    # ──────────────────────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _expire(self):
        """Remove buckets older than max_window."""
        threshold = self._current_ts - self.N
        for dq in self._buckets:
            while dq and dq[-1].timestamp <= threshold:
                dq.pop()

    def _merge_bucket(self, bucket: Bucket, level: int):
        """
        Insert `bucket` at `level`.  If there are now 3 buckets at this
        level, merge the two oldest into one bucket at the next level.
        """
        # Grow the list if needed
        while len(self._buckets) <= level:
            self._buckets.append(deque())

        self._buckets[level].appendleft(bucket)   # newest first

        if len(self._buckets[level]) >= 3:
            # Merge the two oldest (rightmost)
            old1 = self._buckets[level].pop()
            old2 = self._buckets[level].pop()
            merged = Bucket(size=old1.size + old2.size,
                            timestamp=max(old1.timestamp, old2.timestamp))
            self._merge_bucket(merged, level + 1)
