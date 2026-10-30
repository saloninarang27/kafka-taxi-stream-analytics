"""
utils/reservoir.py – Reservoir sampling with exact-mean tracking.

Supports multiple reservoir sizes in a single pass.
"""

import random
import math


class ReservoirSampler:
    """
    Classic Algorithm R reservoir sampler.

    Parameters
    ----------
    k : int   Reservoir size.
    """

    def __init__(self, k: int):
        self.k = k
        self.reservoir: list = []
        self._n_seen = 0

        # Exact streaming stats (for evaluation)
        self._exact_sum_fare = 0.0
        self._exact_sum_distance = 0.0
        self._exact_count = 0

    def add(self, fare: float, distance: float):
        self._n_seen += 1
        self._exact_sum_fare += fare
        self._exact_sum_distance += distance
        self._exact_count += 1

        item = {"fare_amount": fare, "trip_distance": distance}
        if len(self.reservoir) < self.k:
            self.reservoir.append(item)
        else:
            j = random.randint(0, self._n_seen - 1)
            if j < self.k:
                self.reservoir[j] = item

    # ──────────────────────────────────────────────────────────────────────────

    def sample_mean_fare(self) -> float:
        if not self.reservoir:
            return 0.0
        return sum(r["fare_amount"] for r in self.reservoir) / len(self.reservoir)

    def sample_mean_distance(self) -> float:
        if not self.reservoir:
            return 0.0
        return sum(r["trip_distance"] for r in self.reservoir) / len(self.reservoir)

    def exact_mean_fare(self) -> float:
        return self._exact_sum_fare / self._exact_count if self._exact_count else 0.0

    def exact_mean_distance(self) -> float:
        return self._exact_sum_distance / self._exact_count if self._exact_count else 0.0

    def stats(self) -> dict:
        exact_f = self.exact_mean_fare()
        sample_f = self.sample_mean_fare()
        exact_d = self.exact_mean_distance()
        sample_d = self.sample_mean_distance()
        return {
            "k": self.k,
            "n_seen": self._n_seen,
            "reservoir_size": len(self.reservoir),
            "exact_mean_fare": round(exact_f, 4),
            "sample_mean_fare": round(sample_f, 4),
            "fare_error_pct": round(abs(sample_f - exact_f) / exact_f * 100, 4) if exact_f else 0,
            "exact_mean_distance": round(exact_d, 4),
            "sample_mean_distance": round(sample_d, 4),
            "distance_error_pct": round(abs(sample_d - exact_d) / exact_d * 100, 4) if exact_d else 0,
        }
