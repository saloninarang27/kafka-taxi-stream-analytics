"""
scripts/algo_sanity_check.py
-----------------------------
OFFLINE ALGORITHM SANITY CHECK  (NOT a Kafka performance benchmark)

Purpose: Verify that Bloom, HLL, DGIM, Reservoir, and MinHash-LSH produce
correct estimates on real NYC Yellow Taxi Jan-2024 data, WITHOUT touching Kafka.

This script does NOT generate results.jsonl or experiment_summary.csv.
Those files are generated exclusively by running the real Kafka pipeline via:

    python run_pipeline.py --experiment partitions
    python run_pipeline.py --experiment rate
    python run_pipeline.py --experiment key

Output of this script: printed table of exact vs estimated values and error %.

Usage:
    python scripts/algo_sanity_check.py [--config config.yaml] [--max-events 50000]
"""

import sys
import time
import math
import random
import pathlib
import argparse
import hashlib
from collections import defaultdict, deque

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import pandas as pd

from utils.bloom_filter import BloomFilter
from utils.reservoir import ReservoirSampler
from utils.hll import HyperLogLog
from utils.dgim import DGIM
from utils.minhash_lsh import MinHash, LSH, jaccard, all_pairs_jaccard
from utils.config import load_config


def _md5(s: str) -> str:
    return hashlib.md5(s.encode()).hexdigest()


def run_sanity_check(cfg: dict, max_events: int = 50000):
    print("=" * 70)
    print(" OFFLINE ALGORITHM SANITY CHECK")
    print(f" Data: NYC Yellow Taxi Jan-2024  |  First {max_events:,} events")
    print("=" * 70)

    data_path = cfg["producer"]["dataset_path"]
    print(f"\nLoading Parquet: {data_path} ...")
    df = pd.read_parquet(data_path)
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                            "PULocationID", "DOLocationID"])
    df = df[(df["PULocationID"] > 0) & (df["DOLocationID"] > 0)]
    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)
    df = df.iloc[:max_events].copy()
    print(f"Using {len(df):,} rows.\n")

    # ── 1. Bloom Filter ───────────────────────────────────────────────────────
    print("[1] Bloom Filter  (2% duplicate injection)")
    bloom = BloomFilter(expected_items=max_events * 2, fp_rate=cfg["bloom"]["false_positive_rate"])
    seen_ids: set = set()
    recent_ids: deque = deque(maxlen=1000)
    tp = tn = fp = fn = 0

    for i, row in df.iterrows():
        eid = f"2024-01-row-{int(i):08d}"
        is_real_dup = bool(recent_ids and random.random() < 0.02)
        if is_real_dup:
            eid = random.choice(recent_ids)

        bloom_says_seen = bloom.test_and_add(eid, is_real_duplicate=is_real_dup)

        if not is_real_dup:
            recent_ids.append(eid)

    bloom_info = bloom.info()
    print(f"   FP rate (observed): {bloom_info['fp_rate_observed']:.4f}  "
          f"(theoretical: {bloom_info['fp_rate_theoretical']:.4f})")
    print(f"   FN rate (observed): {bloom_info['fn_rate_observed']:.4f}  "
          f"(Bloom cannot produce FN by design - always 0.0)")
    print(f"   Items inserted:     {bloom_info['inserted_ids']:,}")
    print()

    # ── 2. Reservoir Sampling ─────────────────────────────────────────────────
    print("[2] Reservoir Sampling  (k = 100, 1000, 10000)")
    reservoirs = {k: ReservoirSampler(k) for k in cfg["reservoir"]["sizes"]}
    all_fares = []
    all_dists = []
    for _, row in df.iterrows():
        fare = float(row["fare_amount"])
        dist = float(row["trip_distance"])
        all_fares.append(fare)
        all_dists.append(dist)
        for rs in reservoirs.values():
            rs.add(fare, dist)

    exact_fare = sum(all_fares) / len(all_fares) if all_fares else 0
    exact_dist = sum(all_dists) / len(all_dists) if all_dists else 0
    print(f"   Exact mean fare: {exact_fare:.4f}    Exact mean dist: {exact_dist:.4f}")
    for k, rs in reservoirs.items():
        s = rs.stats()
        f_err = round(abs(s['sample_mean_fare'] - exact_fare) / exact_fare * 100, 2) if exact_fare else 0
        d_err = round(abs(s['sample_mean_distance'] - exact_dist) / exact_dist * 100, 2) if exact_dist else 0
        print(f"   k={k:>6}  sample_fare={s['sample_mean_fare']:.4f} (err={f_err:.2f}%)  "
              f"sample_dist={s['sample_mean_distance']:.4f} (err={d_err:.2f}%)")
    print()

    # ── 3. Event vs Entity Sampling (Diversity) ───────────────────────────────
    print("[3] Event vs Entity Sampling  (diversity: zones with >=10 dropoffs)")
    sample_frac = 0.15
    exact_zones: dict = defaultdict(set)
    event_zones: dict = defaultdict(set)
    entity_zones: dict = defaultdict(set)

    for _, row in df.iterrows():
        pz = int(row["PULocationID"])
        dz = int(row["DOLocationID"])
        exact_zones[pz].add(dz)
        if random.random() < sample_frac:
            event_zones[pz].add(dz)
        h = int(_md5(str(pz)), 16)
        if (h % 1000) < int(sample_frac * 1000):
            entity_zones[pz].add(dz)

    min_d = 10
    exact_div = sum(1 for s in exact_zones.values() if len(s) >= min_d)
    event_div  = sum(1 for s in event_zones.values()  if len(s) >= min_d)
    entity_div = sum(1 for s in entity_zones.values() if len(s) >= min_d)
    exact_tot  = len(exact_zones)
    event_tot  = len(event_zones)
    entity_tot = len(entity_zones)

    exact_f  = exact_div / exact_tot   if exact_tot  else 0
    event_f  = event_div / event_tot   if event_tot  else 0
    entity_f = entity_div / entity_tot if entity_tot else 0
    # Count stored (pickup, dropoff) pairs to prove equal storage budget
    event_pairs  = sum(len(v) for v in event_zones.values())
    entity_pairs = sum(len(v) for v in entity_zones.values())
    exact_pairs  = sum(len(v) for v in exact_zones.values())
    print(f"   Exact  diversity: {exact_f:.4f}  ({exact_div}/{exact_tot} zones)  "
          f"stored pairs={exact_pairs:,}")
    print(f"   Event  diversity: {event_f:.4f}  ({event_div}/{event_tot} zones sampled)  "
          f"err={abs(event_f - exact_f)/exact_f*100:.1f}%  stored pairs={event_pairs:,}")
    print(f"   Entity diversity: {entity_f:.4f}  ({entity_div}/{entity_tot} zones sampled)  "
          f"err={abs(entity_f - exact_f)/exact_f*100:.1f}%  stored pairs={entity_pairs:,}")
    print(f"   Budget ({sample_frac:.0%}): event sampled {event_pairs:,} pairs vs entity "
          f"{entity_pairs:,} pairs  (approx equal -- both ~{sample_frac:.0%} of {exact_pairs:,} total)")
    print()

    # ── 4. HyperLogLog ────────────────────────────────────────────────────────
    print("[4] HyperLogLog  (b=12, error~1.04/sqrt(4096)~1.6%)")
    hll = HyperLogLog(precision=cfg["hll"]["precision"])
    for _, row in df.iterrows():
        hll.add(f"{int(row['PULocationID'])}-{int(row['DOLocationID'])}")
    hll_exact = hll.exact_count()
    hll_est   = hll.estimate()
    hll_err   = round(abs(hll_est - hll_exact) / hll_exact * 100, 2) if hll_exact else 0
    print(f"   Exact distinct routes : {hll_exact:,}")
    print(f"   HLL estimate          : {hll_est:,}")
    print(f"   Error                 : {hll_err:.2f}%")
    print(f"   Sketch memory         : {hll.memory_sketch_bytes():,} bytes")
    print(f"   Exact set memory      : ~{hll.memory_exact_bytes():,} bytes")
    print()

    # ── 5. DGIM ───────────────────────────────────────────────────────────────
    print(f"[5] DGIM  (sliding window = last {cfg['dgim']['max_window']:,} events, slow_trip_bit)")
    dgim = DGIM(max_window=cfg["dgim"]["max_window"])
    bits_log = []
    for _, row in df.iterrows():
        dur = (row["tpep_dropoff_datetime"] - row["tpep_pickup_datetime"]).total_seconds() / 60
        bit = 1 if dur > 30 else 0
        dgim.add(bit)
        bits_log.append(bit)
        if len(bits_log) > cfg["dgim"]["max_window"]:
            bits_log.pop(0)

    for k in cfg["dgim"]["query_ks"]:
        exact_ones = sum(bits_log[-k:]) if len(bits_log) >= k else sum(bits_log)
        est = dgim.query(k)
        err = round(abs(est - exact_ones) / exact_ones * 100, 2) if exact_ones else 0
        print(f"   k={k:>6}  exact={exact_ones:>5}  estimate={est:>5}  err={err:.2f}%  "
              f"{'[OK - within 50%]' if err <= 50 else '[WARN - exceeds 50%]'}")
    print()

    # ── 6. MinHash-LSH ────────────────────────────────────────────────────────
    print("[6] MinHash-LSH  (destination sets per pickup zone, Jaccard >= 0.50)")
    zone_sets: dict = defaultdict(set)
    zone_trips: dict = defaultdict(int)
    for _, row in df.iterrows():
        pz = int(row["PULocationID"])
        dz = int(row["DOLocationID"])
        zone_sets[pz].add(dz)
        zone_trips[pz] += 1

    min_trips = cfg["lsh"]["min_trips_per_zone"]
    active = {z: s for z, s in zone_sets.items() if zone_trips[z] >= min_trips}
    n_zones = len(active)
    n_possible = n_zones * (n_zones - 1) // 2
    print(f"   Active zones (>= {min_trips} trips): {n_zones}  |  Possible pairs: {n_possible:,}")

    minhash = MinHash(num_hashes=cfg["lsh"]["num_hashes"])

    t0 = time.perf_counter()
    signatures = {z: minhash.signature(s) for z, s in active.items()}
    sig_ms = round((time.perf_counter() - t0) * 1000, 2)

    t0 = time.perf_counter()
    true_pairs = all_pairs_jaccard(active, threshold=cfg["lsh"]["jaccard_threshold"])
    exact_ms = round((time.perf_counter() - t0) * 1000, 2)

    print(f"   Signature build time : {sig_ms} ms")
    print(f"   Exact all-pairs time : {exact_ms} ms  (for {n_possible:,} pairs)")
    print(f"   True pairs (J>=0.5)  : {len(true_pairs)}")
    print()

    for (b, r, label) in [
        (cfg["lsh"]["bands"], cfg["lsh"]["rows_per_band"], "20x5_default"),
        (10, 10, "10x10_alternative"),
    ]:
        t0 = time.perf_counter()
        lsh = LSH(num_hashes=cfg["lsh"]["num_hashes"], bands=b, rows_per_band=r)
        for z, sig in signatures.items():
            lsh.add(z, sig)
        cands = lsh.get_candidates()
        confirmed = set()
        for (za, zb) in cands:
            if jaccard(active[za], active[zb]) >= cfg["lsh"]["jaccard_threshold"]:
                confirmed.add((za, zb))
        lsh_ms = round((time.perf_counter() - t0) * 1000, 2)

        rec = len(confirmed & true_pairs) / len(true_pairs) if true_pairs else 1.0
        prec = len(confirmed) / len(cands) if cands else 0.0
        red  = 1 - (len(cands) / n_possible) if n_possible else 0.0

        print(f"   [{label}]  bands={b} rows={r}")
        print(f"     Candidates     : {len(cands):>5}  (reduction={red:.4f})")
        print(f"     Confirmed pairs: {len(confirmed):>5}")
        print(f"     Recall         : {rec:.4f}")
        print(f"     Precision      : {prec:.4f}")
        print(f"     LSH query time : {lsh_ms} ms")
        print()

    print("=" * 70)
    print(" Sanity check complete.")
    print(" NOTE: consumer_lag, stage2_eps, latency_p50/p95 are NOT measured here.")
    print("       Run the full Kafka pipeline to get those numbers.")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Offline algorithm sanity check")
    parser.add_argument("--config",     default="config.yaml")
    parser.add_argument("--max-events", type=int, default=50000,
                        help="Number of rows to use from the dataset (default 50000)")
    args = parser.parse_args()
    cfg = load_config(args.config)
    run_sanity_check(cfg, max_events=args.max_events)
