"""
stage3_lsh/stage3.py
─────────────────────
Consumes taxi.clean → builds per-pickup-zone destination sets per 60-min window
→ runs MinHash-LSH to find similar pickup zones → publishes results to taxi.lsh.

Timing Instrumentation (Task 3 / Table 8):
  - sig_build_ms: time to compute MinHash signatures for all active zones
  - lsh_query_ms: time for LSH banding, candidate retrieval, and candidate Jaccard verification
  - exact_all_pairs_ms: offline evaluation baseline time for all-pairs brute force Jaccard

Watermarks & Late Events (Task 5.1):
  - Aligned with Stage 2 event-time watermark
  - Closed windows do not re-open on late arrivals
"""

import json
import sys
import time
import logging
import argparse
import pathlib
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from kafka import KafkaConsumer, KafkaProducer
from utils.config import load_config
from utils.minhash_lsh import MinHash, LSH, jaccard, all_pairs_jaccard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE3] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def parse_event_time(event_time_str: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(event_time_str))
    except Exception:
        return None


def get_window_bounds(dt: datetime, window_minutes: int) -> tuple[str, str, datetime, datetime]:
    floored_minute = dt.minute - (dt.minute % window_minutes)
    w_start = dt.replace(minute=floored_minute, second=0, microsecond=0)
    w_end   = w_start + timedelta(minutes=window_minutes)
    return w_start.isoformat(), w_end.isoformat(), w_start, w_end


def run_lsh_on_window(
    zone_sets: dict,          # pickup_zone → set of dropoff_zones
    trip_counts: dict,        # pickup_zone → trip count
    min_trips: int,
    minhash: MinHash,
    lsh_params: list,         # [(bands, rows_per_band, label), ...]
    threshold: float,
    window_start: str,
    window_end: str,
    run_id: str,
) -> dict:
    """Run MinHash-LSH with clean, independent latency instrumentation."""
    # Filter to active pickup zones with at least min_trips
    active_zones = {z: s for z, s in zone_sets.items()
                    if trip_counts.get(z, 0) >= min_trips}

    n_active = len(active_zones)
    n_possible = n_active * (n_active - 1) // 2

    if n_active < 2:
        return {
            "run_id": run_id,
            "window_start": window_start,
            "window_end": window_end,
            "active_zones": n_active,
            "possible_pairs": n_possible,
            "true_pairs": 0,
            "lsh_settings": {},
            "note": "fewer than 2 active zones in window",
        }

    # 1. Independent timing: MinHash Signatures
    t_sig_start = time.perf_counter()
    signatures = {}
    for zone, dest_set in active_zones.items():
        signatures[zone] = minhash.signature(dest_set)
    sig_build_ms = round((time.perf_counter() - t_sig_start) * 1000, 2)

    # 2. Independent timing: Exact All-Pairs Jaccard (Evaluation Baseline)
    t_exact_start = time.perf_counter()
    true_pairs = all_pairs_jaccard(active_zones, threshold)
    exact_latency_ms = round((time.perf_counter() - t_exact_start) * 1000, 2)

    # 3. Independent timing: LSH Banding & Candidate Verification for each setting
    results_per_setting = {}
    for (bands, rows_per_band, label) in lsh_params:
        t_lsh_start = time.perf_counter()
        lsh = LSH(num_hashes=minhash.num_hashes,
                  bands=bands, rows_per_band=rows_per_band)

        for zone, sig in signatures.items():
            lsh.add(zone, sig)

        candidates = lsh.get_candidates()

        # Verify candidate pairs with exact Jaccard
        confirmed = set()
        candidate_jaccards = {}
        for (a, b) in candidates:
            j = jaccard(active_zones[a], active_zones[b])
            candidate_jaccards[(a, b)] = round(j, 4)
            if j >= threshold:
                confirmed.add((a, b))

        lsh_query_ms = round((time.perf_counter() - t_lsh_start) * 1000, 2)

        recall = len(confirmed & true_pairs) / len(true_pairs) if true_pairs else 1.0
        precision = len(confirmed) / len(candidates) if candidates else 0.0
        candidate_reduction = 1 - (len(candidates) / n_possible) if n_possible else 0.0

        sample_pairs = [
            {"zone_a": a, "zone_b": b, "jaccard": candidate_jaccards.get((a, b), 0.0)}
            for (a, b) in list(confirmed)[:5]
        ]

        results_per_setting[label] = {
            "bands":               bands,
            "rows_per_band":       rows_per_band,
            "candidates":          len(candidates),
            "true_pairs":          len(true_pairs),
            "confirmed_pairs":     len(confirmed),
            "recall":              round(recall, 4),
            "candidate_precision": round(precision, 4),
            "candidate_reduction": round(candidate_reduction, 4),
            "sig_build_ms":        sig_build_ms,
            "lsh_query_ms":        lsh_query_ms,
            "exact_all_pairs_ms":  exact_latency_ms,
            "sample_pairs":        sample_pairs,
        }

    return {
        "run_id":          run_id,
        "window_start":    window_start,
        "window_end":      window_end,
        "active_zones":    n_active,
        "possible_pairs":  n_possible,
        "true_pairs":      len(true_pairs),
        "lsh_settings":    results_per_setting,
    }


def run(cfg: dict, group_id: str = "stage3-group", run_id_override: str = None):
    kafka_cfg = cfg["kafka"]
    lsh_cfg   = cfg["lsh"]
    run_id    = run_id_override or cfg["producer"].get("run_id", "P3-medium")
    win_mins  = int(cfg.get("window_minutes", 60))
    max_delay = int(cfg["producer"].get("max_delay_seconds", 20))
    allowed_lateness = timedelta(seconds=max_delay + 5)

    minhash = MinHash(num_hashes=lsh_cfg["num_hashes"])

    # Two required parameter settings (both use 100 hash values)
    lsh_params = [
        (lsh_cfg["bands"], lsh_cfg["rows_per_band"], "20x5_default"),
        (10, 10, "10x10_alternative"),
    ]

    consumer = KafkaConsumer(
        kafka_cfg["topics"]["clean"],
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=120_000,
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=5,
    )

    lsh_topic = kafka_cfg["topics"]["lsh"]

    zone_sets: dict[str, dict[int, set]] = {}
    trip_counts: dict[str, dict[int, int]] = {}
    window_end_dts: dict[str, datetime] = {}
    closed_windows: set[str] = set()

    max_event_time: datetime | None = None

    log.info("Stage 3 running. Group=%s | RunID=%s | Consuming %s -> %s",
             group_id, run_id, kafka_cfg["topics"]["clean"], lsh_topic)

    for msg in consumer:
        event = msg.value
        e_time_str = event.get("event_time", "")
        dt = parse_event_time(e_time_str)
        if not dt:
            continue

        if max_event_time is None or dt > max_event_time:
            max_event_time = dt

        watermark = max_event_time - allowed_lateness
        w_start, w_end, start_dt, end_dt = get_window_bounds(dt, win_mins)

        # Ignore late arrivals for already closed windows, or events whose
        # window end is already past the current watermark (Task 5.1 watermark)
        if w_start in closed_windows or end_dt <= watermark:
            continue

        if w_start not in zone_sets:
            zone_sets[w_start] = defaultdict(set)
            trip_counts[w_start] = defaultdict(int)
            window_end_dts[w_start] = end_dt
            log.info("Stage 3 opened window %s", w_start)

        z_p = int(event.get("pickup_zone", 0))
        z_d = int(event.get("dropoff_zone", 0))
        if z_p and z_d:
            zone_sets[w_start][z_p].add(z_d)
            trip_counts[w_start][z_p] += 1

        # Check for windows to close based on watermark
        to_close = [k for k, w_dt in window_end_dts.items() if watermark >= w_dt]
        for wk in to_close:
            log.info("Stage 3 closing window %s (watermark=%s)", wk, watermark.isoformat())
            result = run_lsh_on_window(
                zone_sets=dict(zone_sets[wk]),
                trip_counts=dict(trip_counts[wk]),
                min_trips=lsh_cfg["min_trips_per_zone"],
                minhash=minhash,
                lsh_params=lsh_params,
                threshold=lsh_cfg["jaccard_threshold"],
                window_start=wk,
                window_end=window_end_dts[wk].isoformat(),
                run_id=run_id,
            )
            producer.send(lsh_topic, value=result)
            producer.flush()
            closed_windows.add(wk)
            del zone_sets[wk]
            del trip_counts[wk]
            del window_end_dts[wk]

    # Flush remaining windows on stream end
    for wk in list(zone_sets.keys()):
        result = run_lsh_on_window(
            zone_sets=dict(zone_sets[wk]),
            trip_counts=dict(trip_counts[wk]),
            min_trips=lsh_cfg["min_trips_per_zone"],
            minhash=minhash,
            lsh_params=lsh_params,
            threshold=lsh_cfg["jaccard_threshold"],
            window_start=wk,
            window_end=window_end_dts[wk].isoformat(),
            run_id=run_id,
        )
        producer.send(lsh_topic, value=result)
        producer.flush()
        log.info("Stage 3 finalized last window %s", wk)

    log.info("Stage 3 finished.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 3: LSH Similarity")
    parser.add_argument("--config",   default="config.yaml")
    parser.add_argument("--group-id", default="stage3-group")
    parser.add_argument("--run-id",   default=None, help="Override run_id")
    args = parser.parse_args()
    cfg = load_config(args.config)
    run(cfg, group_id=args.group_id, run_id_override=args.run_id)
