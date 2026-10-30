"""
stage2_stream_analytics/stage2.py
──────────────────────────────────
Consumes taxi.clean → computes streaming analytics → publishes summaries to taxi.metrics.

Analytics implemented:
  5.1  Bloom Filter  – FP/FN rates (tracked in Stage 1, aggregated at final consumer)
  5.2  Reservoir sampling – stream-level mean fare_amount and trip_distance (k=100, 1000, 10000)
  5.3  Event vs entity sampling – window diversity of pickup zones (equal storage budget)
  5.4  HyperLogLog – distinct route_key values per 60-min event-time window
  5.5  DGIM – continuous sliding window of most recent 10,000 cleaned events

Watermarks & Late Events (Task 5.1):
  - Watermark = max_event_time - allowed_lateness (default 20 seconds)
  - Windows close only when watermark >= window_end
  - Events older than watermark for closed windows are logged as late and discarded
"""

import json
import sys
import time
import logging
import argparse
import pathlib
import hashlib
import random
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from kafka import KafkaConsumer, KafkaProducer
from utils.config import load_config
from utils.reservoir import ReservoirSampler
from utils.hll import HyperLogLog
from utils.dgim import DGIM
from utils.consumer_stats import AssignmentReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE2] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def parse_event_time(event_time_str: str) -> datetime | None:
    try:
        return datetime.fromisoformat(str(event_time_str))
    except Exception:
        return None


def get_window_bounds(dt: datetime, window_minutes: int) -> tuple[str, str, datetime, datetime]:
    """Calculate floored window start and window end ISO strings and datetimes."""
    floored_minute = dt.minute - (dt.minute % window_minutes)
    w_start = dt.replace(minute=floored_minute, second=0, microsecond=0)
    w_end   = w_start + timedelta(minutes=window_minutes)
    return w_start.isoformat(), w_end.isoformat(), w_start, w_end


def hash_entity_sample(zone: int, fraction: float) -> bool:
    """Deterministic hash-based entity sampling for pickup_zone."""
    h = int(hashlib.md5(str(zone).encode()).hexdigest(), 16)
    return (h % 1000) < int(fraction * 1000)


# ──────────────────────────────────────────────────────────────────────────────
# Window State Container (per 60-min window)
# ──────────────────────────────────────────────────────────────────────────────

class WindowState:
    def __init__(self, window_start: str, window_end: str, run_id: str,
                 sampling_fraction: float = 0.15, hll_precision: int = 12):
        self.window_start = window_start
        self.window_end   = window_end
        self.run_id       = run_id

        # 5.3 Sampling – equal storage budget (15% events vs 15% zones)
        self.sample_fraction = sampling_fraction
        self.exact_zones: dict[int, set] = defaultdict(set)
        self.event_sample_zones: dict[int, set] = defaultdict(set)
        self.entity_sample_zones: dict[int, set] = defaultdict(set)

        # 5.4 HLL for distinct routes within this window
        self.hll = HyperLogLog(precision=hll_precision)

        # Window metrics
        self.events_in_window = 0
        self.latencies_ms: list[int] = []
        self.t_window_open = time.time()

    def ingest(self, event: dict):
        self.events_in_window += 1

        route    = event.get("route_key", "")
        zone_p   = int(event.get("pickup_zone", 0))
        zone_d   = int(event.get("dropoff_zone", 0))
        p_ts     = int(event.get("producer_ts_ms", 0))

        # Latency tracking (arrival wall-clock minus producer emission)
        if p_ts > 0:
            lat = int(time.time() * 1000) - p_ts
            if lat >= 0:
                self.latencies_ms.append(lat)

        # 5.3 Zone diversity tracking
        if zone_p and zone_d:
            self.exact_zones[zone_p].add(zone_d)
            if random.random() < self.sample_fraction:
                self.event_sample_zones[zone_p].add(zone_d)
            if hash_entity_sample(zone_p, self.sample_fraction):
                self.entity_sample_zones[zone_p].add(zone_d)

        # 5.4 HLL distinct routes
        if route:
            self.hll.add(route)

    def finalize(self, cfg: dict, consumer_lag: int = 0,
                 dgim_summary: dict = None, reservoir_summary: dict = None,
                 late_events_summary: dict = None) -> dict:
        """Build the standardized taxi.metrics JSON object for this window."""
        elapsed = time.time() - self.t_window_open
        stage2_eps = round(self.events_in_window / elapsed, 1) if elapsed > 0 else 0.0

        # Latency percentiles
        lats = sorted(self.latencies_ms)
        p50 = lats[len(lats) // 2] if lats else 0
        p95 = lats[int(len(lats) * 0.95)] if lats else 0

        # 5.3 Diversity calculation (zones with >= 10 distinct drop-off zones)
        min_dropoffs = 10
        exact_div = sum(1 for s in self.exact_zones.values() if len(s) >= min_dropoffs)
        exact_total = len(self.exact_zones)
        exact_frac = round(exact_div / exact_total, 4) if exact_total else 0.0

        event_div = sum(1 for s in self.event_sample_zones.values() if len(s) >= min_dropoffs)
        event_total = len(self.event_sample_zones)
        event_frac = round(event_div / event_total, 4) if event_total else 0.0

        entity_div = sum(1 for s in self.entity_sample_zones.values() if len(s) >= min_dropoffs)
        entity_total = len(self.entity_sample_zones)
        entity_frac = round(entity_div / entity_total, 4) if entity_total else 0.0

        # 5.4 HLL
        hll_est = self.hll.estimate()
        hll_exact = self.hll.exact_count()
        hll_err = round(abs(hll_est - hll_exact) / hll_exact * 100, 2) if hll_exact else 0.0

        return {
            "run_id":           self.run_id,
            "window_start":     self.window_start,
            "window_end":       self.window_end,
            "events_processed": self.events_in_window,
            "stage2_eps":       stage2_eps,
            "consumer_lag":     int(consumer_lag),
            "latency_p50_ms":   p50,
            "latency_p95_ms":   p95,

            "sampling":         reservoir_summary or {},
            "diversity": {
                "exact_fraction":         exact_frac,
                "event_sample_fraction":  event_frac,
                "entity_sample_fraction": entity_frac,
                "exact_active_zones":     exact_total,
                "storage_budget_fraction": self.sample_fraction,
                "note": "entity_sampling preserves zone route clusters; event_sampling underestimates diversity"
            },
            "hll": {
                "exact_routes":       hll_exact,
                "estimate":           hll_est,
                "error_pct":          hll_err,
                "precision_b":        self.hll.b,
                "registers":          self.hll.m,
                "sketch_bytes":       self.hll.memory_sketch_bytes(),
                "exact_bytes_approx": self.hll.memory_exact_bytes(),
            },
            "dgim":        dgim_summary or {},
            "late_events": late_events_summary or {},
        }


# ──────────────────────────────────────────────────────────────────────────────
# Stage 2 Analytics Worker
# ──────────────────────────────────────────────────────────────────────────────

def measure_consumer_lag(consumer: KafkaConsumer) -> int:
    try:
        partitions = consumer.assignment()
        if not partitions:
            return 0
        end_offsets = consumer.end_offsets(partitions)
        lag = sum(end_offsets.get(tp, 0) - consumer.position(tp) for tp in partitions)
        return max(0, lag)
    except Exception:
        return 0


def run(cfg: dict, group_id: str = "stage2-group", run_id_override: str = None):
    kafka_cfg = cfg["kafka"]
    run_id    = run_id_override or cfg["producer"].get("run_id", "P3-medium")
    win_mins  = int(cfg.get("window_minutes", 60))
    res_sizes = cfg["reservoir"]["sizes"]
    max_delay = int(cfg["producer"].get("max_delay_seconds", 20))
    allowed_lateness = timedelta(seconds=max_delay + 5)

    consumer = KafkaConsumer(
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

    metrics_topic = kafka_cfg["topics"]["metrics"]
    # Report this process's partition assignment (counted into the results CSV)
    consumer.subscribe([kafka_cfg["topics"]["clean"]],
                       listener=AssignmentReporter(producer, metrics_topic, "stage2"))

    # 5.2 Global Reservoir Samplers (streaming across full run)
    reservoirs = {k: ReservoirSampler(k) for k in res_sizes}

    # 5.5 Global DGIM (sliding window of last 10,000 cleaned events)
    dgim = DGIM(max_window=cfg["dgim"]["max_window"])
    dgim_query_ks = cfg["dgim"]["query_ks"]
    slow_trip_bits: list[int] = []

    # Per-window states
    windows: dict[str, WindowState] = {}
    closed_windows: set[str] = set()
    window_end_dts: dict[str, datetime] = {}

    # Watermark tracking
    max_event_time: datetime | None = None
    late_events_count = 0
    last_late_sample = None

    # Run-level EPS tracking (avoids consumer-timeout skew on last window)
    t_run_start: float | None = None
    total_events_all = 0

    log.info("Stage 2 running. Group=%s | RunID=%s | Consuming %s -> %s",
             group_id, run_id, kafka_cfg["topics"]["clean"], metrics_topic)

    def compute_dgim_summary() -> dict:
        queries = {}
        for k in dgim_query_ks:
            recent_bits = slow_trip_bits[-k:] if len(slow_trip_bits) >= k else slow_trip_bits
            exact_ones = sum(recent_bits)
            est = dgim.query(k)
            err = round(abs(est - exact_ones) / exact_ones * 100, 2) if exact_ones else 0.0
            queries[str(k)] = {
                "exact_ones": exact_ones,
                "estimate":   est,
                "abs_error":  abs(est - exact_ones),
                "error_pct":  err,
            }
        return {
            "bucket_count": dgim.bucket_count(),
            "max_window":   dgim.N,
            "queries":      queries,
        }

    for msg in consumer:
        event = msg.value
        e_time_str = event.get("event_time", "")
        dt = parse_event_time(e_time_str)
        if not dt:
            continue

        # Start run-level timer on first valid event
        if t_run_start is None:
            t_run_start = time.time()

        total_events_all += 1

        # Ingest into stream-level analytics
        fare     = float(event.get("fare_amount", 0) or 0)
        dist     = float(event.get("trip_distance", 0) or 0)
        slow_bit = int(event.get("slow_trip_bit", 0))

        for rs in reservoirs.values():
            rs.add(fare, dist)

        dgim.add(slow_bit)
        slow_trip_bits.append(slow_bit)
        if len(slow_trip_bits) > dgim.N:
            slow_trip_bits.pop(0)

        # Update watermark
        if max_event_time is None or dt > max_event_time:
            max_event_time = dt

        watermark = max_event_time - allowed_lateness

        w_start, w_end, start_dt, end_dt = get_window_bounds(dt, win_mins)

        # ── Late Event Check ──────────────────────────────────────────────────
        # Also discard if the window end is already past the watermark,
        # even if it was never explicitly opened (window that never got any events).
        if w_start in closed_windows or end_dt <= watermark:
            late_events_count += 1
            last_late_sample = {
                "event_id": event.get("event_id"),
                "event_time": e_time_str,
                "window": w_start,
                "watermark": watermark.isoformat(),
                "action": "discarded_after_window_closed",
            }
            if late_events_count == 1 or late_events_count % 200 == 0:
                log.info("[LATE EVENT] Event %s (time=%s) arrived after window %s closed. Watermark=%s. Discarded.",
                         event.get("event_id"), e_time_str, w_start, watermark.isoformat())
            continue

        # Open window if not yet open
        if w_start not in windows:
            windows[w_start] = WindowState(w_start, w_end, run_id,
                                           sampling_fraction=float(cfg.get("entity_sample_fraction", 0.15)),
                                           hll_precision=int(cfg["hll"]["precision"]))
            window_end_dts[w_start] = end_dt
            log.info("Opened window %s -> %s", w_start, w_end)

        windows[w_start].ingest(event)

        # ── Check for Windows to Close (Watermark >= window_end) ───────────────
        to_close = [k for k, w_dt in window_end_dts.items() if watermark >= w_dt]
        for wk in to_close:
            lag = measure_consumer_lag(consumer)
            dgim_summary = compute_dgim_summary()
            res_summary = {str(k): rs.stats() for k, rs in reservoirs.items()}
            late_summary = {
                "count": late_events_count,
                "sample": last_late_sample,
                "allowed_lateness_sec": max_delay,
            }

            summary = windows[wk].finalize(
                cfg=cfg,
                consumer_lag=lag,
                dgim_summary=dgim_summary,
                reservoir_summary=res_summary,
                late_events_summary=late_summary,
            )

            log.info("Closing window %s -> publishing to %s (events=%d, lag=%d, HLL_err=%.2f%%)",
                     wk, metrics_topic, summary["events_processed"], lag, summary["hll"]["error_pct"])
            producer.send(metrics_topic, value=summary)
            producer.flush()

            closed_windows.add(wk)
            del windows[wk]
            del window_end_dts[wk]

    # Flush remaining active windows on stream end
    for wk, win in list(windows.items()):
        lag = measure_consumer_lag(consumer)
        dgim_summary = compute_dgim_summary()
        res_summary = {str(k): rs.stats() for k, rs in reservoirs.items()}
        late_summary = {
            "count": late_events_count,
            "sample": last_late_sample,
            "allowed_lateness_sec": max_delay,
        }
        summary = win.finalize(
            cfg=cfg,
            consumer_lag=lag,
            dgim_summary=dgim_summary,
            reservoir_summary=res_summary,
            late_events_summary=late_summary,
        )
        producer.send(metrics_topic, value=summary)
        producer.flush()
        log.info("Stream ended. Finalized last window %s (events=%d)", wk, summary["events_processed"])

    run_elapsed = (time.time() - t_run_start) if t_run_start else 1.0
    run_eps = round(total_events_all / run_elapsed, 1) if run_elapsed > 0 else 0.0
    log.info("Stage 2 finished. Total events consumed: %d | Run EPS: %.1f | Late: %d",
             total_events_all, run_eps, late_events_count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 2: Stream Analytics")
    parser.add_argument("--config",   default="config.yaml")
    parser.add_argument("--group-id", default="stage2-group")
    parser.add_argument("--run-id",   default=None, help="Override run_id")
    args = parser.parse_args()
    cfg = load_config(args.config)
    run(cfg, group_id=args.group_id, run_id_override=args.run_id)
