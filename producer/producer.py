"""
producer/producer.py
────────────────────
Reads NYC Taxi trip data and publishes events to Kafka topic taxi.raw.

Features
--------
* Configurable events-per-second rate via config.yaml  (producer.rate)
* Injects ~2% deliberate duplicates (same event_id re-sent)
* Delays ~2% of events by 1-20 seconds (sent out of order)
* Uses pickup_zone as the Kafka partition key (baseline)
* Reports actual throughput every 5 seconds
"""

import json
import os
import random
import sys
import time
import uuid
import pathlib
import argparse
import logging

# ── path fix so we can import utils from parent directory ─────────────────────
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
from utils.config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Dataset downloader ────────────────────────────────────────────────────────

def ensure_dataset(path: str) -> str:
    """Download Yellow Taxi Jan-2024 Parquet if not already present."""
    p = pathlib.Path(path)
    if p.exists():
        log.info("Dataset found at %s", p)
        return str(p)

    p.parent.mkdir(parents=True, exist_ok=True)
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    log.info("Downloading dataset from %s …", url)
    import urllib.request
    urllib.request.urlretrieve(url, str(p))
    log.info("Downloaded to %s  (%.1f MB)", p, p.stat().st_size / 1e6)
    return str(p)


# ── Record builder ────────────────────────────────────────────────────────────

import math

def _num(val, default, cast):
    """NaN-safe numeric conversion. Returns default if val is NaN/None/falsy."""
    try:
        v = cast(val)
        if cast is float and math.isnan(v):
            return default
        return v
    except (TypeError, ValueError):
        return default


def build_event(row, row_index: int) -> dict:
    """Map a pandas row to the canonical taxi event schema."""
    event_id = f"2024-01-row-{row_index:08d}"
    return {
        "event_id":        event_id,
        "event_time":      str(row.get("tpep_pickup_datetime", "")),
        "pickup_zone":     _num(row.get("PULocationID", 0),   0, int),
        "dropoff_zone":    _num(row.get("DOLocationID", 0),   0, int),
        "trip_distance":   _num(row.get("trip_distance", 0.0), 0.0, float),
        "fare_amount":     _num(row.get("fare_amount", 0.0),  0.0, float),
        "passenger_count": _num(row.get("passenger_count", 1), 1, int) or 1,
        "payment_type":    _num(row.get("payment_type", 1),   1, int) or 1,
        "dropoff_time":    str(row.get("tpep_dropoff_datetime", "")),
        "producer_ts_ms":  int(time.time() * 1000),
    }


# ── Main producer loop ────────────────────────────────────────────────────────

def run(cfg: dict, partition_key: str = "pickup_zone", consumers: int = None):
    kafka_cfg = cfg["kafka"]
    prod_cfg  = cfg["producer"]

    dataset_path = ensure_dataset(prod_cfg["dataset_path"])

    log.info("Loading dataset …")
    df = pd.read_parquet(dataset_path, engine="pyarrow")

    # ── Filter invalid rows ────────────────────────────────────────────────
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                            "PULocationID", "DOLocationID"])
    df = df[(df["PULocationID"] > 0) & (df["DOLocationID"] > 0)]

    # Filter to Jan-2024 only (dataset has stray 2002/2009/2023 rows)
    jan_start = pd.Timestamp("2024-01-01")
    jan_end   = pd.Timestamp("2024-02-01")
    df = df[
        (df["tpep_pickup_datetime"] >= jan_start) &
        (df["tpep_pickup_datetime"] <  jan_end) &
        (df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"])
    ]

    df = df.sort_values("tpep_pickup_datetime").reset_index(drop=True)
    log.info("Dataset ready: %d rows (Jan-2024 only)", len(df))

    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        acks="all",
        retries=3,
        linger_ms=5,
    )

    rate        = prod_cfg["rate"]          # events / second
    dup_rate    = prod_cfg["duplicate_rate"]
    delay_rate  = prod_cfg["delayed_event_rate"]
    max_delay   = prod_cfg["max_delay_seconds"]
    max_events  = int(prod_cfg.get("max_events", 0))  # 0 = unlimited
    topic       = kafka_cfg["topics"]["raw"]
    metrics_topic = kafka_cfg["topics"]["metrics"]

    from collections import deque
    recent_events: deque = deque(maxlen=2000)

    interval    = 1.0 / rate               # seconds per event
    pending_delayed: list = []             # [(release_time, event, key)]

    total_sent  = 0
    window_sent = 0
    window_start = time.time()
    t_run_start  = time.time()             # full-run timer for final EPS
    report_interval = 5.0  # seconds

    # Partition tracking for partition skew measurement (Task 4)
    partition_counts = {}

    def on_send_success(record_metadata):
        p = record_metadata.partition
        partition_counts[p] = partition_counts.get(p, 0) + 1

    def send_event(ev: dict, k):
        # Wall-clock timestamp right when record is pushed to Kafka buffer
        ev["producer_ts_ms"] = int(time.time() * 1000)
        fut = producer.send(topic, value=ev, key=k)
        fut.add_callback(on_send_success)

    log.info("Starting producer -> topic=%s  rate=%d eps  key=%s",
             topic, rate, partition_key)

    for idx, row in df.iterrows():
        if max_events > 0 and total_sent >= max_events:
            log.info("max_events=%d reached. Stopping producer.", max_events)
            break
        event = build_event(row, int(idx))
        key   = str(event[partition_key]) if partition_key else None

        # ── Decide: duplicate previous event? (Task 1.3) ─────────────────
        if recent_events and random.random() < dup_rate:
            dup_ev = random.choice(recent_events).copy()
            dup_key = str(dup_ev[partition_key]) if partition_key and partition_key in dup_ev else key
            send_event(dup_ev, dup_key)
            total_sent += 1
            window_sent += 1

        # ── Decide: delay current event? (Task 1.4) ──────────────────────
        is_delayed = random.random() < delay_rate
        if is_delayed:
            delay_sec = random.uniform(1, max_delay)
            release_at = time.time() + delay_sec
            pending_delayed.append((release_at, event, key))
        else:
            send_event(event, key)
            recent_events.append(event)
            total_sent += 1
            window_sent += 1

        # ── Release any pending delayed events ────────────────────────────
        now = time.time()
        still_pending = []
        for (rt, ev, k) in pending_delayed:
            if now >= rt:
                send_event(ev, k)
                recent_events.append(ev)
                total_sent += 1
                window_sent += 1
            else:
                still_pending.append((rt, ev, k))
        pending_delayed = still_pending

        # ── Throughput report ─────────────────────────────────────────────
        now = time.time()
        elapsed = now - window_start
        if elapsed >= report_interval:
            actual_eps = window_sent / elapsed
            total_p_msgs = sum(partition_counts.values()) or 1
            busiest = max(partition_counts.values()) if partition_counts else 0
            skew = busiest / total_p_msgs

            log.info("Throughput: %.1f eps  |  total sent: %d  |  skew: %.3f  |  partitions: %s",
                     actual_eps, total_sent, skew, dict(partition_counts))

            # Publish throughput & skew to taxi.metrics for final_consumer
            try:
                producer.send(metrics_topic, value={
                    "type": "producer_stats",
                    "run_id": prod_cfg.get("run_id", "default"),
                    "producer_eps": round(actual_eps, 2),
                    "total_sent": total_sent,
                    "partition_skew": round(skew, 4),
                    "partition_counts": dict(partition_counts),
                })
            except Exception as e:
                log.debug("Could not publish producer_stats: %s", e)

            window_sent = 0
            window_start = now

        # ── Rate control ─────────────────────────────────────────────────
        time.sleep(interval)

    # Flush remaining delayed events
    for (_, ev, k) in pending_delayed:
        send_event(ev, k)
        total_sent += 1

    producer.flush()

    total_p_msgs = sum(partition_counts.values()) or 1
    busiest = max(partition_counts.values()) if partition_counts else 0
    final_skew = busiest / total_p_msgs

    # Final stats send – use full run duration for accurate EPS
    run_elapsed = time.time() - t_run_start
    final_eps   = round(total_sent / run_elapsed, 2) if run_elapsed > 0 else 0.0
    try:
        producer.send(metrics_topic, value={
            "type": "producer_stats",
            "run_id": prod_cfg.get("run_id", "default"),
            "producer_eps": final_eps,
            "total_sent": total_sent,
            "partition_skew": round(final_skew, 4),
            "partition_counts": dict(partition_counts),
        })
        producer.flush()
    except Exception:
        pass

    log.info("Producer finished.  Total events sent: %d  |  Final EPS: %.1f  |  Final skew: %.4f",
             total_sent, final_eps, final_skew)


# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Taxi Kafka Producer")
    parser.add_argument("--config",     default="config.yaml",  help="Path to config.yaml")
    parser.add_argument("--rate",       type=int, default=None,  help="Override producer rate")
    parser.add_argument("--key",        default="pickup_zone",   help="Partition key field or 'none'")
    parser.add_argument("--run-id",     default=None,            help="Override run_id")
    parser.add_argument("--max-events", type=int, default=None,  help="Override max_events limit (0=unlimited)")
    args = parser.parse_args()

    cfg = load_config(args.config)
    if args.rate:
        cfg["producer"]["rate"] = args.rate
    if args.run_id:
        cfg["producer"]["run_id"] = args.run_id
    if args.max_events is not None:
        cfg["producer"]["max_events"] = args.max_events

    partition_key = None if args.key.lower() == "none" else args.key
    run(cfg, partition_key=partition_key)
