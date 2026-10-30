"""
stage1_clean_bloom/stage1.py
──────────────────────────────
Consumes taxi.raw → cleans/enriches events → publishes to taxi.clean.

Added fields
────────────
  route_key         = "<pickup_zone>-<dropoff_zone>"
  trip_duration_min = (dropoff_datetime - pickup_datetime) in minutes
  slow_trip_bit     = 1 if trip_duration_min > 30 else 0
  bloom_seen_before = result of Bloom-filter membership test
  stage1_ts_ms      = wall-clock timestamp of Stage 1 emit

Bloom filter
────────────
The filter is keyed on event_id.
We track whether each event_id was a genuine duplicate (producer flag) or
a new ID, enabling FP/FN measurement against ground truth.
"""

import json
import sys
import time
import logging
import argparse
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from kafka import KafkaConsumer, KafkaProducer
from utils.config import load_config
from utils.bloom_filter import BloomFilter
from utils.consumer_stats import AssignmentReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [STAGE1] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def is_valid(event: dict) -> bool:
    """Return False for events with missing/invalid critical fields."""
    try:
        if not event.get("pickup_zone") or not event.get("dropoff_zone"):
            return False
        if float(event.get("fare_amount", -1)) < 0:
            return False
        if not event.get("event_time"):
            return False
        return True
    except Exception:
        return False


def enrich(event: dict, bloom: BloomFilter, seen_ids: set) -> dict:
    """Add derived fields and Bloom-filter result."""
    from datetime import datetime

    # ── Derived fields ──────────────────────────────────────────────────────
    event["route_key"] = f"{event['pickup_zone']}-{event['dropoff_zone']}"

    try:
        pickup  = datetime.fromisoformat(str(event["event_time"]))
        dropoff = datetime.fromisoformat(str(event.get("dropoff_time", event["event_time"])))
        duration = (dropoff - pickup).total_seconds() / 60.0
    except Exception:
        duration = 0.0

    event["trip_duration_min"] = round(max(0.0, duration), 2)
    event["slow_trip_bit"]     = 1 if duration > 30 else 0

    # ── Bloom filter ────────────────────────────────────────────────────────
    eid = event["event_id"]
    is_real_dup = eid in seen_ids          # ground truth: was this id already seen?
    bloom_result = bloom.test_and_add(eid, is_real_dup)
    event["bloom_seen_before"] = bloom_result

    if not is_real_dup:
        seen_ids.add(eid)                  # first time we see this id

    event["stage1_ts_ms"] = int(time.time() * 1000)
    return event


def run(cfg: dict, group_id: str = "stage1-group"):
    kafka_cfg = cfg["kafka"]
    bloom_cfg = cfg["bloom"]

    bloom    = BloomFilter(
        expected_items=bloom_cfg["expected_items"],
        fp_rate=bloom_cfg["false_positive_rate"],
    )
    seen_ids: set = set()   # exact set of event_ids seen (for ground truth only)

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=60_000,
    )

    producer = KafkaProducer(
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8") if k else None,
        acks="all",
        linger_ms=5,
    )

    out_topic     = kafka_cfg["topics"]["clean"]
    metrics_topic = kafka_cfg["topics"]["metrics"]
    # Report this process's partition assignment (counted into the results CSV)
    consumer.subscribe([kafka_cfg["topics"]["raw"]],
                       listener=AssignmentReporter(producer, metrics_topic, "stage1"))
    processed     = 0
    window_cnt    = 0
    t_start       = time.time()

    log.info("Stage 1 running. Consuming from %s → publishing to %s",
             kafka_cfg["topics"]["raw"], out_topic)

    for msg in consumer:
        event = msg.value
        if not is_valid(event):
            continue

        enriched = enrich(event, bloom, seen_ids)
        key = str(enriched.get("pickup_zone", ""))
        producer.send(out_topic, value=enriched, key=key)

        processed  += 1
        window_cnt += 1

        if window_cnt >= 5000:
            elapsed = time.time() - t_start
            eps     = processed / elapsed
            b_info  = bloom.info()
            log.info("Processed %d  |  throughput %.1f eps  |  Bloom FP=%.4f",
                     processed, eps, bloom.fp_rate_observed)
            log.info("Bloom info: %s", b_info)

            # Publish Bloom metrics to taxi.metrics for final_consumer
            try:
                producer.send(metrics_topic, value={
                    "type": "bloom_stats",
                    "fp_rate": b_info["fp_rate_observed"],
                    "fn_rate": b_info["fn_rate_observed"],
                    "inserted_ids": b_info["inserted_ids"],
                    "bit_array_size_bits": b_info["bit_array_size_bits"],
                    "num_hash_functions": b_info["num_hash_functions"],
                    "stage1_eps": round(eps, 2),
                    "total_processed": processed,
                })
            except Exception as e:
                log.debug("Could not publish bloom_stats: %s", e)

            window_cnt = 0

    producer.flush()
    b_final = bloom.info()
    elapsed = time.time() - t_start
    final_eps = processed / elapsed if elapsed > 0 else 0.0

    try:
        producer.send(metrics_topic, value={
            "type": "bloom_stats",
            "fp_rate": b_final["fp_rate_observed"],
            "fn_rate": b_final["fn_rate_observed"],
            "inserted_ids": b_final["inserted_ids"],
            "bit_array_size_bits": b_final["bit_array_size_bits"],
            "num_hash_functions": b_final["num_hash_functions"],
            "stage1_eps": round(final_eps, 2),
            "total_processed": processed,
        })
        producer.flush()
    except Exception:
        pass

    log.info("Stage 1 done. Total processed: %d | Bloom: %s", processed, b_final)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stage 1: Clean + Bloom")
    parser.add_argument("--config",   default="config.yaml")
    parser.add_argument("--group-id", default="stage1-group")
    args = parser.parse_args()
    cfg = load_config(args.config)
    run(cfg, group_id=args.group_id)
