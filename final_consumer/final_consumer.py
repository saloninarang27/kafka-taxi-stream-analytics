"""
final_consumer/final_consumer.py
─────────────────────────────────
Reads from taxi.metrics and taxi.lsh, merges results per window, and:
  • Appends one JSON object per window to  results/results.jsonl
  • Prints a concise table to the console
  • Appends a row to  results/experiment_summary.csv
"""

import json
import sys
import time
import logging
import argparse
import pathlib
import csv
import os
from datetime import datetime

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from kafka import KafkaConsumer
from utils.config import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [FINAL] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

RESULTS_DIR = pathlib.Path(__file__).parent.parent / "results"
RESULTS_JSONL = RESULTS_DIR / "results.jsonl"
SUMMARY_CSV   = RESULTS_DIR / "experiment_summary.csv"

CSV_HEADERS = [
    "run_id",
    # Measured at run time: partitions from Kafka topic metadata, consumers from the
    # consumer_stats reports of the Stage 1 / Stage 2 processes that actually ran.
    "partitions", "stage1_consumers", "stage1_active_consumers",
    "stage2_consumers", "stage2_active_consumers",
    "window_start", "window_end", "events_processed",
    "producer_eps", "stage2_eps", "consumer_lag", "latency_p50_ms", "latency_p95_ms",
    "partition_skew",
    "bloom_fp_rate", "bloom_fn_rate",
    "sample_mean_fare", "exact_mean_fare", "fare_error_pct",
    "hll_exact", "hll_estimate", "hll_error_pct",
    "dgim_k5000_exact", "dgim_k5000_estimate", "dgim_k5000_error_pct",
    "lsh_candidates", "lsh_true_pairs", "lsh_recall", "lsh_candidate_reduction",
]


def build_output(metrics_msg: dict, lsh_msg: dict,
                 latest_bloom: dict = None, latest_producer: dict = None,
                 default_rate: float = 1000.0, parallelism: dict = None) -> dict:
    """Merge taxi.metrics and taxi.lsh summaries into standardized output."""
    run_id  = metrics_msg.get("run_id", "unknown")
    w_start = metrics_msg.get("window_start", "")
    w_end   = metrics_msg.get("window_end", "")

    # ── Producer throughput & skew ─────────────────────────────────────────
    p_eps = (
        metrics_msg.get("producer_eps")
        or (latest_producer.get("producer_eps") if latest_producer else None)
        or default_rate
    )
    skew = latest_producer.get("partition_skew", 0.25) if latest_producer else 0.25

    # ── Bloom stats ────────────────────────────────────────────────────────
    bloom_fp = (
        latest_bloom.get("fp_rate")
        if latest_bloom else 0.0
    )
    bloom_fn = (
        latest_bloom.get("fn_rate")
        if latest_bloom else 0.0
    )
    bloom_out = {
        "fp_rate": round(float(bloom_fp), 6),
        "fn_rate": round(float(bloom_fn), 6),
    }

    # ── Sampling ──────────────────────────────────────────────────────────
    sampling = metrics_msg.get("sampling", {})
    sample_k = sampling.get("1000", {})  # default k=1000
    exact_fare  = sample_k.get("exact_mean_fare", None)
    sample_fare = sample_k.get("sample_mean_fare", None)
    fare_err    = sample_k.get("fare_error_pct", None)
    sample_out = {
        "exact_mean_fare":   exact_fare,
        "sample_mean_fare":  sample_fare,
        "error_pct":         fare_err,
    }

    # ── HLL ───────────────────────────────────────────────────────────────
    hll = metrics_msg.get("hll", {})
    hll_out = {
        "exact_routes": hll.get("exact_routes", 0),
        "estimate":     hll.get("estimate", 0),
        "error_pct":    hll.get("error_pct", 0.0),
    }

    # ── DGIM ──────────────────────────────────────────────────────────────
    dgim_queries = metrics_msg.get("dgim", {}).get("queries", {})
    dgim_5k      = dgim_queries.get("5000", {})
    dgim_out = {
        "k": 5000,
        "exact_ones":   dgim_5k.get("exact_ones", 0),
        "estimate":     dgim_5k.get("estimate", 0),
        "error_pct":    dgim_5k.get("error_pct", 0.0),
        "bucket_count": metrics_msg.get("dgim", {}).get("bucket_count", 0),
    }

    # ── LSH ───────────────────────────────────────────────────────────────
    lsh_settings = lsh_msg.get("lsh_settings", {}) if lsh_msg else {}
    default_lsh  = lsh_settings.get("20x5_default", {})
    alt_lsh      = lsh_settings.get("10x10_alternative", {})

    lsh_out = {
        "candidates":          default_lsh.get("candidates", 0),
        "true_pairs":          default_lsh.get("true_pairs", 0),
        "recall":              default_lsh.get("recall", 0.0),
        "candidate_reduction": default_lsh.get("candidate_reduction", 0.0),
        "alt_10x10": {
            "candidates":          alt_lsh.get("candidates", 0),
            "recall":              alt_lsh.get("recall", 0.0),
            "candidate_reduction": alt_lsh.get("candidate_reduction", 0.0),
        },
    }

    # Standardized output schema (Section 9)
    out = {
        "run_id":           run_id,
        **(parallelism or {}),
        "window_start":     w_start,
        "window_end":       w_end,
        "events_processed": metrics_msg.get("events_processed", 0),
        "producer_eps":     round(float(p_eps), 1),
        "stage2_eps":       round(float(metrics_msg.get("stage2_eps", 0)), 1),
        "consumer_lag":     int(metrics_msg.get("consumer_lag", 0)),
        "latency_p50_ms":   int(metrics_msg.get("latency_p50_ms", 0)),
        "latency_p95_ms":   int(metrics_msg.get("latency_p95_ms", 0)),

        "bloom":            bloom_out,
        "sampling":         sample_out,
        "hll":              hll_out,
        "dgim":             dgim_out,
        "lsh":              lsh_out,

        # Additional instrumented fields
        "partition_skew":   round(float(skew), 4),
        "diversity":        metrics_msg.get("diversity", {}),
        "late_events":      metrics_msg.get("late_events", {}),
        "full_sampling":    sampling,
        "full_dgim":        dgim_queries,
    }

    return out


def print_summary(out: dict):
    """Print a human-readable summary to the console."""
    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  RUN: {out['run_id']}   window: {out['window_start']} -> {out['window_end']}")
    print(f"{sep}")
    print(f"  Partitions       : {out.get('partitions')}   "
          f"Stage1 consumers: {out.get('stage1_consumers')} ({out.get('stage1_active_consumers')} active)   "
          f"Stage2 consumers: {out.get('stage2_consumers')} ({out.get('stage2_active_consumers')} active)")
    print(f"  Events processed : {out['events_processed']:>10,}")
    print(f"  Producer EPS     : {out['producer_eps']:>10.1f}")
    print(f"  Stage2 EPS       : {out['stage2_eps']:>10.1f}")
    print(f"  Consumer lag     : {out['consumer_lag']:>10}")
    print(f"  Latency p50/p95  : {out['latency_p50_ms']} ms / {out['latency_p95_ms']} ms")
    print(f"  Partition skew   : {out.get('partition_skew', 0.25):.4f}")
    b = out['bloom']
    print(f"  Bloom FP / FN    : {b['fp_rate']} / {b['fn_rate']}")
    print(f"  HLL  exact={out['hll'].get('exact_routes','?')}  est={out['hll'].get('estimate','?')}  err={out['hll'].get('error_pct','?')}%")
    dgim = out['dgim']
    print(f"  DGIM k=5000 exact={dgim['exact_ones']}  est={dgim['estimate']}  err={dgim['error_pct']}%")
    s = out['sampling']
    print(f"  Sampling (k=1000) exact_fare={s.get('exact_mean_fare','?')}  est={s.get('sample_mean_fare','?')}  err={s.get('error_pct','?')}%")
    lsh = out['lsh']
    print(f"  LSH (20x5) candidates={lsh['candidates']}  true_pairs={lsh['true_pairs']}  recall={lsh['recall']}  reduction={lsh['candidate_reduction']}")
    alt = lsh.get('alt_10x10', {})
    print(f"  LSH (10x10) candidates={alt.get('candidates','?')}  recall={alt.get('recall','?')}  reduction={alt.get('candidate_reduction','?')}")
    print(f"{sep}\n")


def append_csv(out: dict):
    RESULTS_DIR.mkdir(exist_ok=True)
    if SUMMARY_CSV.exists():
        with open(SUMMARY_CSV, newline="") as f:
            old_header = next(csv.reader(f), [])
        if old_header != CSV_HEADERS:   # older file without these columns: keep it aside
            SUMMARY_CSV.replace(SUMMARY_CSV.with_name("experiment_summary_old_format.csv"))
    write_header = not SUMMARY_CSV.exists()
    with open(SUMMARY_CSV, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        sampling = out.get("sampling", {})
        lsh = out.get("lsh", {})
        hll = out.get("hll", {})
        dgim = out.get("dgim", {})
        bloom = out.get("bloom", {})
        row = {
            "run_id":                   out.get("run_id"),
            **{k: out.get(k) for k in ("partitions", "stage1_consumers", "stage1_active_consumers",
                                       "stage2_consumers", "stage2_active_consumers")},
            "window_start":             out.get("window_start"),
            "window_end":               out.get("window_end"),
            "events_processed":         out.get("events_processed"),
            "producer_eps":             out.get("producer_eps"),
            "stage2_eps":               out.get("stage2_eps"),
            "consumer_lag":             out.get("consumer_lag"),
            "latency_p50_ms":           out.get("latency_p50_ms"),
            "latency_p95_ms":           out.get("latency_p95_ms"),
            "partition_skew":           out.get("partition_skew"),
            "bloom_fp_rate":            bloom.get("fp_rate"),
            "bloom_fn_rate":            bloom.get("fn_rate"),
            "sample_mean_fare":         sampling.get("sample_mean_fare"),
            "exact_mean_fare":          sampling.get("exact_mean_fare"),
            "fare_error_pct":           sampling.get("error_pct"),
            "hll_exact":                hll.get("exact_routes"),
            "hll_estimate":             hll.get("estimate"),
            "hll_error_pct":            hll.get("error_pct"),
            "dgim_k5000_exact":         dgim.get("exact_ones"),
            "dgim_k5000_estimate":      dgim.get("estimate"),
            "dgim_k5000_error_pct":     dgim.get("error_pct"),
            "lsh_candidates":           lsh.get("candidates"),
            "lsh_true_pairs":           lsh.get("true_pairs"),
            "lsh_recall":               lsh.get("recall"),
            "lsh_candidate_reduction":  lsh.get("candidate_reduction"),
            # Table 11 compatibility
            "Rate":                     out.get("run_id"),
            "Produced_per_s":           out.get("producer_eps"),
            "Processed_per_s":          out.get("stage2_eps"),
            "p50_latency":              out.get("latency_p50_ms"),
            "p95_latency":              out.get("latency_p95_ms"),
            "max_lag":                  out.get("consumer_lag"),
            "HLL_err":                  hll.get("error_pct"),
            "DGIM_err":                 dgim.get("error_pct"),
            "LSH_recall":               lsh.get("recall"),
        }
        writer.writerow(row)


def run(cfg: dict, group_id: str = "final-consumer-group"):
    kafka_cfg = cfg["kafka"]
    default_rate = float(cfg.get("producer", {}).get("rate", 1000))
    RESULTS_DIR.mkdir(exist_ok=True)

    consumer = KafkaConsumer(
        kafka_cfg["topics"]["metrics"],
        kafka_cfg["topics"]["lsh"],
        bootstrap_servers=kafka_cfg["bootstrap_servers"],
        group_id=group_id,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=120_000,
    )

    # Real partition count of taxi.raw, read from Kafka (not from config / CLI)
    num_partitions = len(consumer.partitions_for_topic(kafka_cfg["topics"]["raw"]) or [])
    # stage -> {process instance -> assigned partitions}, from consumer_stats reports
    assignments = {"stage1": {}, "stage2": {}}

    def parallelism() -> dict:
        out = {"partitions": num_partitions}
        for stage, inst in assignments.items():
            out[f"{stage}_consumers"] = len(inst)
            out[f"{stage}_active_consumers"] = sum(1 for p in inst.values() if p)
        return out

    # Buffer: window_start → {metrics, lsh}
    buffer: dict = {}
    latest_producer: dict = {}
    latest_bloom: dict = {}

    log.info("Final consumer running. Reading %s + %s",
             kafka_cfg["topics"]["metrics"], kafka_cfg["topics"]["lsh"])

    for msg in consumer:
        data = msg.value
        topic = msg.topic

        # Check for sideband metrics messages on taxi.metrics
        if topic == kafka_cfg["topics"]["metrics"]:
            msg_type = data.get("type")
            if msg_type == "producer_stats":
                latest_producer = data
                log.info("Received producer_stats: eps=%.1f  skew=%.3f",
                         data.get("producer_eps", 0), data.get("partition_skew", 0))
                continue
            elif msg_type == "consumer_stats":
                assignments.setdefault(data["stage"], {})[data["instance"]] = data["partitions"]
                log.info("consumer_stats: %s %s -> partitions %s",
                         data["stage"], data["instance"], data["partitions"])
                continue
            elif msg_type == "bloom_stats":
                latest_bloom = data
                log.info("Received bloom_stats: FP=%.4f  FN=%.4f  items=%d",
                         data.get("fp_rate", 0), data.get("fn_rate", 0), data.get("inserted_ids", 0))
                continue

        wk = data.get("window_start", "unknown")

        if wk not in buffer:
            buffer[wk] = {"metrics": None, "lsh": None}

        if topic == kafka_cfg["topics"]["metrics"]:
            buffer[wk]["metrics"] = data
        elif topic == kafka_cfg["topics"]["lsh"]:
            buffer[wk]["lsh"] = data

        # Once we have both pieces, emit
        if buffer[wk]["metrics"] and buffer[wk]["lsh"]:
            out = build_output(buffer[wk]["metrics"], buffer[wk]["lsh"],
                               latest_bloom, latest_producer, default_rate, parallelism())
            print_summary(out)

            with open(RESULTS_JSONL, "a") as f:
                f.write(json.dumps(out) + "\n")
            append_csv(out)
            log.info("Appended result for window %s to results.jsonl", wk)
            del buffer[wk]

    # Flush remaining partial results (metrics without LSH or vice versa)
    for wk, parts in buffer.items():
        if parts["metrics"]:
            out = build_output(parts["metrics"], parts.get("lsh") or {},
                               latest_bloom, latest_producer, default_rate, parallelism())
            print_summary(out)
            with open(RESULTS_JSONL, "a") as f:
                f.write(json.dumps(out) + "\n")
            append_csv(out)

    log.info("Final consumer finished. Results saved to %s", RESULTS_JSONL)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Final Consumer")
    parser.add_argument("--config",   default="config.yaml")
    parser.add_argument("--group-id", default="final-consumer-group")
    args = parser.parse_args()
    cfg = load_config(args.config)
    run(cfg, group_id=args.group_id)
