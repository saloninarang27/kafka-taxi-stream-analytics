"""
run_pipeline.py
────────────────
Master orchestrator: starts all pipeline stages as subprocesses.

Usage:
    python run_pipeline.py --run-id X --partitions 3 --consumers 2 --rate 500
        --partitions N : taxi.raw / taxi.clean get N partitions (default: kafka.partitions in config.yaml)
        --consumers  M : M Stage 1 processes + M Stage 2 processes (default: kafka.consumers in config.yaml)
        Both are written to results/experiment_summary.csv as measured values
        (partitions, stage1_consumers, stage2_consumers, ..._active_consumers).
        Kafka cannot shrink a topic: to go DOWN in partitions, reset Kafka first
        (easiest: .\\run_experiments.ps1 -Partitions N -Consumers M).

    python run_pipeline.py [--config config.yaml] [--rate 1000] [--run-id P3-medium]
    python run_pipeline.py --experiment partitions  --partitions 1 --consumers 1
    python run_pipeline.py --experiment rate        --rate 3000
    python run_pipeline.py --experiment key         --key none

Experiments
───────────
  partitions   Run P1-P4 partition/consumer experiments
  key          K1 (pickup_zone) vs K2 (round-robin) partition-key experiment
  rate         Low/Medium/High producer-rate experiment
  default      (default) Run standard pipeline once
"""

import argparse
import subprocess
import sys
import time
import os
import pathlib

from kafka import KafkaConsumer
from utils.config import load_config

ROOT = pathlib.Path(__file__).parent
PYTHON = sys.executable


def ensure_partitions(cfg_path: str, partitions: int):
    """Create topics with `partitions` partitions, then confirm Kafka really has that many."""
    subprocess.run([PYTHON, str(ROOT / "setup/setup_kafka.py"), "--skip-start",
                    "--partitions", str(partitions)], cwd=str(ROOT), check=True)
    cfg = load_config(cfg_path)
    c = KafkaConsumer(bootstrap_servers=cfg["kafka"]["bootstrap_servers"])
    actual = {t: len(c.partitions_for_topic(cfg["kafka"]["topics"][t]) or [])
              for t in ("raw", "clean")}
    c.close()
    print(f">> Kafka partitions now: taxi.raw={actual['raw']}  taxi.clean={actual['clean']}")
    if any(n != partitions for n in actual.values()):
        sys.exit(f"[ERROR] Requested {partitions} partitions but Kafka has {actual}. Existing topics "
                 f"cannot shrink: reset Kafka (.\\run_experiments.ps1 -Partitions {partitions} ...).")


def run_stage(script: str, extra_args: list = None, wait: bool = False) -> subprocess.Popen:
    args = [PYTHON, str(ROOT / script)] + (extra_args or [])
    print(f">> Starting: {' '.join(args)}")
    if wait:
        subprocess.run(args, check=True)
        return None
    return subprocess.Popen(args, cwd=str(ROOT))


def wait_seconds(n: int, label: str = ""):
    print(f"  Waiting {n}s {label}")
    time.sleep(n)


def run_default(cfg_path: str, rate: int, run_id: str, key: str, consumers: int = 1, max_events: int = None):
    procs = []
    print(f">> Starting {consumers} Stage 1 consumer(s) and {consumers} Stage 2 consumer(s)")

    # Stage 1
    for i in range(consumers):
        p = run_stage("stage1_clean_bloom/stage1.py",
                      ["--config", cfg_path, "--group-id", f"stage1-group"])
        procs.append(p)

    # Stage 2 (consumers slots)
    for i in range(consumers):
        p = run_stage("stage2_stream_analytics/stage2.py",
                      ["--config", cfg_path, "--group-id", "stage2-group", "--run-id", run_id])
        procs.append(p)

    # Stage 3
    p = run_stage("stage3_lsh/stage3.py",
                  ["--config", cfg_path, "--group-id", "stage3-group", "--run-id", run_id])
    procs.append(p)

    # Final consumer
    p = run_stage("final_consumer/final_consumer.py",
                  ["--config", cfg_path, "--group-id", "final-consumer-group"])
    procs.append(p)

    wait_seconds(3, "for stages to initialize")

    # Producer (last, sends data)
    extra_producer = ["--config", cfg_path, "--rate", str(rate),
                      "--key", key, "--run-id", run_id]
    if max_events is not None:
        extra_producer += ["--max-events", str(max_events)]
    p = run_stage("producer/producer.py", extra_producer)
    procs.append(p)

    print("\n[OK] All stages started. Press Ctrl+C to stop.\n")
    try:
        for proc in procs:
            proc.wait()
    except KeyboardInterrupt:
        print("\nStopping all stages ...")
        for proc in procs:
            proc.terminate()


def main():
    parser = argparse.ArgumentParser(description="BDA Assignment 2 Pipeline Runner")
    parser.add_argument("--config",      default="config.yaml")
    parser.add_argument("--rate",        type=int, default=1000)
    parser.add_argument("--run-id",      default="P3-medium")
    parser.add_argument("--key",         default="pickup_zone",
                        help="Partition key field or 'none' for round-robin")
    parser.add_argument("--consumers",   type=int, default=None,
                        help="Parallel consumer processes for Stage 1 and Stage 2 "
                             "(default: kafka.consumers in config.yaml)")
    parser.add_argument("--partitions",  type=int, default=None,
                        help="Partitions for taxi.raw / taxi.clean "
                             "(default: kafka.partitions in config.yaml)")
    parser.add_argument("--max-events",  type=int, default=None,
                        help="Max events per run (0=unlimited, default: None -> uses config.yaml)")
    parser.add_argument("--experiment",  choices=["default","partitions","rate","key"],
                        default="default")
    args = parser.parse_args()
    kcfg = load_config(args.config)["kafka"]
    if args.consumers is None:
        args.consumers = int(kcfg.get("consumers", 1))
    if args.partitions is None:
        args.partitions = int(kcfg.get("partitions", 4))

    if args.experiment == "partitions":
        print("\n=== Running P1-P4 Partition Experiments ===\n")
        configs = [
            ("P1", 1,  1, "pickup_zone"),
            ("P2", 4,  1, "pickup_zone"),
            ("P3", 4,  4, "pickup_zone"),
            ("P4", 4,  6, "pickup_zone"),
        ]
        for (rid, parts, cons, key) in configs:
            print(f"\n--- Experiment {rid}: partitions={parts} consumers={cons} ---")
            # Ensure topics with exact partition count exist without triggering Windows delete-lock
            subprocess.run([PYTHON, "setup/setup_kafka.py",
                            "--skip-start", "--partitions", str(parts)], cwd=str(ROOT))
            run_default(args.config, args.rate, rid, key, consumers=cons,
                        max_events=args.max_events)
            wait_seconds(10, "before next experiment")

    elif args.experiment == "rate":
        print("\n=== Running Low/Medium/High Rate Experiments (4 partitions, 4 consumers) ===\n")
        rates = [("low", 200), ("medium", 1000), ("high", 3000)]
        for (label, rate) in rates:
            print(f"\n--- Rate experiment: {label} ({rate} eps) ---")
            subprocess.run([PYTHON, "setup/setup_kafka.py",
                            "--skip-start", "--partitions", "4"], cwd=str(ROOT))
            run_default(args.config, rate, f"rate-{label}", args.key, consumers=4,
                        max_events=args.max_events)
            wait_seconds(10)

    elif args.experiment == "key":
        print("\n=== Running K1/K2 Partition-Key Experiments (4 partitions, 4 consumers) ===\n")
        for (label, key) in [("K1-pickup_zone", "pickup_zone"), ("K2-round-robin", "none")]:
            print(f"\n--- Key experiment: {label} ---")
            subprocess.run([PYTHON, "setup/setup_kafka.py",
                            "--skip-start", "--partitions", "4"], cwd=str(ROOT))
            run_default(args.config, args.rate, label, key, consumers=4,
                        max_events=args.max_events)
            wait_seconds(10)

    else:
        print(f"\n=== Default Pipeline Run (partitions={args.partitions}, consumers={args.consumers}, "
              f"rate={args.rate}, key={args.key}) ===\n")
        ensure_partitions(args.config, args.partitions)
        run_default(args.config, args.rate, args.run_id, args.key,
                    consumers=args.consumers, max_events=args.max_events)


if __name__ == "__main__":
    main()
