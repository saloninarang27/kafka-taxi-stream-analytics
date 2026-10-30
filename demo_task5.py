"""
demo_task5.py
-------------
Real demonstration of Task 5: Event Time, Delayed Events, and Failure Recovery.

Covers:
  8.1 Delayed / Out-of-Order Events:
      - Watermark policy and event-time windowing
      - Concrete late event example with real WindowState computation
  8.2 Restart / Recovery:
      - Actually measures Kafka consumer lag via KafkaConsumer.end_offsets()
      - Shows committed vs end offset, then simulates a "resumed" consumer

Run:
    python demo_task5.py [--config config.yaml]

Kafka must be running (start with: python setup/setup_kafka.py --skip-start)
"""

import sys
import time
import random
import pathlib
import argparse
from datetime import datetime, timedelta

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from utils.config import load_config
from stage2_stream_analytics.stage2 import (
    WindowState,
    get_window_bounds,
    parse_event_time,
)


# ─────────────────────────────────────────────────────────────────────────────
# Helper: measure real Kafka consumer lag
# ─────────────────────────────────────────────────────────────────────────────

def measure_lag_real(bootstrap_servers: str, topic: str, group_id: str) -> dict:
    """
    Returns {partition: (committed_offset, end_offset, lag)} for each partition.
    Uses KafkaConsumer with same group_id so we read the committed offsets.
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.structs import OffsetAndMetadata

        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            consumer_timeout_ms=2000,
            auto_offset_reset="earliest",
            request_timeout_ms=3000,
            connections_max_idle_ms=4000,
        )

        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            consumer.close()
            return {}

        tps = [TopicPartition(topic, p) for p in sorted(partitions)]
        consumer.assign(tps)
        consumer.poll(timeout_ms=500)  # trigger partition assignment

        end_offsets = consumer.end_offsets(tps)
        result = {}
        for tp in tps:
            committed = consumer.committed(tp)
            committed_off = committed if isinstance(committed, int) else (committed.offset if committed else 0)
            end_off = end_offsets.get(tp, 0)
            lag = max(0, end_off - committed_off)
            result[tp.partition] = {
                "committed": committed_off,
                "end_offset": end_off,
                "lag": lag,
            }
        consumer.close()
        return result
    except Exception as exc:
        return {"error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# 8.1  Delayed / Out-of-Order Events — real WindowState computation
# ─────────────────────────────────────────────────────────────────────────────

def demo_delayed_events(cfg: dict):
    print("\n" + "=" * 70)
    print(" [8.1] DELAYED / OUT-OF-ORDER EVENT HANDLING")
    print("=" * 70)

    win_mins     = int(cfg.get("window_minutes", 60))
    max_delay    = int(cfg["producer"].get("max_delay_seconds", 20))
    allowed_late = timedelta(seconds=max_delay + 5)

    # Synthetic event stream: first 36 events in window-0 (10:00-11:00),
    # then 10 events jump to window-1 (11:01+) to advance watermark past window-0.
    events = []
    base    = datetime(2024, 1, 1, 10, 0, 0)
    base_w0 = base
    base_w1 = base + timedelta(hours=1, minutes=1)  # 11:01 = firmly in window-1

    for i in range(36):
        t = base_w0 + timedelta(minutes=i * 1.5)   # up to 10:52:30
        events.append({
            "event_id":       f"2024-01-demo-{i:04d}",
            "event_time":     t.isoformat(),
            "pickup_zone":    random.randint(1, 263),
            "dropoff_zone":   random.randint(1, 263),
            "route_key":      f"{random.randint(1,263)}-{random.randint(1,263)}",
            "fare_amount":    round(random.uniform(5, 60), 2),
            "trip_distance":  round(random.uniform(0.5, 20), 2),
            "slow_trip_bit":  random.randint(0, 1),
            "producer_ts_ms": int(time.time() * 1000),
        })

    # Events in window-1: advances watermark beyond window-0 boundary
    for i in range(10):
        t = base_w1 + timedelta(minutes=i * 2)
        events.append({
            "event_id":       f"2024-01-w1-{i:04d}",
            "event_time":     t.isoformat(),
            "pickup_zone":    random.randint(1, 263),
            "dropoff_zone":   random.randint(1, 263),
            "route_key":      f"{random.randint(1,263)}-{random.randint(1,263)}",
            "fare_amount":    round(random.uniform(5, 60), 2),
            "trip_distance":  round(random.uniform(0.5, 20), 2),
            "slow_trip_bit":  random.randint(0, 1),
            "producer_ts_ms": int(time.time() * 1000),
        })

    # Add two deliberately late events (belong to window-0 but arrive after window-1 starts)
    late_events = [
        {
            "event_id":    "LATE-EVENT-A",
            "event_time":  (base + timedelta(minutes=30)).isoformat(),  # window-0 time
            "pickup_zone": 161,
            "dropoff_zone": 236,
            "route_key":   "161-236",
            "fare_amount": 22.50,
            "trip_distance": 3.8,
            "slow_trip_bit": 0,
            "producer_ts_ms": int(time.time() * 1000),
        },
        {
            "event_id":    "LATE-EVENT-B",
            "event_time":  (base + timedelta(minutes=55)).isoformat(),  # window-0 time
            "pickup_zone": 79,
            "dropoff_zone": 140,
            "route_key":   "79-140",
            "fare_amount": 14.75,
            "trip_distance": 2.1,
            "slow_trip_bit": 1,
            "producer_ts_ms": int(time.time() * 1000),
        },
    ]

    # Simulate the watermark-based windowing logic (mirrors stage2.run())
    windows: dict = {}
    closed_windows: set = set()
    window_end_dts: dict = {}
    max_event_time: datetime | None = None
    late_count = 0
    late_samples = []

    all_msgs = events + late_events  # late events appended at the end

    for ev in all_msgs:
        dt = parse_event_time(ev["event_time"])
        if not dt:
            continue

        # Update watermark
        if max_event_time is None or dt > max_event_time:
            max_event_time = dt
        watermark = max_event_time - allowed_late

        w_start, w_end, _, end_dt = get_window_bounds(dt, win_mins)

        # Late event check
        if w_start in closed_windows:
            late_count += 1
            sample = {
                "event_id":   ev["event_id"],
                "event_time": ev["event_time"],
                "window":     w_start,
                "watermark":  watermark.isoformat(),
                "action":     "DISCARDED",
            }
            late_samples.append(sample)
            print(f"\n  [LATE EVENT DETECTED]")
            print(f"    event_id    : {ev['event_id']}")
            print(f"    event_time  : {ev['event_time']}  (belongs to window {w_start})")
            print(f"    watermark   : {watermark.isoformat()}")
            print(f"    window_end  : {w_end}")
            print(f"    action      : DISCARDED (window already closed)")
            continue

        if w_start not in windows:
            windows[w_start] = WindowState(w_start, w_end, run_id="Demo-Task5")
            window_end_dts[w_start] = end_dt
            print(f"  [WINDOW OPENED] {w_start} -> {w_end}")

        windows[w_start].ingest(ev)

        # Close any windows where watermark >= window_end
        to_close = [k for k, wd in window_end_dts.items() if watermark >= wd]
        for wk in to_close:
            win = windows[wk]
            summary = win.finalize(cfg=cfg, consumer_lag=0,
                                   dgim_summary={}, reservoir_summary={},
                                   late_events_summary={})
            print(f"  [WINDOW CLOSED]  {wk}  events={summary['events_processed']}  "
                  f"HLL_err={summary['hll']['error_pct']}%")
            closed_windows.add(wk)
            del windows[wk]
            del window_end_dts[wk]

    # Flush remaining
    for wk, win in windows.items():
        summary = win.finalize(cfg=cfg, consumer_lag=0,
                               dgim_summary={}, reservoir_summary={},
                               late_events_summary={})
        print(f"  [STREAM END FLUSH] {wk}  events={summary['events_processed']}")

    print(f"\n  Summary:")
    print(f"    Total events processed   : {len(events)}")
    print(f"    Late events discarded    : {late_count}")
    print(f"    Allowed lateness         : {allowed_late.total_seconds():.0f}s")
    print(f"    Watermark at stream end  : {watermark.isoformat()}")


# ─────────────────────────────────────────────────────────────────────────────
# 8.2  Real Kafka Lag Measurement (Restart Recovery Demo)
# ─────────────────────────────────────────────────────────────────────────────

def demo_restart_recovery(cfg: dict):
    print("\n" + "=" * 70)
    print(" [8.2] RESTART & FAILURE RECOVERY DEMO (Real Kafka Lag Measurement)")
    print("=" * 70)

    servers = cfg["kafka"]["bootstrap_servers"]
    clean_topic = cfg["kafka"]["topics"]["clean"]
    group_id    = "stage2-group"

    print(f"\n  Kafka:       {servers}")
    print(f"  Topic:       {clean_topic}")
    print(f"  Group:       {group_id}")
    print()

    print("  Measuring current consumer lag on taxi.clean ...")
    lag_data = measure_lag_real(servers, clean_topic, group_id)

    if "error" in lag_data:
        print(f"  [WARNING] Could not connect to Kafka: {lag_data['error']}")
        print("  Start Kafka first:  python setup/setup_kafka.py --skip-start")
        print()
        print("  How to run the restart demo manually:")
        print("    1. Run the full pipeline:  python run_pipeline.py")
        print("    2. Kill the Stage 2 window (Ctrl+C in that terminal).")
        print("    3. Wait 10-20 seconds (producer keeps sending to taxi.clean).")
        print("    4. Run this script: python demo_task5.py  -> lag > 0 is shown.")
        print("    5. Restart Stage 2: python stage2_stream_analytics/stage2.py")
        print("    6. Run this script again: python demo_task5.py  -> lag draining.")
        print("    Screenshot the three outputs for submission.")
    else:
        total_lag = sum(v["lag"] for v in lag_data.values() if isinstance(v, dict))
        print(f"  {'Part':>6} | {'Committed':>12} | {'End Offset':>12} | {'Lag':>8}")
        print(f"  {'-'*6}-+-{'-'*12}-+-{'-'*12}-+-{'-'*8}")
        for p, info in sorted(lag_data.items()):
            if isinstance(info, dict):
                print(f"  {p:>6} | {info['committed']:>12} | {info['end_offset']:>12} | {info['lag']:>8}")

        print(f"\n  Total consumer lag (all partitions): {total_lag:,} messages")

        if total_lag == 0:
            print("  Status: Stage 2 is caught up (lag = 0).")
            print("  To simulate failure: stop Stage 2, let producer run 10s,")
            print("  then re-run this script to see lag accumulate, then restart Stage 2.")
        else:
            print(f"  Status: Stage 2 is BEHIND by {total_lag:,} messages.")
            print(f"  Backlog volume: {total_lag:,} events waiting in taxi.clean.")
            print("  Restarting Stage 2 with same group_id will resume from committed offsets.")

    print("\n  Recovery Semantics (Viva-Ready Honest Answer):")
    print("  - Guarantee: At-Least-Once delivery via Kafka consumer offsets.")
    print("  - Offsets are saved in Kafka (__consumer_offsets): Stage 2 resumes from the last committed offset.")
    print("  - In-memory state is LOST on crash: open tumbling windows, DGIM bit-buckets,")
    print("    and reservoir samples are wiped to zero on process restart.")
    print("  - Effect on tumbling windows:")
    print("    * The active window at crash time loses all pre-crash data. Upon restart,")
    print("      it starts accumulating from zero, producing an INCOMPLETE / undercounted summary.")
    print("  - Effect on duplicate counting:")
    print("    * Uncommitted events (processed in the ~5s auto-commit window before crash) ARE re-delivered.")
    print("      These events get processed twice (events_processed, DGIM, and reservoir double-counting).")
    print("  - Why Bloom Filter DOES NOT protect Stage 2:")
    print("    * Bloom Filter is located in Stage 1 (taxi.raw -> taxi.clean).")
    print("    * When Stage 2 crashes and re-reads taxi.clean, those events are already written")
    print("      to taxi.clean with bloom_seen_before=False. Stage 2 has no Bloom filter of its own.")
    print("  - Conclusion: Offsets bach jaate hain, par in-memory state kho jata hai;")
    print("    isliye restart ke baad window adhoori aur uncommitted events duplicate ho sakte hain.")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Task 5 Demo: Late Events & Recovery")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    cfg = load_config(args.config)

    print("=" * 70)
    print(" TASK 5: EVENT-TIME, DELAYED EVENTS & RESTART RECOVERY DEMO")
    print("=" * 70)

    demo_delayed_events(cfg)
    demo_restart_recovery(cfg)

    print("=" * 70)
    print(" Demo complete.")
    print("=" * 70)


if __name__ == "__main__":
    main()
