# Task 5.2 – Restart & Recovery (Live Kafka Run)

All files in this folder come from one live run of `python scripts/run_restart_demo_live.py`
(Kafka 3.7 on `127.0.0.1:9092`, 4 partitions, producer 300 eps requested / ~230–250 eps actual, key = `pickup_zone`).
Each `stepN_*.txt` file is the unedited output of `demo_task5.py`, which reads committed and end offsets
of consumer group `stage2-group` on topic `taxi.clean` from Kafka.

## Procedure
1. Start Zookeeper, Kafka, create topics, start Stage 1, Stage 2, Stage 3, final consumer and producer.
2. Let the pipeline run for 45 s, then kill the Stage 2 process (simulated crash).
3. Measure lag immediately (step 1) and again after 15 s while the producer keeps sending (step 2).
4. Restart Stage 2 with the same `--group-id stage2-group`, wait 75 s, and measure again (step 3).

## Measured results

| Step | File | Committed offsets (P0 / P1 / P2 / P3) | Total lag |
|---|---|---|---|
| 1. Right after kill | `step1_lag_after_kill.txt` | 2026 / 2732 / 1781 / 1619 | 1,471 |
| 2. 15 s later, Stage 2 down | `step2_lag_growing.txt` | 2026 / 2732 / 1781 / 1619 (frozen) | 5,280 |
| 3. 75 s after restart | `step3_lag_draining.txt` | 5980 / 8165 / 5090 / 5451 | 0 |

Partition 1 holds the largest share of the backlog (1,841 of 5,280 ≈ 35%), consistent with the ~0.33 skew of `pickup_zone` keying.

## Observations from the Stage 2 log during this run
- **Re-join delay:** the restarted Stage 2 joined the group ~25 s after starting (22:26:32 → 22:26:57). The crashed member never left the group cleanly, so the coordinator waited for its session timeout before rebalancing.
- **Offsets survived:** after joining, Stage 2 continued from the committed offsets above; no message on `taxi.clean` was lost, and lag fell back to single digits within one window (window 02:00 closed with lag = 6).
- **State was lost:** the window that was open during the crash (2024-01-01 01:00) was emitted with only **2,011 events**, compared with ~6,000+ for a complete hour. Its pre-crash in-memory aggregates were gone.
- **Late-event side effect:** while catching up on the backlog, the watermark advanced quickly, so several backlog events (e.g. `2024-01-row-00009018`, time 01:19:18) were discarded as late.

## Recovery semantics
- **At-least-once delivery:** offsets are committed to Kafka (`__consumer_offsets`), so a restarted consumer resumes from the last committed position.
- **In-memory state is not recovered:** open windows, DGIM buckets and reservoir samples restart from zero.
- **Possible duplicates:** events processed after the last auto-commit (~5 s interval) and before the crash are delivered again and counted twice.
- **Bloom filter does not help here:** it lives in Stage 1 (`taxi.raw` → `taxi.clean`); Stage 2 re-reads `taxi.clean`, where events are already marked.
