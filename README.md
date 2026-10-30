# BDA Assignment 2 – Real-Time Taxi Stream Analytics

## How to Run (Windows 11)

### Prerequisites & Setup
- Windows 10/11, Python 3.11, Java 17 (`jdk-17.0.20.101-hotspot` — Kafka 3.7 does **not** work on Java 23+)
- Kafka 3.7.0 at `C:\kafka` (auto-downloaded by `setup/setup_kafka.py`); dataset `data/yellow_tripdata_2024-01.parquet`

```powershell
pip install -r requirements.txt
# One-time Kafka setup: store data in C:\kafka\data and advertise 127.0.0.1
(Get-Content C:\kafka\config\server.properties) -replace '^log.dirs=.*','log.dirs=C:/kafka/data/kafka-logs' | Set-Content C:\kafka\config\server.properties
(Get-Content C:\kafka\config\zookeeper.properties) -replace '^dataDir=.*','dataDir=C:/kafka/data/zookeeper' | Set-Content C:\kafka\config\zookeeper.properties
Add-Content C:\kafka\config\server.properties "advertised.listeners=PLAINTEXT://127.0.0.1:9092"
```

---

### Option A: Run ALL Experiments Automatically (Recommended)
One script manages the lifecycle (stop broker → clear `C:\kafka\data` → start Zookeeper/Kafka → create topics → run pipeline → save results):
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\run_experiments.ps1                         # all runs: P1, P2, P3, P4, K2, Rate-Low, Rate-High
.\run_experiments.ps1 -Only P1                # single run (-Only P1,P3 for multiple)
.\run_experiments.ps1 -Partitions 3 -Consumers 2   # ANY custom combo (optional -Rate 500 -Key none -RunId X)
```

**Changing partitions / consumers (demo):** one command, `.\run_experiments.ps1 -Partitions N -Consumers M`. It resets Kafka, creates `taxi.raw`/`taxi.clean` with N partitions, and starts M Stage 1 processes plus M Stage 2 processes (Stage 3 stays at 1). Every row of `results/experiment_summary.csv` then shows:

| Column | Where the value comes from |
|---|---|
| `partitions` | Kafka topic metadata of `taxi.raw`, read by the final consumer |
| `stage1_consumers` / `stage2_consumers` | number of distinct Stage 1 / Stage 2 processes (host-PID) that reported a partition assignment |
| `stage1_active_consumers` / `stage2_active_consumers` | how many of them got at least one partition; with M > N only N are active |

Each Stage 1 / Stage 2 process sends its assignment to `taxi.metrics` (`type: consumer_stats`, see `utils/consumer_stats.py`). So these columns are measured from the processes that actually ran, not copied from the command line. The final consumer also prints them for every window, and `run_pipeline.py` prints the partition count Kafka reports before it starts.
| Run | Parts | Cons | Rate (eps) | Key | Run | Parts | Cons | Rate (eps) | Key |
|---|---|---|---|---|---|---|---|---|---|
| **P1** | 1 | 1 | 500 | `pickup_zone` | **K2** | 4 | 4 | 500 | `none` (round-robin) |
| **P2** | 4 | 1 | 500 | `pickup_zone` | **Rate-Low** | 4 | 4 | 200 | `pickup_zone` |
| **P3** (=K1) | 4 | 4 | 500 | `pickup_zone` | **Rate-High** | 4 | 4 | 100000 (burst) | `pickup_zone` |
| **P4** | 4 | 6 | 500 | `pickup_zone` | *Each run:* 50k events (`config.yaml`), ~3–4 min. Results in `results/`. |

---

### Option B: Manual Run (Step-by-Step)
- **Terminal 1 (Zookeeper):**
  `& "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot\bin\java" -Xmx512M "-Dlog4j.configuration=file:C:/kafka/config/log4j.properties" "-Dkafka.logs.dir=C:/kafka/logs" -cp "C:\kafka\libs\*" org.apache.zookeeper.server.quorum.QuorumPeerMain C:\kafka\config\zookeeper.properties`
- **Terminal 2 (Kafka Broker — direct java bypasses missing `wmic`):**
  `& "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot\bin\java" -Xmx1G -Xms1G "-Dlog4j.configuration=file:C:/kafka/config/log4j.properties" "-Dkafka.logs.dir=C:/kafka/logs" -cp "C:\kafka\libs\*" kafka.Kafka C:\kafka\config\server.properties`
- **Terminal 3 (Pipeline):**
  ```powershell
  python setup/setup_kafka.py --skip-start --partitions 4
  python run_pipeline.py --run-id P3 --rate 500 --consumers 4 --key pickup_zone
  # Useful flags: --rate <N>, --consumers <N>, --key none, --max-events 10000 (0 = full dataset)
  ```
- **Resetting between runs:** Ctrl+C Terminals 1 & 2 → `Remove-Item -Recurse -Force C:\kafka\data` → restart.

### Verification, Demos & Troubleshooting
```powershell
python test_utils.py                         # algorithm unit tests
python scripts/algo_sanity_check.py          # offline validation on 50k trips
python demo_bloom_fp.py                      # Bloom filter false-positive demo
python demo_task5.py                         # late-event watermark & lag recovery demo
Get-Content results\results.jsonl -TotalCount 3
Import-Csv results\experiment_summary.csv | Format-Table run_id, window_start, events_processed, latency_p50_ms, consumer_lag
```
- *`wmic` error / syntax incorrect:* Start Kafka via the direct `java` command above.
- *`Cannot bootstrap`:* Ensure `127.0.0.1:9092` is configured and Kafka is up (`netstat -ano | findstr :9092`).
- *Kafka crashes on topic delete:* Stop Kafka, delete `C:\kafka\data`, restart (known Windows file-lock bug).

---

## Architecture & Framework

```
[Producer] -> taxi.raw (1/4P) -> [Stage 1: Clean + Bloom] -> taxi.clean (1/4P)
                                                                 ├──> [Stage 2: Analytics] -> taxi.metrics (1P) ──┐
                                                                 └──> [Stage 3: LSH]       -> taxi.lsh (1P)     ──┴─> [Final Consumer]
```
- **Framework:** Python + `kafka-python`. Low overhead, zero JVM layer beyond Kafka, full explicit control over streaming sketches, partition keys, offsets, and consumer groups.
- **Machine Specs:** Windows 11 Home, 11th Gen Intel Core i5-1155G7 @ 2.50GHz (4C/8T), 16 GB DDR4, Java 17, Python 3.11+. No Docker/WSL.

---

## Empirical Benchmark Results

> All numbers are measured from the latest local Kafka runs (11 Oct 2026, 50,000 events each, `run_experiments.ps1`). Sources: `results/results_<RUN>.jsonl`, `results/experiment_summary.csv` (one row per 60-min window), `results/run_summary.csv` (one row per experiment, built with `python scripts/make_run_summary.py`), `results/log_<RUN>.txt`.
>
> **Stage 2 throughput:** with several Stage 2 instances, each logs its own "Run EPS"; the tables show their sum. Run EPS divides events by the time from the first event until the stage shuts down (including its ~2 min idle timeout), so it understates the live rate. Lag stayed ≤ 14 in every run, i.e. consumers kept up with the producer.

### Table 11: Rate Experiment Summary (4 partitions, 4 consumers, key = pickup_zone)

| Rate | Produced/s (avg / peak) | Stage 2 Run EPS (sum) | p50 latency | p95 latency | Max lag | HLL err. | DGIM err. (k=5000) | LSH recall (20×5) |
|---|---|---|---|---|---|---|---|---|
| **Low** (200 requested) | 153.0 / 173.5 | 112.2 | 28.7 ms | 48.8 ms | 1 | 0.57% | 13.5% | 72.9% |
| **Medium** (500 requested, = P3) | 308.9 / 367.2 | 178.5 | 31.4 ms | 54.4 ms | 5 | 0.71% | 14.9% | 63.7% |
| **High** (100,000 requested, no sleep) | 749.0 / 909.0 | 277.5 | 41.1 ms | 62.1 ms | 11 | 0.67% | 10.1% | 70.6% |

- Higher input rate raised latency (p50 28.7 → 41.1 ms) and lag (1 → 11), but approximation quality did not degrade: HLL stays < 1%, DGIM 10–15% (within its 50% bound). The approximations see the same events, only later.
- The Python producer is the bottleneck on this machine (~750 eps max), so lag never grew large; visible lag was demonstrated in the restart demo (Task 5.2).

---

### P1–P4: Partition / Consumer Experiments (500 eps requested)

| Run | Parts | Cons | Produced/s | Stage 2 Run EPS (sum) | Lag (avg / max) | p50 / p95 lat | Skew |
|---|---|---|---|---|---|---|---|
| **P1** | 1 | 1 | 313.5 | 180.5 | 4.1 / 10 | 32.4 / 59.3 ms | 1.0000 |
| **P2** | 4 | 1 | 310.8 | 181.8 | 5.7 / 14 | 33.6 / 60.1 ms | 0.3349 |
| **P3** | 4 | 4 | 308.9 | 178.5 | 1.3 / 5 | 31.4 / 54.4 ms | 0.3350 |
| **P4** | 4 | 6 | 295.6 | 175.6 | 1.4 / 4 | 30.3 / 52.0 ms | 0.3350 |

- **P1 vs P2:** one partition holds 100% of the data (skew 1.0); with 4 partitions the busiest holds ~33.5%. Throughput is unchanged, since one consumer reads all partitions either way.
- **P2 vs P3:** throughput did not improve (~180 vs ~178 Stage 2 Run EPS): the producer (~310 eps) is the bottleneck and even one consumer kept up (max lag 14). Average lag dropped from 5.7 to 1.3 because each consumer handles one partition. Extra consumers only raise throughput when consumers are the bottleneck.
- **P4:** 2 of the 6 consumers in each of Stage 1 and Stage 2 processed **0 events** (log: `Total processed: 0`, `partition assignment: []`). A partition is assigned to at most one consumer per group, so 4 partitions allow at most 4 active consumers; results match P3.
- **Limitation:** in multi-consumer runs the final consumer keeps one instance's summary per window, so `results_<P3/P4/K2/Rate>.jsonl` covers ~25% of events; algorithm accuracy is therefore reported from P1.

---

### K1/K2: Partition Key Experiment (4 partitions, 4 consumers, 500 eps requested)

| Run | Key | Skew | Produced/s | Lag (avg / max) | p50 / p95 lat | Stage 1 events per consumer | Stage 2 events per consumer |
|---|---|---|---|---|---|---|---|
| **K1** (= P3) | `pickup_zone` | **0.3350** | 308.9 | 1.3 / 5 | 31.4 / 54.4 ms | 9,521 – 16,495 | 9,521 – 16,495 |
| **K2** | none (round-robin) | **0.2531** | 299.3 | 0.7 / 3 | 32.8 / 57.0 ms | 12,181 – 12,481 | 9,510 – 16,493 |

- Keying by `pickup_zone` keeps each zone's trips on one partition (needed for per-zone state) but busy zones create a hot partition (33.5% vs ideal 25%). Round-robin balances load almost perfectly (25.3%).
- **Repartition/shuffle needed?** **Yes.** In K2, Stage 1 load was balanced, but Stage 1 re-keys `taxi.clean` by `pickup_zone`, so Stage 2 load became uneven again (9.5k–16.5k). That re-keying is the shuffle required for per-zone windowed state.

---

### 5.1 Bloom Filter (Stage 1, P1 run, 49,304 events)
| Parameter | Value | Parameter | Value |
|---|---|---|---|
| Expected items / target FP | 200,000 / 1% | Hash functions | 6 |
| Bit-array size | 1,917,011 bits (~234 KB) | Inserted IDs | 48,325 (979 duplicates flagged ≈ 2.0%) |
| Observed FP / FN rate | 0.0 / 0.0 | Semantics | Duplicates are flagged (`bloom_seen_before`), never dropped. |

*No FP occurred because the filter was only ~24% loaded. `demo_bloom_fp.py` shrinks the filter (1,000 expected @5% = 6,235 bits, 4 hashes, 3,000 inserted) and shows a concrete false positive: `2024-01-row-00100001` was never inserted but is reported as seen. Its first part (filter for 10,000 items @1%) measures 49 FP in 5,000 unseen queries = 0.98%, matching the 1% target.*

### 5.2 Reservoir Sampling (stream-level, P1 run, 49,304 events)
| k | Exact Fare | Sample Fare | Error | Exact Dist | Sample Dist | Error |
|---|---|---|---|---|---|---|
| **100** | 21.50 | 20.95 | 2.58% | 3.62 | 3.73 | 3.27% |
| **1,000** | 21.50 | 21.23 | 1.25% | 3.62 | 3.61 | 0.05% |
| **10,000** | 21.50 | 21.12 | 1.78% | 3.62 | 3.54 | 2.13% |

*Errors are within ~3% for all k. In this single run k = 10,000 happened to be slightly worse than k = 1,000; one sample per k is random, and the expected error shrinks roughly as 1/√k only on average over repeated runs.*

### 5.4 HyperLogLog (distinct `route_key` per 60-min window)
- Precision $b = 12 \implies 4,096$ registers (4 KB sketch vs ~160 KB exact set). Theoretical error $1.04/\sqrt{4096} \approx 1.6\%$.
- Window `2024-01-01T01:00` (P1): exact = 2,824 routes, HLL = 2,793, error = **1.10%**. Mean error over P1 windows ≈ 0.81%.

### 5.5 DGIM (`slow_trip_bit`, last 10,000 events, end of P1 run)
| k | Exact 1s | DGIM estimate | Abs error | Relative error |
|---|---|---|---|---|
| **1,000** | 105 | 121 | 16 | 15.24% |
| **5,000** | 498 | 601 | 103 | 20.68% |
| **10,000** | 800 | 857 | 57 | 7.12% |
- All errors are within DGIM's 50% bound. **Buckets stored: 15** (reported by Stage 2 in `results_P1.jsonl`; bound ≈ 2·log₂(10,000) ≈ 28).

---

### 5.3 Event Sampling vs Entity Sampling (Non-Linear Statistic)
- **Statistic:** Fraction of active pickup zones with $\ge 10$ distinct drop-off destinations (destination diversity). 15% storage budget.
- **Results (`algo_sanity_check.py` on 50k trips):**
  - **Exact:** 0.5268 (7,340 stored pairs, 118/224 active zones)
  - **Event Sampling (15%):** 0.4059 (**23.0% error**, 3,078 stored pairs) — drops rare destinations, severely underestimating diversity.
  - **Entity Sampling (15%):** 0.5208 (**1.1% error**, 1,621 stored pairs) — hashes `PULocationID`, preserving complete graph clusters per sampled zone using half the memory.

---

### Task 3: LSH Parameter Comparison (100 Hashes, $s = 0.50$, 50k trips)
| Setting | Bands ($b$) | Rows ($r$) | Candidates | Confirmed ($J \ge 0.5$) | Recall | Precision | Reduction | Time |
|---|---|---|---|---|---|---|---|---|
| **20x5 default** | 20 | 5 | 1,293 | 1,038 | **85.3%** | 80.3% | 73.9% | 13.6 ms |
| **10x10 alt** | 10 | 10 | 115 | 109 | **9.0%** | 94.8% | **97.7%** | 2.5 ms |
*Takeaway:* $b=20, r=5$ balances high recall (85.3%) and 73.9% reduction. $b=10, r=10$ achieves 97.7% reduction but suffers extreme false negatives (9.0% recall) due to an overly steep S-curve threshold ($s \approx 0.79$).

---

### Task 5: Late Events & Failure Recovery

- **5.1 Delayed Events & Watermark:** 2% of events delayed 1–20 s. Watermark = $\max(\text{event\_time}) - 25\text{s}$. Late events for closed windows are discarded (P1: 922 of 49,304 ≈ 1.9%).
  ```
  [LATE EVENT] Event 2024-01-row-00004117 (time=2024-01-01 00:40:55) arrived after window 2024-01-01T00:00:00 closed. Watermark=2024-01-01T01:00:11. Discarded.
  ```
- **5.2 Restart & Recovery (live run, `results/restart_demo/`):** Stage 2 was killed after 45 s. Its committed offsets on taxi.clean stayed frozen (2026 / 2732 / 1781 / 1619) while lag grew from 1,471 to 5,280 messages in 15 s. After restarting with the same group_id, the consumer re-joined only after ~25 s (the group waits for the crashed member's session timeout), resumed from the committed offsets, and lag returned to 0 (committed 5980 / 8165 / 5090 / 5451). State loss was visible: the window open during the crash (01:00) was emitted with only 2,011 events instead of ~6,000. Catch-up also advanced the watermark quickly, so some backlog events were discarded as late. Delivery is at-least-once, and in-memory state (windows, DGIM, reservoir) is not recovered.
  - **Recovery Semantics:** At-least-once delivery. Offsets are committed in Kafka, so Stage 2 resumes from the last committed offset. In-memory state (open windows, DGIM, reservoir) is lost on restart: open windows at crash time produce partial summaries, and uncommitted events in the ~5s auto-commit window are re-processed. Stage 1 Bloom filter does not protect Stage 2.

---

## Running Individual Stages & Changing Config

### Start Kafka first (needed for every stage)
```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\scripts\start_kafka_local.ps1                            # deletes C:\kafka\data, starts Zookeeper + broker
python setup/setup_kafka.py --skip-start --partitions 4    # creates the 4 topics
```

### Run one stage at a time (one terminal each; start consumers first, producer last)
| Part | Command | What it does |
|---|---|---|
| Producer | `python producer/producer.py --rate 500 --max-events 5000 --run-id demo` | Parquet → `taxi.raw`, injects 2% duplicates + 2% delayed events |
| Stage 1 | `python stage1_clean_bloom/stage1.py` | `taxi.raw` → clean + `route_key`, `slow_trip_bit`, Bloom flag → `taxi.clean` |
| Stage 2 | `python stage2_stream_analytics/stage2.py --run-id demo` | Reservoir, HLL, DGIM, sampling, watermark → `taxi.metrics` |
| Stage 3 | `python stage3_lsh/stage3.py --run-id demo` | MinHash + LSH similar zones → `taxi.lsh` |
| Final consumer | `python final_consumer/final_consumer.py` | Writes `results/results.jsonl` and `results/experiment_summary.csv` |

Inspect a topic's output (e.g. after Producer + Stage 1 only):
```powershell
C:\kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server 127.0.0.1:9092 --topic taxi.clean --from-beginning --max-messages 5
```

> **Committed offsets:** if a stage already ran with the same group id, re-running it will not re-read old messages (Kafka remembers the committed offset; `auto_offset_reset=earliest` only applies to a new group). Use a new group (`--group-id stage1-demo2`) or reset Kafka.

No Kafka needed: `python test_utils.py`, `python scripts/algo_sanity_check.py`, `python demo_bloom_fp.py`.

### Config parameters — where each one is actually set
| Change | How | Notes |
|---|---|---|
| Consumers (e.g. 1 → 3) | `.\run_experiments.ps1 -Partitions 4 -Consumers 3`, or `python run_pipeline.py --run-id C3 --partitions 4 --consumers 3` | Default: `kafka.consumers` in `config.yaml`. CSV columns `stage1_consumers` / `stage2_consumers` show 3. With 4 partitions / 3 consumers one consumer gets 2 partitions (uneven load). Throughput stays ~310 eps (producer bottleneck); lag falls between P2 and P3; skew unchanged (~0.335, producer-side). |
| Partitions | `.\run_experiments.ps1 -Partitions N -Consumers M`, or `python run_pipeline.py --partitions N ...` | Default: `kafka.partitions` in `config.yaml`. Kafka cannot shrink an existing topic: `run_pipeline.py` checks the real count and stops with an error if it differs, so use `run_experiments.ps1` (it resets Kafka) when lowering N. CSV column `partitions` shows N. |
| Rate | `--rate N` flag | `run_pipeline.py` always passes `--rate` (default 1000), overriding `producer.rate` in `config.yaml`. |
| DGIM window (e.g. 12000) | `config.yaml`: `dgim.max_window: 12000`, `query_ks: [1000, 5000, 10000, 12000]` | `query()` clamps `k > N` to `N`, so add 12000 to `query_ks` *and* raise `max_window`. Results for k ≤ 10000 stay similar; bucket count grows slightly (O(log² N), bound ≈ 2·log₂12000 ≈ 27). Error always ≤ 50%. CSV only has `dgim_k5000_*`; see `results.jsonl` → `dgim.queries` or `python scripts/algo_sanity_check.py`. |
| Entity sample fraction | `config.yaml`: `entity_sample_fraction: 0.15` | Storage budget for both event and entity sampling in Stage 2. See `results.jsonl` → `sampling` / diversity fields. |
| HLL precision | `config.yaml`: `hll.precision: 12` | Registers = 2^p; standard error ≈ 1.04/√(2^p) (p=10 → 3.25%, p=14 → 0.81%). CSV column `hll_error_pct`. |

### Reading the result CSV
- `latency_p50_ms` / `latency_p95_ms`: Stage 2 arrival time − `producer_ts_ms`, sorted per window; median and 95th percentile. P1: 32 / 59 ms; Rate-High: 41 / 62 ms.

### Concept FAQ
- **Why Kafka?** Decouples producer and consumers, partitions give parallelism, consumer groups share load, committed offsets allow resume after a crash (at-least-once), and data is durable and replayable.
- **Why a Bloom filter?** Detects duplicate `event_id`s in ~234 KB for 200k IDs instead of storing the full set. No false negatives, ~1% false positives. m = −n·ln p / (ln 2)², k = (m/n)·ln 2. Duplicates are flagged, not dropped.
- **DGIM:** counts 1s in the last N bits using O(log² N) memory. Buckets of size 2ⁱ, at most 2 per size; a third triggers a merge of the two oldest. Half of the oldest bucket is counted, so error ≤ 50%.
- **HLL:** distinct count with b = 12 → 4,096 registers (4 KB), error ≈ 1.04/√m ≈ 1.6%.
- **Partitions vs consumers:** a partition goes to at most one consumer per group, so 6 consumers on 4 partitions leaves 2 idle (P4 log: `Total processed: 0`).

---

## Known Limitations
1. **Multi-Consumer Window Aggregation:** In P3/P4, each consumer emits independent window summaries; final consumer stores the latest received summary per window.
2. **In-Memory Sketch Reset:** DGIM, HLL, Bloom, and Reservoir state are kept in memory and reset on process restarts.
3. **Windows File Locks on Topic Deletion:** Deleting topics while Kafka runs can intermittently lock files on Windows; broker restart + data directory wipe cleanly clears them.
