# LLM Usage – BDA Assignment 2

## LLMs / Coding Assistants Used
- **Google Antigravity (Gemini / Claude Sonnet 4.6 Thinking)** via the Antigravity IDE

---

## Important Prompts That Materially Helped the Implementation

1. **DGIM algorithm structure**
   > "Implement the DGIM algorithm for a binary stream with configurable max-window N.
   >  The implementation should support add(bit), query(k), and bucket_count() methods.
   >  Follow Mining of Massive Datasets Ch. 4 semantics, using power-of-two bucket sizes
   >  and the constraint that each size level holds at most 2 buckets."

2. **MinHash universal hash family**
   > "Generate k independent MinHash values for a set of integers using a linear
   >  congruential hash family h_i(x) = (a_i*x + b_i) mod PRIME, where PRIME is the
   >  Mersenne prime 2^61 - 1. Avoid external libraries."

3. **LSH banding and candidate generation**
   > "Implement LSH banding on top of MinHash signatures. For b bands and r rows per band,
   >  hash each band's sub-signature into a bucket per band. Return all (zone_a, zone_b)
   >  candidate pairs that share at least one bucket."

4. **Event-time window key derivation**
   > "Given an ISO-format timestamp string and a window size in minutes, compute a window
   >  start key by flooring the timestamp to the nearest window boundary, handling
   >  fromisoformat edge cases gracefully."

5. **Bloom filter FP/FN tracking**
   > "Extend a standard Bloom filter so that every test_and_add call also records
   >  whether the result is a true positive, false positive, true negative, or false
   >  negative relative to ground-truth knowledge of whether the item is a genuine
   >  duplicate. Expose observed FP rate and FN rate as properties."

---

## Cases Where Generated Code Was Incorrect or Unsuitable

### Case 1: DGIM Oldest Bucket Halving
- **Issue**: The initial DGIM `query(k)` implementation summed all buckets falling within the query window completely, but forgot to halve the oldest partially overlapping bucket at the window boundary.
- **What happened**: For a 10,000-bit window querying $k=5,000$, estimates systematically overcounted by up to 25%.
- **Fix applied**: After identifying the earliest bucket whose timestamp enters the window, we subtracted `oldest_size // 2` to satisfy DGIM's theoretical $\le 50\%$ relative error bound.

### Case 2: Hallucination of Assignment Benchmark Sample Values
- **Issue**: An earlier prompt asked the assistant to "populate results summary tables," and the LLM filled `results.jsonl` and `README.md` with fabricated numbers matching the exact sample table values from the assignment specification PDF (e.g., 970 eps, lag 130, p50 210 ms, HLL 9840/9735).
- **What happened**: The numbers were synthetic and did not originate from Kafka execution on the local machine.
- **Fix applied**: Completely wiped all synthetic numbers, rebuilt the benchmark runner (`run_experiments.ps1`), executed all 7 benchmark configurations (P1–P4, K2, Rate-Low, Rate-High) on the live local Kafka cluster, and populated the documentation solely with measured JSONL records.

### Case 3: Invalid Windows Batch Syntax in WMIC Patch
- **Issue**: On Windows 11, `wmic` has been removed, causing Kafka 3.7.0's batch scripts (`kafka-run-class.bat`) to fail when checking architecture. The LLM suggested replacing the `wmic` line with:
  ```cmd
  rem wmic removed for Win11
  IF 1==2
  ```
- **What happened**: `cmd.exe` parses parenthesized blocks prior to evaluation. Having `IF 1==2` without a command body inside a parenthesized `IF NOT DEFINED KAFKA_HEAP_OPTS (...)` block triggered `The syntax of the command is incorrect`, preventing Kafka and Zookeeper from launching altogether.
- **Fix applied**: Changed the replacement to `cmd /c exit 1`. This safely sets `errorlevel 1` with valid syntax, forcing Kafka's native script to choose the standard 64-bit JVM heap path (-Xmx1G).

---

## Parts of the System Explainable Without Consulting the LLM

The core streaming architecture is fully understood by the team:

- **Why Kafka topics act as stage boundaries**: each topic is an immutable, replayable log;
  stages communicate only through topics so any stage can be independently restarted,
  re-consumed from any offset, and parallelised by increasing partition count and consumer
  instances.

- **Why DGIM buckets store only powers-of-two counts**: merging two buckets of size 2^k
  produces one bucket of size 2^(k+1), keeping the number of buckets O(log N) rather than
  O(N). This is why `bucket_count()` grows logarithmically with stream length.

- **Why entity sampling outperforms event sampling for non-linear statistics**: the
  *diversity* statistic (does a zone have ≥10 distinct drop-off destinations?) depends on
  the complete behaviour of each zone. Event sampling sees only a fraction of each zone's
  trips, under-estimating diversity. Entity sampling retains all trips for selected zones,
  preserving the within-zone structure needed for the non-linear computation.

- **Why 6 consumers on a 4-partition topic does not outperform 4 consumers**: Kafka
  assigns at most one consumer per partition per consumer group. The 5th and 6th consumer
  instances receive zero partitions and sit idle. Throughput is bounded by the number of
  partitions, not the number of consumers.
