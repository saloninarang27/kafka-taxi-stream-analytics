"""
make_run_summary.py
Builds results/run_summary.csv : ONE row per experiment
(P1, P2, P3, P4, K1, K2, Rate-Low, Rate-Medium, Rate-High).
Restart demo -> results/restart_demo/ ; Bloom false-positive demo -> python demo_bloom_fp.py
K1 and Rate-Medium reuse the P3 run (identical configuration).

Inputs (already produced by run_experiments.ps1, nothing is re-run):
  results/log_<RUN>.txt        -> producer / Stage 1 / Stage 2 / final-consumer log lines
  results/results_<RUN>.jsonl  -> per-window summaries written by final_consumer.py

results/experiment_summary.csv stays as it is (one row per 60-min window).
Usage (from the P2 folder):  python scripts/make_run_summary.py
"""
import ast
import csv
import json
import os
import re
import statistics as st

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "results")

# Same settings as the $runs table in run_experiments.ps1
RUNS = [
    ("P1",        1, 1, 500,    "pickup_zone", "1 partition, 1 consumer"),
    ("P2",        4, 1, 500,    "pickup_zone", "4 partitions, 1 consumer"),
    ("P3",        4, 4, 500,    "pickup_zone", "4 partitions, 4 consumers (default)"),
    ("P4",        4, 6, 500,    "pickup_zone", "4 partitions, 6 consumers (2 idle)"),
    ("K2",        4, 4, 500,    "none",        "no key -> round-robin"),
    ("Rate-Low",  4, 4, 200,    "pickup_zone", "low rate"),
    ("Rate-High", 4, 4, 100000, "pickup_zone", "high rate (as fast as possible)"),
]


def read_log(run):
    path = os.path.join(RES, f"log_{run}.txt")
    raw = open(path, "rb").read()
    for enc in ("utf-16", "utf-8-sig", "utf-8"):
        try:
            return raw.decode(enc)
        except UnicodeError:
            pass
    return raw.decode("utf-8", errors="ignore")


def mean(xs):
    xs = list(xs)
    return round(st.mean(xs), 4) if xs else ""


def summarise(run, parts, cons, rate, key, note):
    log = read_log(run)
    W = [json.loads(l) for l in open(os.path.join(RES, f"results_{run}.jsonl"), encoding="utf-8") if l.strip()]

    # Producer
    m = re.search(r"Total events sent: (\d+)\s*\|\s*Final EPS: ([\d.]+)\s*\|\s*Final skew: ([\d.]+)", log)
    sent, prod_eps, skew = (int(m.group(1)), float(m.group(2)), float(m.group(3))) if m else ("", "", "")
    peaks = [float(x) for x in re.findall(r"\[PRODUCER\] INFO Throughput: ([\d.]+) eps", log)]
    peak = max(peaks) if peaks else ""

    # Stage 1 (one line per consumer)
    s1 = re.findall(r"Stage 1 done\. Total processed: (\d+) \| Bloom: (\{.*\})", log)
    s1_counts, inserted, fp, fn = [], 0, [], []
    for cnt, d in s1:
        s1_counts.append(int(cnt))
        try:
            b = ast.literal_eval(d)
            inserted += b.get("inserted_ids", 0)
            fp.append(b.get("fp_rate_observed", 0.0))
            fn.append(b.get("fn_rate_observed", 0.0))
        except Exception:
            pass
    s1_total = sum(s1_counts)

    # Stage 2 (one line per consumer)
    s2 = re.findall(r"Stage 2 finished\. Total events consumed: (\d+) \| Run EPS: ([\d.]+) \| Late: (\d+)", log)
    s2_counts = [int(c) for c, _, _ in s2]
    s2_eps = round(sum(float(e) for _, e, _ in s2), 1)
    late = sum(int(l) for _, _, l in s2)

    # Final-consumer window prints (lag, latency)
    lags = [int(x) for x in re.findall(r"Consumer lag\s*:\s*(\d+)", log)]
    lat = [(int(a), int(b)) for a, b in re.findall(r"Latency p50/p95\s*:\s*(\d+) ms / (\d+) ms", log)]

    # Algorithms (per-window jsonl)
    lsh_w = [w for w in W if w["lsh"].get("true_pairs", 0) > 0]
    alt_w = [w for w in lsh_w if "alt_10x10" in w["lsh"]]
    last = W[-1] if W else {}
    fs = last.get("full_sampling", {})

    def res_err(k):
        v = fs.get(str(k), {})
        return v.get("fare_error_pct", "")

    return {
        "run_id": run,
        # measured values from the run when present (older result files: table above)
        "partitions": last.get("partitions", parts),
        "consumers_per_stage": last.get("stage2_consumers", cons),
        "partition_key": key,
        "requested_rate": rate,
        "events_sent": sent,
        "producer_eps": prod_eps,
        "producer_peak_eps": peak,
        "partition_skew": skew,
        "stage1_events": s1_total,
        "stage1_active_consumers": sum(1 for c in s1_counts if c > 0),
        "stage1_load_min_max": f"{min(s1_counts)}-{max(s1_counts)}" if s1_counts else "",
        "stage2_events": sum(s2_counts),
        "stage2_run_eps_total": s2_eps,
        "stage2_load_min_max": f"{min(s2_counts)}-{max(s2_counts)}" if s2_counts else "",
        "windows": len(W),
        "lag_avg": mean(lags),
        "lag_max": max(lags) if lags else "",
        "latency_p50_ms_avg": mean(a for a, _ in lat),
        "latency_p95_ms_avg": mean(b for _, b in lat),
        "duplicates_detected": s1_total - inserted if s1 else "",
        "bloom_fp_rate": max(fp) if fp else "",
        "bloom_fn_rate": max(fn) if fn else "",
        "late_events": late,
        "reservoir_k100_fare_err_pct": res_err(100),
        "reservoir_k1000_fare_err_pct": res_err(1000),
        "reservoir_k10000_fare_err_pct": res_err(10000),
        "hll_err_pct_avg": mean(w["hll"]["error_pct"] for w in W),
        "dgim_k5000_err_pct_avg": mean(w["dgim"]["error_pct"] for w in W),
        "diversity_exact_avg": mean(w["diversity"]["exact_fraction"] for w in W),
        "diversity_event_sample_avg": mean(w["diversity"]["event_sample_fraction"] for w in W),
        "diversity_entity_sample_avg": mean(w["diversity"]["entity_sample_fraction"] for w in W),
        "lsh_20x5_recall_avg": mean(w["lsh"]["recall"] for w in lsh_w),
        "lsh_20x5_reduction_avg": mean(w["lsh"]["candidate_reduction"] for w in lsh_w),
        "lsh_10x10_recall_avg": mean(w["lsh"]["alt_10x10"]["recall"] for w in alt_w),
        "lsh_10x10_reduction_avg": mean(w["lsh"]["alt_10x10"]["candidate_reduction"] for w in alt_w),
        "notes": note,
    }


def add_alias_rows(rows):
    """K1 and Rate-Medium use exactly the P3 configuration (4 partitions, 4 consumers,
    key=pickup_zone, 500 eps requested), so they reuse the P3 run instead of a duplicate run."""
    by_id = {r["run_id"]: r for r in rows}
    cols = list(rows[0].keys())
    out = []
    for r in rows:
        out.append(r)
        if r["run_id"] == "P4" and "P3" in by_id:
            k1 = dict(by_id["P3"], run_id="K1", notes="key=pickup_zone; same configuration as P3, so the P3 run is reused")
            out.append(k1)
        if r["run_id"] == "Rate-Low" and "P3" in by_id:
            med = dict(by_id["P3"], run_id="Rate-Medium", notes="medium rate (500 requested); same configuration as P3, so the P3 run is reused")
            out.append(med)
    return out


def main():
    rows = []
    for r in RUNS:
        if not os.path.exists(os.path.join(RES, f"log_{r[0]}.txt")):
            print(f"[skip] {r[0]}: log file not found")
            continue
        rows.append(summarise(*r))
        print(f"[ok]   {r[0]}")
    rows = add_alias_rows(rows)
    out = os.path.join(RES, "run_summary.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    print(f"Written {out} ({len(rows)} runs)")


if __name__ == "__main__":
    main()
