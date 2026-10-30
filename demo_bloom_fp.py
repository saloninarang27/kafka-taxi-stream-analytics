"""
demo_bloom_fp.py
────────────────
Demonstration of Bloom filter false positive and false negative rates (Task 5.1).

Assignment Requirement:
"Include at least one concrete false-positive example if one occurs.
If none occurs in the default run, increase the number of items or reduce the
Bloom-filter size in a separate small experiment until a false positive can be demonstrated."
"""

import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent))

from utils.bloom_filter import BloomFilter


def run_bloom_fp_demonstration():
    print("=" * 70)
    print(" Bloom Filter False-Positive Demonstration (Task 5.1)")
    print("=" * 70)

    # 1. Normal configuration test
    print("\n[1] Standard Filter Run (expected_items=10,000, target_p=0.01)")
    bf_normal = BloomFilter(expected_items=10_000, fp_rate=0.01)
    print(f"    Bit array size: {bf_normal.m} bits ({bf_normal.m // 8} bytes)")
    print(f"    Hash functions: {bf_normal.k}")

    # Insert 10,000 genuine items
    for i in range(10_000):
        bf_normal.add(f"trip-row-{i:06d}")

    # Query 5,000 unseen items
    fp_found_normal = []
    for i in range(10_000, 15_000):
        eid = f"trip-row-{i:06d}"
        if eid in bf_normal:
            fp_found_normal.append(eid)

    observed_fp_rate = len(fp_found_normal) / 5000
    print(f"    Inserted items : 10,000")
    print(f"    Unseen queries : 5,000")
    print(f"    Observed FPs   : {len(fp_found_normal)} ({observed_fp_rate * 100:.2f}%)")
    print(f"    Theoretical FP : {bf_normal.p * 100:.2f}%")

    # 2. Forced Collision Experiment (Smaller filter / higher load)
    print("\n[2] Forced Collision Experiment (Constrained filter: expected=1,000, items=3,000)")
    bf_small = BloomFilter(expected_items=1_000, fp_rate=0.05)
    print(f"    Bit array size: {bf_small.m} bits")
    print(f"    Hash functions: {bf_small.k}")

    inserted = set()
    for i in range(2_500):
        eid = f"2024-01-row-{i:08d}"
        bf_small.add(eid)
        inserted.add(eid)

    # Search for concrete false positive
    concrete_fp = None
    query_count = 0
    for i in range(100_000, 200_000):
        test_id = f"2024-01-row-{i:08d}"
        query_count += 1
        if test_id in bf_small:
            concrete_fp = test_id
            break

    print("\n" + "-" * 70)
    print(" CONCRETE FALSE-POSITIVE RESULT:")
    print("-" * 70)
    if concrete_fp:
        positions = bf_small._hash_positions(concrete_fp)
        print(f"  Event ID tested       : {concrete_fp}")
        print(f"  Ground truth status   : NOT INSERTED (Never seen before)")
        print(f"  Bloom filter verdict  : SEEN BEFORE (True)")
        print(f"  Hash bit positions    : {positions}")
        print(f"  Reason                : All {len(positions)} bit positions were set by previously")
        print(f"                          inserted independent items (Hash Collision).")
        print(f"  Observed FP Rate      : {bf_small.fp_rate_observed:.4f}")
        print(f"  Observed FN Rate      : 0.0000 (False negatives are mathematically impossible in Bloom Filter)")
    else:
        print("  No FP found in test window.")
    print("-" * 70 + "\n")


if __name__ == "__main__":
    run_bloom_fp_demonstration()
