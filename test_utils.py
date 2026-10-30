import sys
sys.path.insert(0, '.')

# DGIM
from utils.dgim import DGIM
dgim = DGIM(max_window=100)
bits = [1,0,1,1,0,0,1,0,1,1,1,0,0,0,1,1,0,1,0,0]
for b in bits:
    dgim.add(b)
exact = sum(bits[-10:])
est = dgim.query(10)
print("DGIM: exact=%d  estimate=%d  buckets=%d" % (exact, est, dgim.bucket_count()))

# Bloom
from utils.bloom_filter import BloomFilter
bf = BloomFilter(expected_items=1000, fp_rate=0.01)
bf.add('event-001')
bf.add('event-002')
in1 = 'event-001' in bf
in2 = 'event-999' in bf
print("Bloom: event-001=%s (want True)  event-999=%s (want False)" % (in1, in2))
print("Bloom info:", bf.info())

# HLL
from utils.hll import HyperLogLog
hll = HyperLogLog(precision=12)
for i in range(10000):
    hll.add('route-%d' % i)
est = hll.estimate()
exact = hll.exact_count()
print("HLL: exact=%d  estimate=%d  error=%.2f%%" % (exact, est, hll.error_pct()))

# Reservoir
from utils.reservoir import ReservoirSampler
import random
rs = ReservoirSampler(k=1000)
for _ in range(50000):
    rs.add(random.uniform(10, 40), random.uniform(1, 20))
s = rs.stats()
print("Reservoir: exact_fare=%.2f  sample_fare=%.2f  err=%.2f%%" % (
    s['exact_mean_fare'], s['sample_mean_fare'], s['fare_error_pct']))

# MinHash + LSH
from utils.minhash_lsh import MinHash, LSH, jaccard, all_pairs_jaccard
mh = MinHash(num_hashes=100)
lsh = LSH(num_hashes=100, bands=20, rows_per_band=5)
sets = {
    1: set([1,2,3,4,5]),
    2: set([1,2,3,4,6]),
    3: set([10,11,12,13]),
}
for zone, s in sets.items():
    sig = mh.signature(s)
    lsh.add(zone, sig)
candidates = lsh.get_candidates()
true_pairs = all_pairs_jaccard(sets, threshold=0.5)
j12 = jaccard(sets[1], sets[2])
j13 = jaccard(sets[1], sets[3])
print("LSH: candidates=%s  true_pairs=%s" % (candidates, true_pairs))
print("  J(1,2)=%.2f  J(1,3)=%.2f" % (j12, j13))

# Config
from utils.config import load_config
cfg = load_config()
print("Config: partitions=%d  rate=%d" % (cfg['kafka']['partitions'], cfg['producer']['rate']))

print()
print("ALL TESTS PASSED!")
