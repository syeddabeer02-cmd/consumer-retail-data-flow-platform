# Measured benchmark

Executed October 3, 2026 on Python 3.12.14, PySpark 3.5.3, Java 17 and Linux.
Spark master: local[2], four shuffle partitions. The runtime reported nine
logical CPUs; this run used two local Spark workers. Seed: 42. The run processed
all eight output datasets, contract checks, funding calculations, reconciliation,
publication gates, source hash verification and success publication.

| Measure | Actual result |
| --- | ---: |
| Requested valid funding records | 100,000 |
| Funding input rows including deliberate invalid/duplicate | 100,002 |
| Payout input rows | 110,001 |
| Total source rows | 210,003 |
| Synthetic generation | 2.96 seconds |
| Complete pipeline | 31.90 seconds |
| Funding rows / pipeline second | 3,134.8 |
| Written output bytes | 20,189,405 |
| Matched funding records | 80,000 |
| Mismatch / missing / reversal / contra | 5,000 each |
| Orphan payout records | 1 |
| Rejected input rows | 1 |

[Raw measured record](benchmark-results.json) retains runtime/configuration and
counts. Memory sampling was unavailable in this execution environment (null),
so no memory figure is claimed. The benchmark tool can sample process-tree RSS
from /proc on a normal Linux runtime; shared pages may be counted multiple times.

This is a reproducible portfolio benchmark, not a 50M-record production test.
It is a single source-date snapshot with deliberately simple deterministic
financial rules and 20 vendors. Do not extrapolate linear production throughput:
larger history, vendor skew, cloud I/O, shuffles, data cardinality and worker
memory change performance. The four-hour job timeout is a limit, not proof that
the resume's historical production SLA has been reproduced.

Reproduce in a fresh data root:

```bash
python -m retail_flow.cli benchmark --root data/benchmark --date 2026-10-01 --rows 100000 --master 'local[2]'
```

Increase rows and retained partitions separately. Set SPARK_SHUFFLE_PARTITIONS
for cluster tuning; record every configuration and compare measured results.

## Larger measured run

The same full pipeline also processed **1,000,000 valid funding records plus
1,100,001 payout rows** (2,100,003 total source rows including deliberate
funding duplicates/invalid input) in **142.07 seconds**, following 30.249 seconds
of generation. It wrote 271,322,033 output bytes. Counts: 800,000 matched;
50,000 each mismatch/missing/reversal/contra; one orphan. Seed 42 and local[2].
See [larger measured record](benchmark-million.json).

The runtime virtualizes Python process IDs differently from /proc. Its original
memory sample was incomplete and has been marked unavailable; no memory usage
claim is made for this larger run. The monitor now resolves /proc's kernel
process ID before sampling. Throughput and accounting counts are unaffected.
This larger run still does not reproduce a 50M-record production workload.
