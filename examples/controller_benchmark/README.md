# Archived controller benchmark

The [local CPU report](results/local-cpu/report.md) and
[machine-readable results](results/local-cpu/result.json) preserve measurements
from the retired controller. The original harness and reproduction instructions
are available at the [benchmark revision](https://github.com/vincentkoc/autoresearch-swarm/tree/395410b392aab406179a3fb9f4096534cf2f5e55/examples/controller_benchmark).

## Included measurement

The included panel completed all 14 cells and all 48 CPU trials with eligible
receipts, 96 stage attempts, and no outstanding budget reservation or uncertainty.
Paired two-lane speedups were 1.77×, 2.02× and 1.80×; the median was 1.80×, or
about 90% parallel efficiency. The slightly superlinear second pair is a noisy
short-workload observation, not evidence of sustained superlinear scaling.

Synthetic heartbeat throughput ranged from 514 to 882 accepted commands/second
across the four connection counts, with no failed command/query/replay samples.
Each budget race admitted exactly one reservation. Runtime reopen and exact replay
took 63.9 ms in its single sample. These are descriptive local measurements;
three CPU pairs and one recovery sample do not establish confidence bounds or
production capacity. The JSON contains the exact system and implementation hashes.
