# Campaign search quality on fresh v1 panels

The operator portfolio with ASHA missed the three-workload acceptance target. Only nanoGPT beat both baselines on mean confirmed score. The program tied random on all three seeds. On MLP, the seed-11 loss outweighed two gains over random. All 324 search attempts returned a score.

## Mean confirmed score

Each value is the mean selected-recipe score across seeds 11, 23 and 47. Gains are paired differences in metric units. Positive means the swarm did better, including for nanoGPT where lower loss wins. A tie counts as zero.

| Metric | Swarm | Random | Greedy | Gain over random | Gain over greedy |
| --- | ---: | ---: | ---: | ---: | ---: |
| Program pass rate | 0.798611 | 0.798611 | 0.298611 | 0.000000 | +0.500000 |
| MLP accuracy | 0.897135 | 0.903646 | 0.907552 | -0.006510 | -0.010417 |
| nanoGPT loss | 1.885019 | 1.934733 | 1.987143 | +0.049714 | +0.102123 |

On nanoGPT, the entire gain over random came from seed 23. The policies tied on seeds 11 and 47. The swarm beat greedy on all three confirmation seeds.

### Confirmed score by search seed

| Metric | Seed | Swarm | Random | Greedy | Gain over random | Gain over greedy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Program pass rate | 11 | 0.395833 | 0.395833 | 0.104167 | 0.000000 | +0.291667 |
| Program pass rate | 23 | 1.000000 | 1.000000 | 0.395833 | 0.000000 | +0.604167 |
| Program pass rate | 47 | 1.000000 | 1.000000 | 0.395833 | 0.000000 | +0.604167 |
| MLP accuracy | 11 | 0.859375 | 0.921875 | 0.906250 | -0.062500 | -0.046875 |
| MLP accuracy | 23 | 0.910156 | 0.878906 | 0.910156 | +0.031250 | 0.000000 |
| MLP accuracy | 47 | 0.921875 | 0.910156 | 0.906250 | +0.011719 | +0.015625 |
| nanoGPT loss | 11 | 1.883113 | 1.883113 | 1.888832 | 0.000000 | +0.005719 |
| nanoGPT loss | 23 | 1.888832 | 2.037974 | 2.036298 | +0.149142 | +0.147466 |
| nanoGPT loss | 47 | 1.883113 | 1.883113 | 2.036298 | 0.000000 | +0.153185 |

## Best full-fidelity search score

The swarm missed a strict mean search win over both baselines on every workload. On nanoGPT its best search loss averaged 1.796141, even though its selected recipes scored better on confirmation.

| Metric | Swarm | Random | Greedy | Gain over random | Gain over greedy |
| --- | ---: | ---: | ---: | ---: | ---: |
| Program pass rate | 0.791667 | 0.791667 | 0.291667 | 0.000000 | +0.500000 |
| MLP accuracy | 0.904948 | 0.942708 | 0.933594 | -0.037760 | -0.028646 |
| nanoGPT loss | 1.796141 | 1.782509 | 1.777326 | -0.013632 | -0.018814 |

<details>
<summary>Best full-fidelity search score by seed</summary>

| Metric | Seed | Swarm | Random | Greedy | Gain over random | Gain over greedy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Program pass rate | 11 | 0.375000 | 0.375000 | 0.125000 | 0.000000 | +0.250000 |
| Program pass rate | 23 | 1.000000 | 1.000000 | 0.375000 | 0.000000 | +0.625000 |
| Program pass rate | 47 | 1.000000 | 1.000000 | 0.375000 | 0.000000 | +0.625000 |
| MLP accuracy | 11 | 0.859375 | 0.945313 | 0.925781 | -0.085938 | -0.066406 |
| MLP accuracy | 23 | 0.925781 | 0.937500 | 0.941406 | -0.011719 | -0.015625 |
| MLP accuracy | 47 | 0.929688 | 0.945313 | 0.933594 | -0.015625 | -0.003906 |
| nanoGPT loss | 11 | 1.804445 | 1.804445 | 1.845063 | 0.000000 | +0.040618 |
| nanoGPT loss | 23 | 1.865780 | 1.824883 | 1.821737 | -0.040897 | -0.044043 |
| nanoGPT loss | 47 | 1.718198 | 1.718198 | 1.665179 | 0.000000 | -0.053019 |

</details>

The trial records contain every recipe, operator, fidelity, score and running best at that fidelity. Each arm also records the full-fidelity nominee, the selected recipe and its measured confirmation score. [All 27 arm records](../benchmarks/results/v1-cli-cpu-small.jsonl)

## What ran

All 27 campaigns used the installed `research swarm run` command on an Apple M3 Ultra with macOS 26.6.2, Python 3.12.14 and PyTorch 2.15.0. The two training workloads used CPU with one Torch thread. Each arm started from the same recipe and ran one worker. The proposal and training initialization seeds were 11, 23 and 47. Random sampled with replacement. Greedy mutated one field of its incumbent and preferred untried neighbors.

The operator bandit offered parameter mutation and crossover on every workload. On program and nanoGPT it also offered a local `qwen2.5-coder:0.5b` choice rewrite. ASHA used rungs 0.5 and 1.0, a reduction factor of three and no replication. In every swarm arm, nine half-fidelity trials and three full-fidelity promotions filled the budget. Each baseline used 12 full-fidelity trials. The nominal fidelity sums were 7.5 and 12 respectively. Actual elapsed times are below. The tested operator, scheduler, worker and benchmark sources match the merged portfolio. [Tested revision](https://github.com/vincentkoc/autoresearch-swarm/commit/6b7bdb3ad88aedde4df200f8f964e64a8f04adbc) · [Merged portfolio](https://github.com/vincentkoc/autoresearch-swarm/pull/84)

The comparison kept v1's recipe spaces, scoring functions, training stream (17) and panel sizes. The five evaluation streams came from the first four bytes of SHA-256 of `ars-cli-acceptance-v1/<role>`, interpreted as an unsigned big-endian integer. The roles were `search`, `selection`, `confirmation`, `calibration_proxy` and `calibration_target`. Their case IDs were disjoint from one another and from the published v1 and v2 panels. Each workload used a new panel database shared across all policies and seeds.

Before search, the study calibrated three recipes on three seeds on separate proxy and target panels. Each arm nominated its first best full-fidelity recipe after trial 12. To replace the common starting recipe, the nominee had to clear the calibrated noise threshold across the paired selection seeds and improve on confirmation. The study evaluated confirmation at seed 11 once per distinct recipe and reused the recorded score across arms. Proposers saw search observations only.

## Panel use and cost

A look is one scored recipe on a panel at one seed. The times below sum the three CLI runs per policy, including startup and held-out decisions. The first arm for each workload also performed the shared calibration.

| Metric | Calibration looks | Search looks | Selection looks | Confirmation looks | Swarm seconds | Random seconds | Greedy seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Program pass rate | 18 | 108 | 54 | 10 | 13.7 | 7.8 | 7.3 |
| MLP accuracy | 18 | 108 | 36 | 7 | 136.7 | 137.5 | 134.3 |
| nanoGPT loss | 18 | 108 | 24 | 5 | 136.4 | 135.2 | 135.7 |

The shared studies warn after 120 search, 36 selection or 6 confirmation looks. Program exceeded the selection and confirmation thresholds. MLP exceeded confirmation. nanoGPT stayed within all three. Repeated selected recipes reuse the same confirmation measurement, so the three paired results need not represent three distinct held-out fits.

| Workload | Proxy-to-target slope | Pooled seed noise | Calibration trials/second |
| --- | ---: | ---: | ---: |
| Program pass rate | 1.0337 | 0.0000 | 1039.97 |
| MLP accuracy | 0.9582 | 0.0463 | 8.21 |
| nanoGPT loss | 0.9567 | 0.0662 | 8.62 |

Local Ollama 0.40.16 served six successful generation requests. Three nanoGPT trials recorded a model rewrite as their operator. Program recorded none, and MLP used only the non-model operators. The campaign adapter did not record input or output token counts for these calls.

## Reproduce the comparison

From a source checkout with the operator portfolio and the comparison driver, start local Ollama with the model digest recorded in the driver. Use a new output directory for each comparison. The driver writes the campaign specifications, SQLite queue and panel databases, and `arms.jsonl`. [Comparison driver](../benchmarks/cli_acceptance.py) · [Benchmark definitions](../benchmarks/README.md)

~~~sh
uv sync --locked --group dev --extra torch --python 3.12
uv run --no-sync python -m benchmarks.cli_acceptance cli-acceptance-repeat --port 11434
~~~

To replay with the exact measured runtime, install the tested revision, run from its root and point its Python interpreter at the comparison driver from this report's checkout. Set `PYTHONPATH` to the runtime root so the driver imports the measured scorers. Timing and gain-per-cost bandit choices can vary between runs.

## Diagnosis of the program and MLP results

ASHA promoted a best full-fidelity recipe among the nine it screened in **every** original program and MLP arm. We checked all 54 screened recipes at their original search seed. The retained trials supplied 28 full-fidelity scores. We evaluated the other 26 on the same search cases, outside the original trial budget. Three promotions consumed trial slots, leaving nine distinct recipes for the swarm to screen per seed.

- **Program.** At seeds 23 and 47, random and the swarm each found a perfect recipe. At seed 11, their different recipes tied at 0.375 on search and 0.395833 on confirmation. The fixed-case evaluator has no training randomness. Proposal coverage and the score ceiling explain the ties.
- **MLP.** Neither baseline's winning recipe appeared among the swarm's nine screened recipes at any seed. Random sampled 11, 10 and 9 distinct recipes across the three seeds. Greedy tried 12 each time. All nine arm nominees passed the original held-out decision. Initialization still mattered. Among 22 recipes evaluated at full fidelity on more than one search seed, the largest accuracy range was 0.144531. Calibration estimated pooled seed noise at 0.046348. That estimate describes within-recipe variability. The three paired seeds cannot establish a population-wide advantage.

### Correction to finite mutation

The finite mutation operator reserved a screened recipe, but its exploration bonus counted only recipes with target-fidelity scores. On a flat two-coordinate grid with `(0, A)` already screened, the unique point covering both unseen levels is `(1, B)`. At seed 11 the old operator chose `(0, B)`. It chose the maximal-coverage point on two of three seeds before the correction and on all three afterward. The fix counts the features of each distinct recipe once when the operator issues it or observes a result. Promotions and repeated measurements of that recipe keep the same coverage count. Only successful target-fidelity scores enter the predictor.

We replayed the installed campaign CLI with finite mutation and crossover, the same starting recipes, search streams, seeds, budget and ASHA rungs. The model-free control reproduced all 72 archived program and MLP trials in order, including recipes, operators, fidelities and scores. The only algorithm change in the second run was the coverage count. Both runs scored the search panel only. For this search-only replay, set `evaluation_mode: smoke`, use `python -m benchmarks.cli_acceptance score TASK` as the worker command and omit the panel and selection factories. The table shows the best full-fidelity search score at trial 12. The baseline columns come from the original queues. [Before revision](https://github.com/vincentkoc/autoresearch-swarm/commit/901c8a39ee48e637dc271e613230634a69314a04) · [Screened recipes and trial records](../benchmarks/results/v1-finite-coverage.jsonl)

| Metric | Seed | Before | After | Change | Random | Greedy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Program pass rate | 11 | 0.375000 | 1.000000 | +0.625000 | 0.375000 | 0.125000 |
| Program pass rate | 23 | 1.000000 | 1.000000 | 0.000000 | 1.000000 | 0.375000 |
| Program pass rate | 47 | 1.000000 | 1.000000 | 0.000000 | 1.000000 | 0.375000 |
| MLP accuracy | 11 | 0.859375 | 0.894531 | +0.035156 | 0.945313 | 0.925781 |
| MLP accuracy | 23 | 0.925781 | 0.925781 | 0.000000 | 0.937500 | 0.941406 |
| MLP accuracy | 47 | 0.929688 | 0.910156 | -0.019531 | 0.945313 | 0.933594 |

Mean program search pass rate rose from 0.791667 to 1.000000. Mean MLP search accuracy rose from 0.904948 to 0.910156, still below random (0.942708) and greedy (0.933594). Before its first promotion, the MLP portfolio covered five distinct coordinate levels on each seed. After the fix it covered six. Broader early coverage helped one seed and hurt another. The original held-out acceptance result stands. This diagnosis made zero new selection or confirmation measurements.
