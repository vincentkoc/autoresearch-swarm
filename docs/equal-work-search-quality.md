# Equal-work campaign search quality

The swarm missed the registered Phase 3 acceptance criterion. On program, swarm and random each scored 48/48 on confirmation for every seed. Even a perfect swarm recipe could only tie that baseline. Every arm spent its registered search allowance. All 396 search attempts completed on the first try.

## Confirmed scores

Each value scores the recipe selected after the held-out decision. Gains are paired differences in pass rate, accuracy or next-token loss in nats. Positive favors the swarm. The decision uses unrounded means across the three search seeds.

| Mean across seeds | Swarm | Random | Greedy | Gain over random | Gain over greedy | Strict win |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Program pass rate ↑ | 1.000000 | 1.000000 | 0.243056 | 0.000000 | +0.756944 | No |
| MLP accuracy ↑ | 0.911458 | 0.898438 | 0.906250 | +0.013021 | +0.005208 | Yes |
| nanoGPT loss ↓ | 1.721110 | 1.645046 | 1.731189 | -0.076064 | +0.010080 | No |

| Workload | Search seed | Swarm | Random | Greedy | Gain over random | Gain over greedy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Program | 3375328711 | 1.000000 | 1.000000 | 0.312500 | 0.000000 | +0.687500 |
| Program | 1721423239 | 1.000000 | 1.000000 | 0.312500 | 0.000000 | +0.687500 |
| Program | 1767091650 | 1.000000 | 1.000000 | 0.104167 | 0.000000 | +0.895833 |
| MLP | 3375328711 | 0.906250 | 0.882812 | 0.906250 | +0.023438 | 0.000000 |
| MLP | 1721423239 | 0.921875 | 0.906250 | 0.906250 | +0.015625 | +0.015625 |
| MLP | 1767091650 | 0.906250 | 0.906250 | 0.906250 | 0.000000 | 0.000000 |
| nanoGPT | 3375328711 | 1.653641 | 1.653641 | 1.640834 | 0.000000 | -0.012808 |
| nanoGPT | 1721423239 | 1.640834 | 1.640748 | 1.683881 | -0.000086 | +0.043047 |
| nanoGPT | 1767091650 | 1.868854 | 1.640748 | 1.868854 | -0.228106 | 0.000000 |

On MLP, the swarm gained 1.30 percentage points over random and 0.52 over greedy. Its entire gain over greedy came from seed 1721423239. On nanoGPT, swarm and random selected the same recipe on the first seed. On the third seed, the swarm’s constant-rate AdamW recipe had lower full-rung search loss than random’s cosine-decay AdamW recipe (1.855449 versus 1.871568). Confirmation reversed that ordering (1.868854 versus 1.640748). Both nominees passed their own held-out comparison against the initial recipe.

## Work and search curves

Every program arm executed 576 generated cases per seed. MLP used 288 optimizer updates per arm and seed, and nanoGPT used 144. The common initial recipe and full refits after promotion both consumed this allowance. A search trial can be a half screen or a full refit of a previously seen recipe.

Screen and full trial counts and CLI seconds sum across three seeds. The three change columns give the oriented improvement from the first full-rung score to the final full-rung best for each seed in registered order. All full-rung best-so-far curves were monotone. A zero change identifies a flat curve.

| Workload | Policy | Half trials | Full trials | Seed 1 change | Seed 2 change | Seed 3 change | CLI seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Program | swarm | 48 | 12 | +0.375000 | 0.000000 | 0.000000 | 26.8 |
| Program | random | 0 | 36 | +1.000000 | +1.000000 | +1.000000 | 12.6 |
| Program | greedy | 0 | 36 | +0.437500 | +0.437500 | +0.166667 | 12.7 |
| MLP | swarm | 48 | 12 | +0.179688 | +0.007812 | +0.093750 | 218.2 |
| MLP | random | 0 | 36 | +0.335938 | +0.425781 | +0.378906 | 132.5 |
| MLP | greedy | 0 | 36 | +0.378906 | +0.425781 | +0.378906 | 198.4 |
| nanoGPT | swarm | 48 | 12 | +0.008846 | +0.045807 | +0.068604 | 240.9 |
| nanoGPT | random | 0 | 36 | +0.971272 | +1.041956 | +0.846979 | 140.2 |
| nanoGPT | greedy | 0 | 36 | +0.977308 | +0.996112 | +0.863097 | 167.2 |

## Held-out work and model usage

Each workload shared one study across nine arms. Every cell gives panel looks / actual work units, including the common calibration. Reusing a cached confirmation adds no fit. Each study exceeded its 120-look search warning. Program also exceeded its 36-look selection and 6-look confirmation warnings. The queue records the search scores for worker evaluations. CLI elapsed time includes the shared calibration in the first arm, worker startup and any model requests.

| Workload | Work unit | Search | Calibration proxy | Calibration target | Selection | Confirmation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Program | cases | 132 / 5,184 | 9 / 432 | 9 / 432 | 48 / 2,304 | 9 / 432 |
| MLP | updates | 132 / 2,592 | 9 / 216 | 9 / 216 | 18 / 432 | 4 / 96 |
| nanoGPT | updates | 132 / 1,296 | 9 / 108 | 9 / 108 | 30 / 360 | 6 / 72 |

Arms selecting the same recipe share its single fixed-seed confirmation measurement. The reuse counts below concern selected recipes. The panel totals above also include reference and rejected-recipe measurements where applicable.

| Workload | Selections | Distinct selected recipes | Most uses of one confirmation | Rejected nominees |
| --- | ---: | ---: | ---: | ---: |
| Program | 9 | 8 | 2 | 0 |
| MLP | 9 | 3 | 7 | 0 |
| nanoGPT | 9 | 5 | 2 | 0 |

| Workload | Local Qwen requests | Successful | Input tokens | Output tokens | Request seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| Program | 6 | 6 | 5946 | 198 | 5.7 |
| MLP | 0 | 0 | 0 | 0 | 0.0 |
| nanoGPT | 6 | 6 | 6766 | 320 | 6.9 |

Qwen was available to the program and nanoGPT swarm arms. All successful requests used CPU inference. Equal case or update counts control search expenditure. Model inference, architecture-dependent FLOPs and elapsed time are separate quantities. These generated CPU measurements leave physical accelerator, billing and authentic-workload acceptance unmeasured.

## Reproduction and evidence

The run used Python 3.12.14, PyTorch 2.15.0 with one CPU thread, local Ollama 0.40.16 and an Apple M3 Ultra on macOS 26.6.2. The common bandit exploration setting was 1.0. Production source and the lockfile matched qualified main `96e0db02`. The benchmark driver matched `c2faed9a`. Both were present in the measured revision. [Measured source](https://github.com/vincentkoc/autoresearch-swarm/tree/2d23d8412b012e1cf400af9e52b72ca8dc13e20b)

The protocol was recorded before the first fit, with SHA-256 `07ae249851d692d9e5bf7b282acc92e790a90600d26426a702a55361167b4b97`. The archive includes all search recipes and rung scores, the selected recipes and their measured confirmations, decision reasons, actual work by role and model counters. [Frozen protocol](../benchmarks/protocol-equal-work.md) · [27 arm records](../benchmarks/results/v3-equal-work-cpu.jsonl)

To reproduce from this checkout, serve the registered Qwen manifest with Ollama on loopback. Use the local port and a new output directory. The driver writes each campaign spec, queue, evaluation log and summary, plus one panel study per workload.

~~~sh
uv sync --locked --group dev --extra torch --python 3.12
uv run --no-sync python -m benchmarks.equal_work .research/equal-work-repeat --port 11434
~~~
