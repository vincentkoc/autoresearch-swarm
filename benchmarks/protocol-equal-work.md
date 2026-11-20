# Phase 3 equal-work comparison

Run the installed campaign CLI once per workload, seed and policy on generated CPU-small data. The earlier installed-CLI comparison spent 180 MLP updates per swarm seed and 288 per baseline seed. Its failed strict superiority result remains part of the record.

## Work and policies

Use the v1 program, MLP and nanoGPT recipe spaces, objectives, initial recipes, evaluator and calibration variants. Use one local CPU worker and one Torch thread. Random draws independently with replacement. Greedy chooses an untried one-coordinate neighbor of its earliest best incumbent, falling back to any neighbor when exhausted. Both baselines start with the initial recipe and always run at full fidelity.

| Workload | Metric | Half / full trial | Search allowance per seed and arm |
| --- | --- | ---: | ---: |
| Program | Generated cases passed / cases run, maximize | 24 / 48 cases | 576 executed cases |
| MLP | Correct classifications / 256, maximize | 12 / 24 optimizer updates | 288 completed updates |
| nanoGPT | Mean next-token cross entropy in nats, minimize | 6 / 12 optimizer updates | 144 completed updates |

Swarm uses the production operator portfolio. Offer finite parameter mutation and crossover on all three workloads and local `qwen2.5-coder:0.5b` choice rewrites on program and nanoGPT. Use ASHA at 0.5 and 1.0, reduction 3, family cap 2 and replication disabled. Pin the Qwen manifest `6ff36c41f6f588c457e538a5a42d24f9730cc3be7caa391d95d8b8e14f92d103` and request CPU inference. Freeze the exact operator configuration, scorer, driver, runtime revisions and model digest alongside this protocol before observing any fresh data. The common bandit exploration setting is chosen on the three archived v2 search seeds for each workload. Compare 0.25, 0.5 and 1.0 using mean within-block midrank of the best full-rung score across nine blocks. Prefer 0.5 on a tie, then the smaller tied value. No fresh observation enters that choice.

Charge the initial recipe, every half screen and every full refit, including a promotion trained from scratch. Each arm has at most 24 trials and one attempt per trial. Stop search at the work allowance. Compare actual completed optimizer steps or program case executions with every admission and its fidelity. Equal step counts do not establish equal FLOPs or wall time. Account for calibration, selection, confirmation, model requests and elapsed seconds separately. Record token counts when the local model supplies them.

## Paired seeds and panels

Take the first four bytes of SHA-256 of each UTF-8 label below, as an unsigned big-endian integer. The prefix is `ars-cli-equal-work-v1/`. All arms use the same seed and streams within a block.

| Labels after prefix, in order | Seeds |
| --- | --- |
| `search-seed/{0,1,2}` | 3375328711, 1721423239, 1767091650 |
| `calibration-seed/{0,1,2}` | 834499900, 2181106248, 3679631634 |
| `data/{train,calibration_proxy,calibration_target,search,selection,confirmation}` | 2369003720, 1211798524, 29554551, 1044117231, 3209056711, 3653294413 |

Use one initially empty panel study per workload, shared by all nine arms. Run program, MLP and nanoGPT in that order, each in the published seed order, with swarm, random and greedy in each block. Calibrate the three fixed recipes on the three calibration seeds on independent proxy and target panels. The first calibration seed fixes the confirmation fit. Each proposer sees only search results. Cache held-out decisions and measurements according to the production panel study. Report actual evaluations per role and repeat use of a confirmation score.

## Endpoints and failure rules

After exhausting the allowance, nominate the earliest successful full-fidelity recipe with the best search score. Compare it with the common initial recipe using the production paired selection threshold and confirmation decision. A rejected nominee retains the initial recipe. Extract the measured confirmation score of the selected recipe from the bound study. Keep the raw rung curves, full-fidelity running best, nominee, decision, selected recipe and confirmation score.

Compute both paired gains per seed from the unrounded selected confirmation scores. For program and MLP subtract the baseline from swarm. For nanoGPT subtract swarm from the baseline. A workload passes only if the arithmetic mean gain across the three registered seeds is strictly positive against **both** baselines. Ties contribute zero. Phase 3 also requires monotone best-so-far curves at common full fidelity on all three workloads. Report the first-to-last full-rung change separately so that a flat curve is visible. Publish all workloads and comparisons even if the criterion fails.

Retain failures and partial evidence. A failed or lost attempt, unspent allowance, overrun, absent full-fidelity finalist, unavailable selected confirmation, missing actual-work record or unqualified runtime makes the affected comparison incomplete. No replacement seed, extra fit, post-result tuning or alternative endpoint may rescue it. Report diagnostics and stop scoring an affected arm. Data, evaluator and proposer execution use local resources only. Hardware, billing and authentic workloads require separate measurements.
