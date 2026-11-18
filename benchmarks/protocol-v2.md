# Confirmatory benchmark comparison

The first protocol remains available as `v1`. An exhaustive audit after its
comparison found that the random arm reached the MLP search maximum on each
of its three seeds. The greedy arm reached the language model search minimum
on those seeds. Those ties remain part of the result.

This second protocol uses fresh cases and adds choices that affect fitting.
Freeze the search policy and evaluator revision before scoring the comparison.
Run all three workloads with all three policies. Publish the outcome even if
the baselines tie or win.

## Fixed design

- Use the small CPU profile and a budget of 12 attempts per policy and search
  seed. Count the common initial candidate, invalid proposals and failures.
- Use one study per workload across all arms. Calibration and held-out
  evaluations are additional measurements recorded separately from search.
- Keep the v1 program grammar and fixed-case pass-rate metric.
- Keep the v1 MLP training length, width and activation choices. Add hidden
  depth 1, 2 or 3, AdamW or SGD with 0.9 momentum, and weight decay 0, 0.001
  or 0.01. Use learning rates 0.001, 0.002, 0.004, 0.008, 0.016, 0.032
  and 0.064. The space contains 1,512 classifier recipes.
- Keep the causal model, training length, three optimizers and three
  schedules. Use the same seven base learning rates as the MLP. Add training
  label smoothing of 0, 0.03, 0.1 or 0.2 and a post-backward gradient norm
  cap of off, 0.5, 1 or 2. Score ordinary unsmoothed validation cross
  entropy for all 1,008 recipes.
- Use the first value of each dimension for the initial candidate. Keep the
  v1 calibration recipes with 0.004 and 0.016 replacing 0.005 and 0.02.
  New calibration dimensions keep their initial values.

## Seeds and membership

Derive each seed from the first four bytes of SHA-256 of a UTF-8 label,
interpreted as an unsigned big-endian integer. Use these labels exactly.

| Purpose | Label |
| --- | --- |
| Search initialization and proposer RNG | `ars-bench-v2/search-seed/{i}` for i = 0, 1, 2 |
| Paired calibration and selection replicates | `ars-bench-v2/calibration-seed/{i}` for i = 0, 1, 2 |
| Training and evaluation data stream | `ars-bench-v2/data/{role}` |

The roles are train, calibration_proxy, calibration_target, search, selection
and confirmation. The first calibration seed supplies the fixed confirmation
fit. Member IDs use the data stream seed and row offset for the workload.

## Endpoints

Nominate the best search recipe from each arm. Use the shared panel study to
compare each nominee with the common reference. If the nominee fails the
held-out decision, retain the reference. Read the selected recipe's recorded
confirmation score from the study.

Report every paired seed result. For each workload, average the signed
differences in confirmed selected score between the challenger and each
baseline. Orient loss so that a reduction is positive. A strict win requires
a positive mean against both baselines. Ties contribute zero.

Also report raw search curves, final search scores, confirmed recipes, failed
attempts, elapsed time and model tokens where available. A scorer failure
that leaves a required score unavailable makes the comparison incomplete.
Keep confirmation observations out of subsequent proposals and tuning.
