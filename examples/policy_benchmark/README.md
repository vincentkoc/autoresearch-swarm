# Empirical policy and no-op pilot

This example runs real CPU experiments through the same SDK commands available to
an external agent. It compares the shipped greedy and portfolio planners, funds
each search as a child campaign, and measures timed no-op confirmation panels.
It does not train a policy or claim that one search method is generally better.

From the repository root:

```sh
uv run python examples/policy_benchmark/run.py --state ./benchmark-state
```

The default design runs 81 training trials and 162 native stages: 12 policy
children with four trials each, plus three no-op panels with 11 trials each.
The state directory retains the controller, evidence, frozen definition, command
intents and result. The CLI emits the result as JSON. Run the same command to
resume or retrieve the completed result. Changed definitions, package source,
workload source or worker fingerprints require a new state directory.

Two admitted CPU slots are required. The normal shared machine registry remains
in use. Do not create a separate registry to bypass an existing reservation.
The benchmark is bounded; it does not start a persistent agent service.

## Frozen design

`benchmark.json` declares development and held-out starting seeds before any
trial executes. A repeat uses another predeclared seed block. Greedy and portfolio
receive the same block, source, evaluator, device profile and allowances.

Each child receives:

| Allowance | Quantity |
|---|---:|
| Trials | 4 |
| Native attempts | 8 |
| Proposal requests | 2 |
| Execution reservation ceiling | 160,000 ms |
| Active training per trial | 300 ms |
| LLM calls and tokens | 0 |

The first trial establishes the zero-learning-rate baseline. A fixed scripted
proposer then supplies learning rates 0.05 and 0.35. The actual planners admit,
observe and select those experiments through normal commands. Proposal responses
record their scripted identity and zero reported token usage. The script does
not consult held-out outcomes to change its proposal sequence.

Before the fourth trial, the selected candidate and its supporting receipt are
frozen. A fresh audit seed, matched across policies, evaluates that candidate.
The audit result is reported separately and is not fed back into the planner.
This measures a selected candidate on a new initialization; the dataset and
evaluator remain the CPU example's original synthetic benchmark.

Both policies have access to the same two-slot worker. Greedy admits one active
research trial; portfolio may admit two. The driver drains each ready batch
before the next policy advance. This pilot therefore does not measure the full
benefit of continuously interleaved asynchronous research. Existing native
portfolio tests separately verify refill while a sibling remains alive.

Execution order alternates by repeat. The parent ledger includes child spending;
the result verifies their equality and does not add the two totals together.
Native timeouts and closure allowances determine admission ceilings. They are
separate from the 300 ms training region. Unused grants are released by ordinary
child completion, and failures retain their actual costs.

## Timed no-op panels

Each no-op panel uses the positive learning-rate workload, with identical source
bytes on both sides. A baseline establishes the incumbent, then a sealed
five-seed plan creates ten new candidate/comparator realizations. The protected
scorer and the existing `confirmation.assess` command decide the outcome.

No-op stages execute serially through `LocalDriver(..., concurrency=1)` while the
worker retains its original two-slot admission. The cap limits delivery; it does
not change resource ownership or prove closure of any other work. The current
plan admits candidate before comparator for each seed. This fixed order and
other host activity can influence timed results. The report records that limit.

The result includes every score difference, gate decision, receipt, optimizer
step count, active-training time, native elapsed time and execution fingerprint.
It reports the observed false-promotion frequency and a Wilson 95% interval.
That interval assumes independent Bernoulli panels. Three panels cannot validate
a universal 5% error bound, and the fixed execution order does not establish the
independent symmetric-sign assumptions of the mathematical sign test.

## Evidence and recovery

The definition hashes all installed package Python source, the runner/session helpers, workload
files and worker profile. This is a replay boundary, not an immutable container
or a guarantee that every dependency file is unchanged.

Command envelopes are written before delivery. A lost reply reuses the original
revision and idempotency key. Source preparation publishes complete trees
atomically and verifies their bytes before capture. Incomplete reports use new
progress watermarks; they cannot silently replay an earlier budget snapshot.

An incomplete cell remains in the denominator. Its aggregate policy quality is
missing rather than averaged over successful survivors. An unknown execution
retains its original resources and budget. A failed no-op baseline remains an
explicit incomplete panel. Infrastructure failures stop the runner with durable
state available for diagnosis and resume; they are not silently retried as new
scientific realizations.

Completed results are immutable snapshots. Later evidence corrections can change
current eligibility without rewriting this historical analysis. Use the live
research API before making a new promotion decision. This example never adopts
a research policy based on the report.

`definition.json` and `result.json` are also uploaded through the artifact API and
held with `artifact.retain`; `artifact.json` identifies the final artifact and its
retention reference. These
are discretionary manual holds. An authorized `artifact.release` can release
such a hold. It cannot release required evaluator, attempt or campaign evidence
owned by another lifecycle. Retaining a file grants no scientific authority.

## Qualification

The ordinary test suite covers source interruption, changed inputs, missing
results, a real failed baseline, fresh report snapshots, and concurrency caps.
Enable the empirical run explicitly:

```sh
RESEARCH_BENCHMARK_TESTS=1 uv run pytest -q -s tests/test_policy_benchmark.py
```

That integrated test runs a 27-trial reduced design and injects a lost child
creation reply. To exercise the complete checked-in design:

```sh
RESEARCH_BENCHMARK_TESTS=1 RESEARCH_BENCHMARK_FULL=1 \
  uv run pytest -q -s tests/test_policy_benchmark.py -k real
```

Both verify independent receipts, closed accounting, new audit realizations,
reopen without new trials, and rejection of changed benchmark inputs. Neither
asserts that a policy must win or that a no-op panel must never promote.

A local macOS/Apple M5 Max pilot on 2026-10-30 completed the full 81-trial
design. All 12 policy cells and all three no-op panels completed. No no-op panel
promoted; the 15 paired score differences had a sample standard deviation of
about 0.0292 bpb. The Wilson upper bound from only three panels was about 56.1%.
This run qualified the evidence and accounting path under ordinary host activity,
not a dedicated performance environment. The held-out mean audit improvements
were approximately 3.3277 bpb for greedy and 3.3272 for portfolio relative to the
zero-learning baseline. Those close values do not establish a policy advantage.
