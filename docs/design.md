# Autoresearch Swarm design

A campaign searches for a better recipe under a fixed objective and budget.
The coordinator asks for candidates, sends trials to workers, and compares
the resulting measurements. A workload supplies its own model and scorer.

[Run a campaign](run-a-campaign.md) · [Operate a campaign](operator-guide.md)

## Workloads

| Shape | Candidate | Trial | Objective example |
| --- | --- | --- | --- |
| LLM-only | Prompt or small program | Apply the artifact to fixed cases on CPU | Test pass rate |
| GPU-only | Model and optimizer parameters | Train and evaluate a model | Classification accuracy |
| LLM+GPU | A change to training code | Apply the edit, train and evaluate | Next-token validation loss |

An LLM can propose a candidate in the first or third workload.
It receives the task description and a bounded search history.
The worker runs the resulting candidate under the same scorer as its peers.

## Campaign contract

The YAML spec supplies the task, objective, capacity and stop conditions.
Python callers use the same fields on `CampaignSpec`.
The path to the YAML file provides the base for its task directory.

| Field | Meaning |
| --- | --- |
| `campaign_id` | Durable identity of this search |
| `metric`, `direction` | Numeric output and whether to maximize or minimize it |
| `evaluation_mode` | Calibrated research by default, or explicit smoke testing |
| `task_dir`, `command` | Working directory and argument vector for a trial |
| `candidates`, `search_space` | Named starting recipes or dimensions of a finite grid |
| `seed`, `fidelity` | Default random seed and amount of work for a trial |
| `max_trials`, `max_workers` | Total distinct trials and concurrent work |
| `max_hours` | Wall-clock limit for new work, measured from campaign creation |
| `max_attempts` | Maximum executions of each trial, including the first |
| `trial_timeout_seconds` | Workload deadline |
| `proposal_timeout_seconds` | Proposal deadline |
| `target` | Optional stopping score in the declared direction |
| `top_k`, `min_replicates`, `min_delta` | Default ranking and replacement rule |
| `max_cost` and prices | Dispatch limit and rates for measured usage |

A campaign can select a proposer, backend, worker, selection policy and panel
policy by importable factory name. The backend sets up execution, while the
panel policy owns the evaluation split and the held-out decision.

The store keeps the effective campaign spec. Resuming with the same campaign
ID uses the recorded definition. A conflicting definition requires a new ID.

## Records

### Candidate

A candidate contains parameters, an optional text artifact or diff, and a
source revision when the task uses one. The task decides how to apply these
inputs. An optimizer can produce the same type of record as an LLM.

Each candidate has an ID, family, operator and parent IDs. Lineage describes
derivation. A parent link makes no claim about measured improvement.

### Trial

A trial binds one candidate to a campaign, seed and fidelity.
The loop derives a stable trial ID from these values.
Submitting the same trial again leaves one queue entry.

A different seed measures another draw from training randomness.
A different fidelity changes the work performed.
A retry of a failed attempt uses the original trial.

### Attempt

An attempt records one claim of a trial. It has an attempt ID, worker ID,
number and start time. The worker renews its heartbeat during execution.

An attempt produces an observation with a status, metrics, diagnostic
output and measured usage. Failed and lost attempts remain in the archive.

| Status | Interpretation |
| --- | --- |
| `ok` | The task produced its required metric and completed |
| `failed` | The execution reported a failure, such as a nonzero exit or invalid metric |
| `lost` | The worker or backend lost the execution, including an expired heartbeat |

## Control loop

~~~text
load spec and open queue
calibrate the panel policy
restore observations and the incumbent
while work remains:
    read completed attempts in event order
    tell the proposer and update selection
    ask for candidates up to available capacity
    submit new trials and start workers for queued trials
    wait for a worker result or observe work owned elsewhere
report the archive and selected trial
~~~

Available capacity depends on the backend, active workers, queued work and
remaining trial budget. A large requested batch cannot override those limits.

The default proposer consumes named candidates and the finite grid.
A custom proposer receives the spec, archive and requested batch size.
It can return fewer candidates when an operator times out or exhausts its
space. The coordinator continues processing already submitted work.

Each accepted attempt reaches `tell`. A proposer can use failures to change
its next proposal. A failed recipe does not require a human decision before
other trials can finish.

The loop can resume after interruption. It restores archived observations,
reads existing trial IDs and recovers the selected trial from SQLite.
Replaying a completed finite campaign reuses its recorded trials.
Resuming preserves the original wall-clock deadline. After that deadline,
the queue refuses new claims and the coordinator collects active attempts.

## Queue and result publication

SQLite serializes claims and results. The swarm store uses six tables.

| Table | Content |
| --- | --- |
| `swarm_projects` | Project IDs |
| `swarm_campaigns` | Campaign spec and selected trial |
| `swarm_candidates` | Campaign-scoped recipe and ancestry |
| `swarm_trials` | Seed, fidelity, state and attempt count |
| `swarm_attempts` | Worker claim, heartbeat, reservation, cost and observation |
| `swarm_events` | Ordered changes for a campaign |

A claim checks concurrency and budget in the same write transaction that
moves a queued trial to running. The worker receives its attempt only after
the store records the claim.

~~~text
queued -> running -> ok
                  -> failed or lost
                  -> queued if the retry allowance remains
~~~

A heartbeat updates the active attempt. Once an observation exists, a late
heartbeat or result cannot replace it. This prevents an old worker from
overwriting an accepted retry.

An expired heartbeat permits another attempt within the retry allowance.
The original process might still be alive. Tasks must tolerate duplicate
execution and keep outputs separate until the store accepts a result.

## Workload interface

The worker starts the task with its argument vector in `task_dir`. The task can be an
ordinary Python program, a training binary or a project-specific evaluator.
The swarm package does not need to import that project's model.

| Environment variable | Content |
| --- | --- |
| `SWARM_TRIAL` | Path to the JSON trial envelope |
| `SWARM_METRICS` | Destination for the numeric metrics object |
| `SWARM_USAGE` | Optional destination for measured resource counters |
| `SWARM_DATA` | Path to staged inputs when the backend provides one |

The envelope includes the candidate, seed, fidelity and trial ID.
For example, a classifier could write this metrics object.

~~~json
{"accuracy": 0.875, "validation_examples": 256}
~~~

The declared metric must have a finite numeric value. The backend records
diagnostics from stdout and stderr. A nonzero exit, missing metric or timeout
produces a failed attempt.

Use the trial seed for stochastic training and record the source revision
needed to reconstruct a candidate. The workload owns its dataset, splits,
aggregation convention and treatment of missing examples.

Use separate attempt output locations for artifacts that can conflict.
The local backend removes its temporary trial envelope and output files
after it reads the result. A workload that needs a persistent checkpoint
writes it to a durable project path.

## Worker resources

A worker claims one trial, executes it with a backend and reports its
result. It releases its allocation after the attempt. Subprocess lifetime
belongs to the backend.

The local backend starts a separate process group. It captures bounded output
and kills remaining group members during teardown. A child that inherits
the output pipe should not keep the trial alive after its deadline.

The resource pool distinguishes CPU slots, LLM slots and GPU memory.
A worker can account for estimated token demand before it makes a claim.
For GPU jobs, the pool uses demand estimates and measured peak VRAM to
choose a device with sufficient headroom.

A backend with explicit device binding can execute on the chosen device.
The training command must respect that binding.
Record device and workload size when comparing throughput.

## Meter

The cost meter combines elapsed execution time and token counts.
All configured prices use the same currency.

~~~text
compute cost = elapsed seconds × slot price per hour / 3600
input cost   = input tokens × input price per million / 1000000
output cost  = output tokens × output price per million / 1000000
attempt cost = compute cost + input cost + output cost
~~~

At claim time, the store checks recorded charges and in-flight estimates.
The estimate uses the configured time allowance and expected token counts.
Measured charges replace the estimate when execution finishes.

During a running attempt, a heartbeat can record accrued cost. If the worker
disappears, the store retains the amount it has observed.
Provider billing can include additional charges outside these inputs.

A zero hourly rate is appropriate for a local comparison that reports time
separately. It cannot estimate the cost of running the same workload on a
different backend.

## Search and population

The archive retains all observations. Ranking starts with successful
observations of the objective. The default population groups them by
candidate and fidelity, with one measurement per distinct seed.

A candidate must have enough distinct seeds for the configured replicate
count. The population ranks eligible candidates at the highest eligible
fidelity by mean score. Candidate ID breaks an exact tie.

~~~text
candidate A at fidelity 0.25, seed 11
candidate A at fidelity 1.00, seed 11
candidate A at fidelity 1.00, seed 23
~~~

Only the last two observations contribute to A's full-fidelity mean.
Repeating the first observation cannot supply a second full-fidelity seed.

Operators produce children by changing parameters, rewriting an artifact,
editing code, combining parents or drawing a new seed. An operator name in
the candidate record lets the campaign measure which changes paid off.

A multi-fidelity policy can run inexpensive trials first and promote a
fraction to a higher rung. An island policy can choose parents from several
families. A paired-seed comparison can request another replicate before a
gain becomes eligible for promotion.

## Evaluation panels

A panel lists stable IDs for evaluation members. Use subject IDs when
examples from the same subject would leak information between splits.
Check those IDs against the training manifest before the first trial.

| Split | When the study reads it |
| --- | --- |
| Training | Fit a candidate's model |
| Calibration proxy | Measure known variants with the proxy protocol |
| Calibration target | Measure the same variants with the target protocol |
| Search | Rank proposed candidates |
| Selection | Compare a nominated candidate against the incumbent |
| Confirmation | Check that the gain survives on independent cases |

The study rejects duplicate IDs within a split and overlaps between splits.
The evaluator receives the declared member list for its role.
A workload must use that list when it computes the score.

### Calibration

A research campaign requires a panel policy. It calibrates before its first
proposal or dispatch. Measure the reference and at least two other known
recipes with at least two distinct seeds. Pair each proxy measurement with a
target measurement for the same recipe and seed.

For a dispatch smoke test, the spec can set `evaluation_mode: smoke`.
The summary identifies this mode. It ranks direct measurements and excludes
held-out panels.

For each recipe, compute the mean proxy score and mean target score across
seeds. Fit a line across the recipe means.

~~~text
target mean = intercept + slope × proxy mean
~~~

The slope gives the fitted change in target score per unit of proxy change.
The intercept accounts for a difference in scale or origin. If every recipe
has the same proxy mean, the slope and intercept are undefined.

Seed noise is the square root of the pooled within-recipe target variance.
The fit uses equal numbers of seeds for each recipe. This separates changes
between recipes from variation between repeated fits of one recipe.

Calibration also reports the number of measured trials, mean seconds per
trial and trials per second. Throughput includes scoring both calibration
splits on the chosen device.

### Selection and confirmation

The study measures challenger and reference on selection cases at the same
seeds. It orients each difference so a positive gain is better.
The mean gain must exceed the configured minimum and noise threshold.

A selection winner receives one confirmation look at the fixed confirmation
seed. Its gain must exceed the minimum on that panel. The study caches each
recipe's confirmation score for later comparisons. An unsuccessful decision
leaves the incumbent in place.

The benchmark nominates one finalist per arm at the end of its search budget.
The swarm loop can nominate a candidate when a new result changes its
ranking. After a rejection, it can consider the next eligible candidate.
Held-out scoring runs off the event loop so other workers can heartbeat.
Both routes use the same study decision.

### Exposure history

Reserve a look before exposing a scorer to the cases. Count a failed
evaluation if the cases could already have been read.
A search dispatch can reserve its look before the worker starts.

The sidecar SQLite file records look counts, calibration and held-out
decisions. Its lock serializes calibration and promotion decisions.
A unique confirmation row per recipe prevents two workers from obtaining
a second look after a crash.

If the confirmation call fails after reservation, the look is consumed.
Reopening the file preserves that outcome. A stored successful decision
can be replayed without exposing the panel again.

Look limits are warnings. Counts remain available across runs that share
the same study. Recalibrate after changing the scorer, split, calibration
recipes or decision rule. A separate study identifies the new comparison.

## Backends and data

Local execution uses subprocess slots. A local command can train on CPU,
MPS or CUDA when its environment supports the requested device.
The command owns framework-specific device setup.

An SSH pool assigns work to configured hosts. Preparation and hydration
allow several trials to reuse the same data on a host. An idle keeper can
stop an unused host after its configured delay.

A managed-job backend hands an attempt to a provider and observes its
terminal state. A recoverable placement failure or preemption can yield a
lost observation. The queue's retry allowance bounds subsequent attempts.

A data stage prepares a snapshot per location and records its digest.
Hydration copies that snapshot to local storage and verifies the copy.
A read-only mount can protect a shared input tree during training.

Keep provider lifecycle decisions separate from task execution failures.
A failed attempt consumes one try. Removing a shared data volume or host
is a distinct operator action.

## Evidence and comparison

Every policy comparison needs a metric direction, a baseline, a fixed
search budget, data splits and seeds. Use the same starting candidate for
all arms. Keep failed attempts in the result.

The benchmark suite supplies random sampling and greedy one-field mutation.
It prints raw scores and a running best. This curve is monotone by
construction. It shows when the best search observation changed.

Separate the search curve from the selected candidate, its held-out gain
and its cost. An apparent improvement late in a long search can disappear
on confirmation.

Report paired results across seeds, including ties and regressions.
Include the device, training profile, trial count and elapsed time.
For an LLM proposer, include its model and observed token counts.

A local smoke run verifies that the task can execute and emit its metric.
Scientific comparisons require the stated evaluation protocol and enough
independent measurements for the question being asked.

[Benchmark workloads and comparison protocol](../benchmarks/README.md)

## Extension points

| Interface | Responsibility |
| --- | --- |
| `Proposer` | Ask for a bounded batch and consume each result |
| `Operator` | Derive children from parent candidates |
| `Backend` | Advertise capacity and execute one claimed attempt |
| `Worker` | Claim, heartbeat, execute and report |
| `Evaluator` | Read the stage's numeric metrics |
| `Meter` | Estimate dispatch cost and price measured usage |
| `Store` | Persist trials, claims, observations, selection and events |

## Compatibility

The swarm queue and YAML entry point have their own trial and attempt
records. Older controller examples use project manifests, command APIs
and evaluator receipts. Use the interface that produced the records when
reading an existing campaign.

The earlier controller design and operational guides remain available at
the revision that describes those records.

[Controller documentation at the previous revision](https://github.com/vincentkoc/autoresearch-swarm/tree/b8b44174b925483c8b202163fef12b733517fa49/docs)
