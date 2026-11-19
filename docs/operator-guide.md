# Operate a campaign

Run a campaign from the checkout that contains its task and plugin factories.
Keep its YAML spec, queue database and panel study together for resumption.

[Run a campaign](run-a-campaign.md) · [Watch and diagnose a campaign](campaign-ops.md) · [Design](design.md)

## Inspect the summary

The swarm CLI emits a JSON summary after the loop finishes. Check the
`reason` field before interpreting the score.

| Reason | What to inspect |
| --- | --- |
| `trial_limit` | The campaign submitted its allowed number of distinct trials. |
| `exhausted` | The proposer has no more work for the available slots. A plain resume reuses this outcome while the history is unchanged. Add `--retry-exhausted` to the run command to ask the proposer again within the remaining budget. Worker retries follow `max_attempts`. |
| `target` | The selected score reached the target in the declared direction. |
| `time_limit` | Wall-clock time since campaign creation reached `max_hours`. Inspect completed attempts and queued work. |
| `proposal_timeout` | The proposal call reached its deadline and the loop had no further work. |
| `dispatch_unavailable` | Check capacity, in-flight reservations and the configured budget. A temporary token-rate limit uses a bounded wait before retry. |

Read `selected` for the incumbent. Read `top` for the ranked search
observations. A held-out policy can reject the top-ranked recipe. Keep both
fields when reporting the outcome.

## Read campaign KPIs

Export a report for the same campaign ID and queue. Choose a new output path
for each capture. Exporting reads existing observations and writes an HTML
file you can open offline. Expand **Campaign KPIs** in the report.

~~~sh
uv run --no-sync research report view --db .research/program.sqlite \
  --campaign program-normalization --output .research/program-kpis.html
~~~

For machine-readable results, read the snapshot from the same queue. Each KPI
has a value, a unit and a detail explaining the source or missing evidence.
Keep a `null` value and its explanation when exporting a report.

~~~sh
uv run --no-sync python - <<'PYTHON'
import json
from pathlib import Path
from research_swarm.swarm.reader import CampaignReader

snapshot = CampaignReader(Path(".research/program.sqlite")).snapshot("program-normalization")
print(json.dumps(snapshot["kpis"], indent=2))
PYTHON
~~~

| KPI key | How to interpret the campaign evidence |
| --- | --- |
| `time_to_first_trial` | Seconds from campaign creation to the first acknowledged workload start. Queues without a launch record report `null`. For a cold-start qualification, also time preparation and provisioning before campaign creation. |
| `trials_per_hour` | Distinct trials that reached a terminal outcome in the 60 minutes ending at capture. A failed attempt that was retried does not add another completed trial. |
| `gpu_utilization` | Sum measured busy device-seconds across the campaign’s dedicated allocations, including measured idle time in the denominator. Divide by total allocated device-seconds and multiply by 100. Inspect coverage and open allocations before using the value. |
| `cost_per_0_01_improvement` | Multiply metered campaign cost by 0.01 and divide by the positive gain in the objective direction. Check the first and current selected scores at the same fidelity. Older reports can mix screening and full-fidelity selections. Missing comparable scores, no net gain or an unpriced free run leave cost per comparable gain unmeasured. Pending estimates are excluded. Invoiced dollars require billing evidence. |
| `failed_attempts_by_class` | Percentages use finished attempts as the denominator. Retries are separate attempts. An unclassified failed or lost observation has no recorded cause. |
| `proposer_acceptance_rate` | Percentage of candidate entries returned by the controller’s proposer that created a new trial. Duplicate and invalid entries count as rejections. A timeout returning no entries adds none. Direct submissions and worker retries are outside this count. |
| `confirmation_pass_rate` | Percentage of distinct, campaign-bound comparisons that reached confirmation with a measured gain and passed. Reference decisions, selection vetoes and evaluator failures are excluded. A completed campaign can have zero eligible decisions. |
| `proxy_to_target_slope` | Slope fitted to the mean proxy and target score for each calibration variant, from the study bound to this campaign. Read `seed_noise` in target-score units alongside it. A flat proxy gives a null slope even when seed noise is known. |

### Reconcile a captured report

For a queue with the telemetry columns, set `:campaign` to the same campaign
ID you passed to `research report view`. In the SQLite shell, open the queue
with `sqlite3 -readonly .research/program.sqlite`, then run
`.parameter set :campaign program-normalization`.

~~~sql
SELECT id, kind, proposed_count, accepted_count, panel_evidence, gpu_time
FROM swarm_events
WHERE campaign_id = :campaign
  AND kind IN ('created', 'proposal_batch', 'proposal_timeout',
               'panel_calibration', 'panel_decision',
               'gpu_allocation_started', 'gpu_device_interval', 'gpu_allocation_finished')
ORDER BY id;
~~~

- **Proposals.** Suppose the proposer returned five entries and three created trials.
  The snapshot shows three accepted, two rejected, a fraction of 0.6 and a report
  value of 60%. With complete coverage and zero returns, the counts are zero and the
  percentage is null. A campaign created before proposal accounting has unknown
  lifetime counts, even if it records a later batch.
- **Confirmation.** A reference decision followed by one passing and one failing
  measured comparison gives one pass in two decisions, or 50%. Filter by the
  campaign’s study binding and count comparisons, including cached outcomes attributed
  to this campaign. The number of panel looks can differ. Missing or incomplete
  attribution leaves the rate unavailable. Use `recorded_passes` and
  `recorded_decisions` for the observed subset. `last_confirmation` gives the latest
  eligible comparison’s promotion decision and measured gain.
- **Calibration.** Variant means of 0, 1 and 2 on the proxy and 0.05, 2.05 and 4.05 on
  the target give a slope of 2 target units per proxy unit. Use the bound calibration
  stored in the queue. The `seed_noise` field is the square root of the mean
  within-variant target variance across seeds. A smoke run has no calibration.
  Opening the report requires no new panel evaluation.
- **GPU time.** If one allocated second contains 0.5 busy seconds and the next 1.5
  seconds are measured idle, the result is 20% over 2.5 device-seconds. An entirely
  measured idle allocation gives 0%. A CPU run, missing samples or an unfinished
  allocation leave the percentage unavailable. Inspect `coverage`,
  `busy_device_seconds`, `covered_device_seconds`, `allocated_device_seconds` and
  `open_allocations` in the snapshot.

For `resource: gpu` on a dedicated local CUDA device, set `options.backend.device` to its full GPU UUID
and `options.backend.sample_gpu_time` to `true`. The sampler reads device utilization
from first dispatch through drain, including idle, staging and retries during the
allocation.

These generated numbers demonstrate how to reconcile a report. A hardware
utilization claim requires measurements from the actual allocation. A private
campaign result requires its own held-out confirmation evidence.

## Inspect a running queue

Open the queue read-only. Set both the queue path and the campaign ID. Several
campaigns can share a queue, including different runs of the same workload.

~~~sh
uv run --no-sync python - <<'PYTHON'
from contextlib import closing
from pathlib import Path
import sqlite3

queue = Path(".research/program.sqlite").resolve()
campaign = "program-normalization"
queries = [
    """SELECT campaign_id,status,count(*) FROM swarm_trials
       WHERE campaign_id=? GROUP BY campaign_id,status""",
    """SELECT id,kind,trial_id,attempt_id FROM swarm_events
       WHERE campaign_id=? ORDER BY id DESC LIMIT 12""",
    """SELECT a.trial_id,a.number,a.worker_id,json_extract(a.observation,'$.status'),
              json_extract(a.observation,'$.error'), a.attempt_id,
              json_extract(a.observation,'$.failure_class')
       FROM swarm_attempts a JOIN swarm_trials t USING(trial_id)
       WHERE t.campaign_id=? ORDER BY a.started_at DESC LIMIT 12""",
]
with closing(sqlite3.connect(queue.as_uri() + "?mode=ro", uri=True)) as db:
    for sql in queries:
        for row in db.execute(sql, (campaign,)):
            print(*row)
PYTHON
~~~

A queued trial awaits a claim. A running trial has an active attempt.
Several attempts can belong to one trial. Active attempts have empty
observation fields.
The heartbeat and accrued cost fields give the latest recorded progress.

Match a `repeated_failure` or `score_target` alert's `event_id` to `swarm_events.id`
in the same campaign. The queue stores the event time in `swarm_events.at` as Unix
seconds. In the notification journal and on stdout, `at` gives the time of the
watcher's poll. A `score_target` alert also gives the original selection time in
`selected_at`. Both notification timestamps include the offset for `--timezone`,
which defaults to UTC.

## Diagnose failures

| Symptom | Action |
| --- | --- |
| Nonzero task exit | Read the attempt's diagnostic output. Reproduce the same recipe, seed and fidelity locally. |
| Missing objective | Check the metric name and the JSON written to `SWARM_METRICS`. |
| Trial timeout | Check the attempt's `failure_class`, `error` and any saved `logs` against `trial_timeout_seconds` in the YAML (default 300 seconds). A retry uses the same trial ID. |
| Proposal timeout | Look for a `proposal_timeout` queue event. The YAML field `proposal_timeout_seconds` defaults to 10 seconds. That call adds no candidates. Already submitted trials can finish. |
| Lost attempt | Inspect the heartbeat and backend status. Check the remaining retry allowance. |
| Capacity unavailable | Inspect active slots and device allocations. Compare estimated cost with the available budget. |
| No selected candidate | Check for successful finite metrics, enough distinct seeds and a passing held-out decision. |

An attempt failure belongs to that attempt. A late report from an expired
claim cannot overwrite its replacement. Use a new attempt under the
configured retry rule when another execution is needed.

For `research swarm advise`, inspect the status of each reply. A hard
`--timeout` (default 10 seconds) returns `timed_out`. A provider can return
`failed` before that deadline. Either outcome gives exit status 3. For an
abandoned job, inspect `outcome.json` beside its heartbeat. [Advisory diagnostics](campaign-ops.md#get-bounded-advice)

## Stop and resume a local run

Press Ctrl-C in the terminal running the foreground campaign. The command
exits with status 130. Inspect the queue for interrupted attempts, then
repeat the original run command with the same spec and queue path.
Eligible trials retry within `max_attempts` (default 2, including the first).

Resume a calibrated campaign with its original bound study. If calibration
was interrupted, use the panel query below before restarting. A changed
workload or budget needs a new campaign ID. A changed scorer revision,
split, calibration recipe or decision rule needs a new campaign ID and study.
Keep the original measurements for comparison.

## Inspect the panel study

The supplied YAML campaign uses `benchmarks.study:worker_policy`. Its study
lives under `.research/panels/` in the task directory. The filename is the
SHA-256 digest of the campaign ID followed by `.sqlite`.

A look reservation counts an exposure even if evaluation later fails.
Read the cumulative counts for overuse and check the reason for each
promotion decision. A rejected selection saves the confirmation cases.
Confirmation can also reject a candidate that passed selection.

Run this from the repository root for the supplied program campaign. For
another campaign using the same policy, set its ID and use its task directory.
The query reads the saved calibration, exposure counts and promotion decisions.

~~~sh
uv run --no-sync python - <<'PYTHON'
from contextlib import closing
from hashlib import sha256
import json
from pathlib import Path
import sqlite3

campaign = "program-normalization"
filename = sha256(campaign.encode()).hexdigest() + ".sqlite"
study = (Path(".research/panels") / filename).resolve()
with closing(sqlite3.connect(study.as_uri() + "?mode=ro", uri=True)) as db:
    saved, reserved, scored = db.execute("""
        SELECT EXISTS(SELECT 1 FROM panel_state WHERE key = 'calibration'),
               count(*), count(score)
        FROM panel_looks WHERE role IN ('calibration_proxy', 'calibration_target')
    """).fetchone()
    print("calibration saved", bool(saved), "reserved", reserved, "scored", scored)
    for role, count in db.execute("SELECT role,count(*) FROM panel_looks GROUP BY role"):
        print(role, count)
    for recipe, decision in db.execute("""
        SELECT json_extract(key, '$[1]'), value FROM panel_state
        WHERE json_type(value, '$.promoted') IS NOT NULL ORDER BY rowid
    """):
        print(json.dumps({"recipe": json.loads(recipe), "decision": json.loads(decision)}))
PYTHON
~~~

Keep the original queue and study. With the calibration recovery fix, use
the query output to choose the next action. [Recovery fix](https://github.com/vincentkoc/autoresearch-swarm/pull/143)

- **Saved summary.** Resume with the original run command. The policy reuses the fit.
- **Partial fit with a finite score for each unique reservation.** Resume with the
  same command. The policy reuses each stored role, recipe and seed score and
  evaluates the missing combinations. The recovered fit reports `null` for
  seconds per trial and trials per second because earlier timings are unavailable.
- **More reservations than scores.** The unscored look is spent. Resume stops
  before trial dispatch. Record the reported role and seed, keep the ledger and
  investigate the scorer. Duplicate scores for a role, recipe and seed also stop the fit.

On revision `d39c765`, restarting a partial calibration could score completed
cases again. Preserve those recorded looks when interpreting older runs.

Standalone benchmark comparisons report `calibration`, `panel_looks` and
`finalists` in their JSON output. Their studies live under
`.research/benchmarks/`. The default v1 program comparison uses
`program-cpu-small.sqlite` in that directory.

An unreadable result after a confirmation reservation leaves that look
consumed. Preserve its study file. Inspect the failed scoring operation
before designing a new experiment.

## Account for resources

Set CPU and LLM slots to the concurrency the host can sustain. GPU capacity
also depends on memory. Measure peak VRAM for the task's data and model size
before increasing the number of simultaneous fits.

Use observed input and output token counts from the proposer or worker.
Use measured elapsed time for compute charges. Inspect the configured rates
alongside the reported currency amount.

Cold calibration can include model loading or framework initialization.
Compare small and full profiles separately. Keep the profile, device and
training seed with every throughput measurement.

## Operate a backend

Local workers execute the task in a process group. On timeout the backend
closes the group and captures recent output. Stage a shared dataset before
dispatch and pass its path through the backend's data configuration.

An SSH pool uses explicitly configured hosts. Its keeper can stop an idle
host. Managed jobs can retry placement through the provider integration.
Check the campaign inventory and job identity before changing remote state.

`research campaign gc CAMPAIGN_ID` lists resource inventory by default.
It can query a provider for recorded managed jobs. Use `--volume` to include
a prepared data volume. Cleanup requires `--apply`. Use `--keep ID` to
preserve a listed resource. For a local interruption, follow the
[stop and resume instructions](#stop-and-resume-a-local-run).

Local tests verify staged-input integrity, retry under the same trial ID,
duplicate-report handling and capacity return. The SSH and managed-job tests
use local simulations. Cold provisioning, real spot recovery, utilization
across allocated devices and provider lifecycle events require measurements
on the target infrastructure. [GPU qualification checklist](gpu-qualification.md)

## Read earlier records

Older controller commands use receipts and a separate event model. Open
their versioned reference when inspecting an existing manifest-based run.

[Earlier controller guides](https://github.com/vincentkoc/autoresearch-swarm/tree/b8b44174b925483c8b202163fef12b733517fa49/docs)
