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
| `exhausted` | The proposer has no more work for the available slots. |
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
| `gpu_utilization` | Run-wide percentage requires summed busy device-seconds divided by allocated device-seconds, including idle allocation. The queue records no such time series. |
| `cost_per_0_01_improvement` | Multiply metered campaign cost by 0.01 and divide by the positive gain from the first to the current recorded selection. Gain follows the objective direction. Missing scores, no net gain or an unpriced free run leave the value unavailable. Pending estimates are excluded. Invoiced dollars require billing evidence. |
| `failed_attempts_by_class` | Percentages use finished attempts as the denominator. Retries are separate attempts. An unclassified failed or lost observation has no recorded cause. |
| `proposer_acceptance_rate` | Unavailable without a campaign count of all proposals, including rejections. Submitted trials alone give no proposal denominator. |
| `confirmation_pass_rate` | Unavailable without campaign-attributed confirmation decisions and outcomes. A shared study can pool exposures from several runs or reuse a measurement without a new look. |
| `proxy_to_target_slope` | Fitted change in target score per unit of proxy score across calibration variants. The snapshot reports `null` without campaign-attributed calibration. Smoke runs omit calibration. A flat proxy has an undefined slope. |

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
              json_extract(a.observation,'$.error')
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

## Diagnose failures

| Symptom | Action |
| --- | --- |
| Nonzero task exit | Read the attempt's diagnostic output. Reproduce the same recipe, seed and fidelity locally. |
| Missing objective | Check the metric name and the JSON written to `SWARM_METRICS`. |
| Timeout | Compare elapsed time with the declared trial deadline. Inspect the last task output. |
| Lost attempt | Inspect the heartbeat and backend status. Check the remaining retry allowance. |
| Capacity unavailable | Inspect active slots and device allocations. Compare estimated cost with the available budget. |
| No selected candidate | Check for successful finite metrics, enough distinct seeds and a passing held-out decision. |

An attempt failure belongs to that attempt. A late report from an expired
claim cannot overwrite its replacement. Use a new attempt under the
configured retry rule when another execution is needed.

For a changed workload or budget, create a new campaign spec and ID.
Keep the previous measurements accessible for comparison.
Resume an interrupted campaign with its original spec and queue.

## Inspect the panel study

The supplied YAML campaign uses `benchmarks.study:worker_policy`. Its study
lives under `.research/panels/` in the task directory. The filename is the
SHA-256 digest of the campaign ID followed by `.sqlite`.

A look reservation counts an exposure even if evaluation later fails.
Read the cumulative counts for overuse and check the reason for each
promotion decision. A rejected selection saves the confirmation cases.
Confirmation can also reject a candidate that passed selection.

Run this from the repository root after the supplied program campaign. It
prints the exposure counts and each stored promotion decision with its recipe.

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
    for role, count in db.execute("SELECT role,count(*) FROM panel_looks GROUP BY role"):
        print(role, count)
    for recipe, decision in db.execute("""
        SELECT json_extract(key, '$[1]'), value FROM panel_state
        WHERE json_type(value, '$.promoted') IS NOT NULL ORDER BY rowid
    """):
        print(json.dumps({"recipe": json.loads(recipe), "decision": json.loads(decision)}))
PYTHON
~~~

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

Use the backend's stop and inventory operations for the campaign it owns.
Review the listed resources before cleanup. A failed training command
provides diagnostic evidence for a retry.

Local tests verify staged-input integrity, retry under the same trial ID,
duplicate-report handling and capacity return. The SSH and managed-job tests
use local simulations. Cold provisioning, real spot recovery, utilization
across allocated devices and provider lifecycle events require measurements
on the target infrastructure. [GPU qualification checklist](gpu-qualification.md)

## Read earlier records

Older controller commands use receipts and a separate event model. Open
their versioned reference when inspecting an existing manifest-based run.

[Earlier controller guides](https://github.com/vincentkoc/autoresearch-swarm/tree/b8b44174b925483c8b202163fef12b733517fa49/docs)
