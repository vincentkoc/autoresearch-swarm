# Operate a campaign

Run a campaign from the checkout that contains its task and plugin factories.
Keep its YAML spec, queue database and panel study together for resumption.

[Run a campaign](run-a-campaign.md) · [Design](design.md)

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

## Inspect a running queue

Open the queue read-only. Replace the path below if the spec uses a different
database. Add a campaign filter when a queue contains several campaigns.

~~~sh
uv run --no-sync python - <<'PYTHON'
from contextlib import closing
from pathlib import Path
import sqlite3

queue = Path(".research/program.sqlite").resolve()
queries = [
    "SELECT campaign_id,status,count(*) FROM swarm_trials GROUP BY campaign_id,status",
    "SELECT id,kind,trial_id,attempt_id FROM swarm_events ORDER BY id DESC LIMIT 12",
    """SELECT trial_id,number,worker_id,json_extract(observation,'$.status'),
              json_extract(observation,'$.error')
       FROM swarm_attempts ORDER BY started_at DESC LIMIT 12""",
]
with closing(sqlite3.connect(queue.as_uri() + "?mode=ro", uri=True)) as db:
    for sql in queries:
        for row in db.execute(sql):
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

The benchmark CLI returns `calibration`, `panel_looks` and `finalists`.
Its default file lives under `.research/benchmarks/`. A worker panel policy
uses a campaign-specific file under `.research/panels/` in the task directory.

A look reservation counts an exposure even if evaluation later fails.
Read the cumulative counts for overuse and check the reason for each
promotion decision. A rejected selection saves the confirmation cases.
Confirmation can also reject a candidate that passed selection.

Use the exact study path for a read-only query.

~~~sh
sqlite3 -readonly .research/benchmarks/program-cpu-small.sqlite \
  'SELECT role,count(*) FROM panel_looks GROUP BY role;'
~~~

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

Rehearse staging, recovery and cleanup locally before measuring time to first
trial and device utilization on hardware. [GPU qualification checklist](gpu-qualification.md)

## Read earlier records

The native controller SQLite adapter supports `Database.backup(destination)`.
Choose a new destination path. The operation writes a consistent image of
the database and refuses an existing file. Back up external artifacts
separately. Preserve the swarm queue and its panel study as separate stores.

Older controller commands use receipts and a separate event model. Open
their versioned reference when inspecting an existing manifest-based run.

[Earlier controller guides](https://github.com/vincentkoc/autoresearch-swarm/tree/b8b44174b925483c8b202163fef12b733517fa49/docs)
