# Hill-climb SVG reports

Export trial scores against elapsed wall-clock hours with the native CLI.
The renderer is read-only, has no extra dependency, and writes standalone SVG
(no scripts, external fonts/assets, browser, or plotting service required).

## CLI

Create an immutable report through the existing `report.create` command and fetch
its full record with `research get report REPORT_ID`. Select the
`comparison_digest` from `data.snapshot.observed_cohorts`, and the exact primary
metric name, unit and direction from its eligible evaluations. An MCP/report
summary is not a full snapshot.

~~~sh
research --state-dir .research report plot \
  --report-id REPORT_ID \
  --comparison-digest sha256:COMPARISON_DIGEST \
  --metric validation_loss --unit nats --direction minimize \
  --output hill-climb.svg
~~~

Repeat `--compare-report OTHER_REPORT_ID` to overlay up to eight different
campaigns. Optionally repeat `--label NAME` once per report (primary first, then
comparisons) to name the runs. The existing `--controller` and `--token-file`
transport options work as usual. Report IDs must already exist: export does not create reports, schedule
trials, or change campaign state.

For offline export, use `--file report.json` and repeat
`--compare-file other-report.json`. Files may contain a full report record,
report content (`snapshot` plus Markdown/digest), or the snapshot itself.
Supplied report digests are verified before rendering; stale/tampered envelopes
are rejected. A digest checks content integrity, not source authenticity. Plain
snapshots are explicitly unauthenticated supplied evidence; retain the original
report and digest for provenance. Offline and live input modes cannot
be mixed. Each file is limited to 2 MiB, each snapshot to 52 receipt rows
(50 recent plus two pins), and the total to eight reports.
The output path must not exist; the CLI never overwrites files.
Success prints a JSON acknowledgment containing the path, format and report count.

## Python API

~~~python
from research_swarm.control.report_plot import render_hill_climb_svg
from research_swarm.domain.experiments import MetricDefinition

svg = render_hill_climb_svg(
    [report["data"]["snapshot"], comparable_report["data"]["snapshot"]],
    comparison_digest=selected_digest,
    metric=MetricDefinition(name="validation_loss", unit="nats", direction="minimize"),
)
~~~

`project_hill_climb` accepts the same snapshots, comparison digest and metric,
returning immutable `PlotRun`/`PlotPoint` projections for custom presentations.
`render_hill_climb_svg` returns an SVG string and accepts optional `title` and
`labels` arguments
(one nonempty display label per input report). The default is a light palette with muted trial points and teal/blue best-observed steps.
Both functions leave the snapshots unchanged. They reject duplicate campaigns
(use one snapshot per run), duplicate receipt IDs, nonfinite scores and oversized
inputs. An empty selected cohort renders an explicit no-observations view.

## Reading the chart

- Dots are eligible evaluator receipts associated with trials, not training logs.
  Hover titles retain receipt, trial, candidate, seed, commit timestamp and score.
- A step is the running best *observed* value among captured points, in the declared
  minimize/maximize direction. It is not an incumbent or a confirmed winner.
  Multiple seeds remain separate observations, not a promoted aggregate.
- Dashed references use each campaign's **original committed baseline** only when
  its receipt is eligible and matches the selected comparison digest, metric and
  timing window. Missing/invalidated/different-cohort references say unavailable.
- Overlay selection requires exact comparison digest and exact metric name, unit
  and direction. A mismatching receipt is excluded and counted, never rescaled
  or pooled. A run with no matches remains an explicitly empty run.
- The x-axis starts at each campaign's stored `campaign_created_at`.
  Each point uses its `evaluation.recorded` event's `recorded_at`, captured in
  the report as `commit_event`. This is controller-recorded wall time inside the
  committed evaluation transaction, not training time, GPU time, or throughput.
  Queueing, retries and evaluator latency are included; wall clocks may drift.
- Timestamps must be timezone-aware, within campaign creation/report capture,
  with an event sequence at or before the report watermark. Missing or inconsistent
  timing is counted and omitted, never synthesized from trial indices, duration,
  seeds or report capture time. Legacy reports lacking timing stay honest and
  empty; capture a new report to include committed event times.
- Each run shows captured/total receipts, exclusions, complete/incomplete history,
  time origin, capture timestamp, schema, campaign revision and event watermark.
  An incomplete curve is only the best of captured evidence; missing history
  could change it. The x range includes report capture times; each best-observed
  curve holds its
  last captured value to that run’s capture time so idle periods remain visible.
  This tail is not a new evaluation or an inferred improvement.
  Failed/invalid trials never become zero scores; consult the report's trial
  outcomes for their details.

There is no inferred worker count, cross-hardware leaderboard, protected/official
evaluation claim, statistical win or speedup claim. Scientific confirmation
remains in the report's existing qualified promotion cohorts.

## Synthetic demo

~~~sh
uv run python examples/hill_climb/generate.py
~~~

This generates [synthetic-demo.svg](../examples/hill_climb/synthetic-demo.svg)
from invented scores and timestamps, prominently labelled **SYNTHETIC DEMO**.
It is a presentation example, not evidence from a running campaign or fleet.
