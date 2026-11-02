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

## Interactive campaign view

Use `research report view` with the **same** input, metric, comparison and label
arguments, changing only the output extension to `.html`:

~~~sh
research report view --file report.json \
  --comparison-digest sha256:COMPARISON_DIGEST \
  --metric validation_loss --unit nats --direction minimize \
  --label 'Captured experiment' --output campaign.html
~~~

The standalone off-white page opens directly in a browser. No server, network
request, controller credential, or live-state mutation is required. The campaign workspace is the only HTML view. A fixed,
content-security-policy-hashed script handles selection and panel resizing;
report text never becomes executable JavaScript or raw HTML.

The view is a viewport-height, three-column workspace: a narrow context rail,
a dominant graph, and a leaderboard stacked above the activity feed. Light
borders divide off-white panes; IDs and scores use monospace. Budget and provenance
details expand in the context rail instead of displacing the graph.

Drag either vertical divider to resize the sidebars, or focus a divider and use
Left/Right arrows. Enter or the header's About and Leaderboard / activity buttons
collapse and restore a sidebar. Restoring a wide sidebar preserves space for the
graph. On mobile, the panels stack and the header buttons still toggle them.

The header links to `vincentkoc/autoresearch-swarm`. To also link the campaign's
main project in About, supply `--project-repository https://github.com/OWNER/REPO`
(or `project_repository=` to the Python renderer). This is optional display
attribution, not captured scientific evidence; no repository is guessed when it
is omitted. Only HTTPS GitHub repository URLs without credentials or query strings
are accepted.

The campaign graph opens directly. Its points use the best captured eligible score per candidate
on the vertical axis and captured candidate order on the horizontal axis. Parent
edges connect those points; this is not a box-and-arrow diagram. Lower values appear
higher for minimize metrics, and higher values appear higher for maximize metrics.
Candidates without an eligible score remain on a separate unscored rail; their
parent edges are dashed. Click a point, or focus it and press Enter/Space, to inspect
its exact ID and score. A campaign selector switches captured trees when comparing
several reports. The graph normalizes finite scores before positioning points,
including opposite extreme values; overflow in a baseline difference is disclosed
rather than shown as a win.
All captured
branches remain muted and visible, including rejected candidates. Teal ancestry
marks the current qualified confirmed selection only when the report's exact
comparison cohort, complete confirmation history and incumbent eligibility agree.
Otherwise it explicitly marks best-observed ancestry, preserving ties and making
no confirmed-winner claim. Parent edges come only from captured parent IDs;
missing parents, truncated lists and incomplete pages are disclosed. Cyclic
input suppresses selection-path highlighting. No missing graph is reconstructed.

Candidate rows rank each candidate's best captured eligible receipt, not an
average or statistical score. Absolute improvement is labelled in the declared
metric units, not automatically called percentage error reduction. Activity
preserves failed/unscored trials. Evaluated observations are ordered by commit
time and event sequence; proposal states are not assigned invented times or
causal attribution. Captured distinct worker IDs describe historical evidence,
never live concurrency. Captured candidate order is not a trial count or a time
axis. Standalone SVG exports retain the explicit controller-time projection.

The Python API is `research_swarm.control.report_view.render_report_html`, with
the same snapshots, comparison digest, metric, optional title and labels as the
SVG renderer. It returns a complete HTML document. The demo generator also writes
[synthetic-demo.html](../examples/hill_climb/synthetic-demo.html), containing only
invented evidence and clearly labelled **SYNTHETIC DEMO**.

### Real-browser smoke proof

The dedicated `report-view` CI job generates this synthetic demo and opens it in
the GitHub Ubuntu runner's preinstalled Google Chrome. Node 22's built-in
WebSocket/CDP support drives real campaign and sidebar interactions; no npm or core dependency is added.

~~~sh
uv run --no-sync python examples/hill_climb/generate.py
node tests/visual/report-view-smoke.mjs
~~~

The test verifies the title, computed off-white background, campaign graph
visibility, paginated campaign selection, keyboard point inspection, all captured branches,
and page errors (including CSP violations). At 1440px the compact header, rail
proportions and graph must fit one viewport, at both 1000px and 700px heights.
Both desktop and 390px mobile layouts must have no document
horizontal overflow. Real PNG captures
are written to `artifacts/report-view/` and uploaded as a synthetic-only CI
artifact, retained for 14 days. Partial captures on failure are diagnostic, not
passing proof.

Chrome must be installed: a missing executable fails explicitly, never skips.
`CHROME_BIN` accepts an exact executable path, including macOS app paths with
spaces. `REPORT_VIEW_HTML` and `REPORT_VIEW_OUTPUT_DIR` optionally override the
synthetic input file and screenshot directory when running on another host;
do not supply private campaign reports. Each run uses a fresh temporary Chrome
profile and an OS-assigned loopback CDP port, with bounded command/startup waits,
a 90-second run deadline and process/profile cleanup. This test neither starts a
controller nor accesses running experiments.
