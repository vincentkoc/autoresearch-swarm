# Campaign view

The campaign workspace is the only HTML view: About on the left, the scored
experiment view in the center, and leaderboard/activity on the right.
Resize or collapse either sidebar. Click or keyboard-select a graph point to
inspect the exact candidate and score.

## Hill climb / Graph

The center opens on **Hill climb**. Switch to **Graph** in the same workspace;
About, leaderboard, activity and resized/collapsed sidebars remain in place.
The tabs support Left/Right and Home/End keyboard navigation.

- **Hill climb** plots individual eligible evaluation receipts chronologically by
  controller commit time, with an independent running-best step curve for the
  selected campaign. Minimize/maximize follows the declared metric. Switching
  campaigns switches the curve, baseline and provenance together: runs are never
  pooled. The last best holds to capture time without inventing another evaluation.
  Original baselines, excluded receipts, missing timing and incomplete captures
  remain explicit. No candidate-order chronology is substituted for missing times.
- **Graph** draws arrows only from captured parent IDs to their children; it never
  connects adjacent observations or infers parentage from scores. The score axis
  retains unscored candidates on a separate rail. Nonselected edges fade by
  generation age: maximum captured depth minus child depth, where depth is the
  longest known parent path. Opacity is `max(0.08, 0.5 × 0.8^age)`.
  The initial qualified-selection/best-observed ancestry stays bright. Selecting
  a point highlights only its captured ancestors at 0.95 opacity; this is inspection,
  not promotion. Cycles suppress ancestry highlighting and generation-age inference.
  Missing parents and truncated parent lists are disclosed, never reconstructed.

The reusable Python API remains
`research_swarm.control.report_view.render_report_html(snapshots,
comparison_digest=..., metric=..., labels=..., project_repository=...)`.
No new data field or mode argument is required; both views use the same bounded
snapshot and exact comparison/metric selection. Live adapters can render new
captures with this API without changing training, scheduling or controller state.

## Serve with the existing API

Start the normal server:

~~~sh
research --state-dir /path/to/state serve --host 127.0.0.1 --port 8765
~~~

Open `http://127.0.0.1:8765/` and enter an existing read-scoped bearer credential.
The shell contains no campaign data before authentication. It keeps the
credential only in page memory, never in a URL, cookie or browser storage.
Disconnect clears the credential and displayed evidence; reloading requires
reconnecting. Remote deployments should use their existing authenticated TLS
ingress.

The browser follows every page of the workspace-scoped campaign list. A
single campaign loads directly; multiple campaigns appear in the picker.
An empty store shows an empty state. Select a campaign to update the entire
workspace, not just its graph. If captured evidence contains multiple comparison
cohorts, a second selector keeps their scores separate.

Refresh reads current evidence. It does not start jobs, create immutable reports,
append events, or change the campaign. Each render uses a bounded capture and
discloses incomplete history; it is not a live worker-concurrency monitor.
The API serves `/v1/campaigns`, `/v1/campaigns/{id}/snapshot`, and
`/v1/campaigns/{id}/view` with the existing read authorization. The HTML endpoint
requires an explicit `comparison_digest` when several cohorts exist.

## Open a captured export

~~~sh
research report view --file report.json \
  --comparison-digest sha256:COMPARISON_DIGEST \
  --metric validation_loss --unit nats --direction minimize \
  --output campaign.html
~~~

Open the file directly. A single export loads as-is, without a server or
credential. Multiple explicitly supplied reports use a local picker and switch
About, graph, leaderboard and activity together. File exports do not discover
other campaigns or make network requests.

Add `--project-repository https://github.com/OWNER/REPO` when a captured export
should link its main project in About. The Swarm repository link is always
present. Repository attribution is never guessed.

The standalone `report plot` SVG export remains a compatibility utility; it is
separate from the two modes inside the campaign workspace.
