# Campaign view

The campaign workspace is the only HTML view: About on the left, the scored
experiment graph in the center, and leaderboard/activity on the right.
Resize or collapse either sidebar. Click or keyboard-select a graph point to
inspect the exact candidate and score.

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
not an alternate mode in the campaign UI.
