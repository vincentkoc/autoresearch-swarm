# CLI and agent control

The CLI, Python SDK, HTTP API, and MCP tools call the same command service.
Each mutation has an idempotency key. Revision-sensitive commands also require
the revision returned by the latest read. An accepted command is a durable
acknowledgment; it does not mean that an experiment has finished.

## Start locally

~~~sh
uv sync --group dev
uv run research init
uv run research capabilities
uv run research project register examples/cpu_bigram --key bigram-source-v1
~~~

The default private state directory is `~/.local/state/autoresearch-swarm`.
Use `research --state-dir PATH ...` to select another runtime. Initialization
preserves its identity and credentials across invocations.

Send a command as a JSON file or through stdin:

~~~sh
uv run research command campaign.create --file campaign.json --key campaign-v1
uv run research command campaign.seal --file target.json --key seal-v1 --revision 0
uv run research query campaign --limit 10
uv run research context CAMPAIGN_ID
uv run research budget CAMPAIGN_ID
uv run research events --after 0 --limit 50
~~~

`campaign.json` uses the `campaign.create` request schema returned by capabilities.
`target.json` contains `{"campaign_id":"CAMPAIGN_ID"}` with the actual UUID.
Use the actual campaign revision for seal. All successful output is JSON on
stdout. Domain errors are JSON on stderr with exit status 2. Parser usage errors
use argparse's ordinary stderr format. The CLI never prints its credential.

## Expose agent tools

~~~sh
uv run research mcp
~~~

This command serves MCP over stdio. Configure an MCP client to launch it with the
repository as its working directory. For clients that require an absolute project
location, use `uv run --project /path/to/autoresearch-swarm research mcp`.
Do not use the MCP stdout channel for shell banners or logging.

The tool catalog includes typed commands for project registration, campaign
creation/validation/sealing/start, candidate proposal/validation, and trial
submission. Query tools expose capabilities, bounded campaign context, records,
budgets, and events. Command results include the durable command ID. Structured
errors retain the domain error code and retry information.

The local `research_capture_project` tool reads only manifest-selected source
and requires `source.capture` authority. Registration remains a separate command
that accepts the resulting immutable ArtifactRef. Capture preserves the selected
Git revision or declared working files; it does not change the research checkout.

Tools return summaries by default and cap replies at 128 KiB. Use cursors,
smaller pages, or the HTTP API for large details and artifact transfer. A tool
result bound does not imply that the campaign's full history fits in one prompt.

## Remote control

~~~sh
uv run research serve --host 127.0.0.1 --port 8765
uv run research --controller https://controller.example.invalid --token-file PATH capabilities
uv run research --controller https://controller.example.invalid --token-file PATH mcp
~~~

The server requires bearer authentication. The remote CLI reads a private token
file; raw secrets do not belong in argv. Remote MCP forwards the same API commands
and does not expose local source capture. Use a scoped credential for an agent.
Revocation takes effect on the next operation, without operator-token fallback.

The stdio server and CLI are clients of durable state. Closing them does not
cancel an accepted campaign. Trial graphs can execute through an explicitly
enrolled `LocalWorker` and the async `LocalDriver`; `research run` bootstraps
the original local workload flow. Automated hypothesis search is separate.

Real subprocess tests exercise stdio discovery and tool calls, CLI JSON input,
cross-transport replay, populated context/query output, conflict responses, scoped
capture refusal, and credential revocation during a live MCP connection.

## Durable external proposals

A proposal request assigns research work to one principal. It freezes an eligible
incumbent, parent source, seed, instructions, bounded evidence context, event
watermark, and controller epoch. It does not execute an LLM call. Any external
agent can answer through the shared command API, including a terminal coding
agent, an MCP client, or a provider-specific application.

Create the campaign with an explicit cumulative request limit:

~~~json
{"dimension":"proposal_requests","limit":"20","kind":"cumulative","unit":"count"}
~~~

Include execution limits as well. The request allowance counts issued research
requests, not provider calls, tokens, or money. Each accepted request immediately
spends one count. Cancellation, agent failure, stale output, and retries do not
refund it. Missing limits, wrong units, and capacity/exposure limits are rejected.
Sealed existing campaigns are not silently amended.

The protocol is:

1. Establish the evaluated incumbent through the promotion API.
2. Read campaign context. Its `search_state` is null before the first request;
   use search revision `0` then. Otherwise use the returned search revision.
3. Call `search.request_proposal` with that revision and the exact incumbent
   revision, controller ID, epoch, assigned proposer principal, seed, and
   instructions. Its command scope is `search.manage`.
4. Deliver the returned request ID to that agent. The assigned principal reads
   it using `proposal.get` and answers using `search.respond`, both under
   `proposal.respond` authority.
5. Keep the response as durable research evidence. A controller can then admit
   it atomically through `search.admit_response`; execution, evaluation, and
   promotion retain their own authority.

~~~python
from research_swarm.control.search_models import ProposalRead, RespondToProposal

view = agent.proposal_get(ProposalRead(request_id=request_id))
response = RespondToProposal(
    request_id=request_id,
    outcome="candidate",
    source_artifact=uploaded_source,
    hypothesis="Test the proposed optimizer change under the frozen protocol",
)
ack = agent.command("search.respond", response, idempotency_key="proposal-response-v1")
~~~

A candidate response requires an ArtifactRef and hypothesis. A failed response
requires a finding and no source. Optional trace artifacts and model, provider,
prompt digest, and usage fields preserve reported provenance. These fields carry
`agent_reported` authority and cannot settle verified provider costs or establish
a scientific result. The response path verifies artifact availability and exact
identity; source structure and permitted mutations are checked during candidate
admission. Thus an invalid source bundle can still be retained as agent output.

`proposal.respond` grants access only to that principal's assigned requests and
responses. It does not grant general workspace reads, source capture, artifact
upload/download, candidate admission, execution, or promotion. Assign any needed
source/artifact permissions explicitly, or pass prepared artifacts from a trusted
caller. Request IDs are delivered by the caller; agent inbox polling is not yet
implemented.

~~~sh
research --token-file AGENT_TOKEN_FILE proposal get REQUEST_ID
research --token-file AGENT_TOKEN_FILE command search.respond --file response.json --key reply-v1
~~~

The equivalent HTTP read is `POST /v1/proposal/get` with a `request_id` body.
The MCP read is `research_proposal_get`; command tools are
`research_search_request_proposal`, `research_search_respond`, and
`research_search_cancel_proposal`. The read returns the full frozen context,
subject to the ordinary MCP response bound. Generic list/context views contain
bounded summaries and pending proposal IDs.

Responses are immutable and keyed by request ID. Replaying the same command
recovers the original acknowledgment; submitting identical output with a new key
returns the original response. Changed output conflicts. Arrival does not advance
search state, so competing policy computations cannot erase an already recorded
response. Cancellation, pause, epoch changes, and incumbent correction preserve
late output with `late_at_recording=true`. This flag records arrival conditions;
future admission must check current eligibility again.

`search.cancel_proposal` requires search revision CAS and records the reason. It
releases the request slot while retaining the spent budget, immutable response,
and artifact retention. It neither claims to stop an external agent nor kills a
process. Greedy requests occupy one slot; `external-v1` permits up to the sealed
`max_parallel_trials` across pending requests and policy-owned active trials.
A response continues to occupy its slot until cancelled or consumed by a policy.

Request/response persistence and explicit atomic admission are implemented.
Automatic observation consumption and a repeating policy driver are separate
work; recording a response alone does not queue an experiment.


## Atomic proposal admission

`search.admit_response` consumes one recorded response against the current search
revision. Supply `campaign_id`, `request_id`, expected incumbent revision, and
expected controller ID/epoch. Its header revision belongs to `search_state`,
not the campaign. The CLI command and `research_search_admit_response` MCP tool
use the same schema and transaction as the SDK/HTTP command.

The caller needs `search.manage`, `candidate.propose`, `candidate.validate`,
`trial.submit`, and `read`. These permissions are checked by the service itself,
including direct Python calls. The response's authenticated agent remains the
candidate's proposer; the transition separately records its committing actor.

Source verification runs outside the controller write transaction while holding
an artifact pin. Admission then rechecks current authority, campaign state,
incumbent evidence, original parent, response ownership, pending membership, and
search revision. A source parse failure leaves the recorded response pending.
An epoch or parent change requires a new proposal context; supplying refreshed
expected values cannot silently rebase the old response.

One transaction records exactly one of four outcomes:

| Outcome | Persisted result | Work admitted |
|---|---|---|
| `admitted` | Candidate, valid mutation receipt, immutable trial/stage graph, policy transition | One trial, occupying the original proposal's search slot |
| `invalid` | Candidate, invalid mutation receipt, policy transition | None; request slot released |
| `duplicate` | Transition referencing the existing same-campaign source tree | None; request slot released |
| `agent_failed` | Transition and original failure finding | None; request slot released |

Duplicate detection includes pending candidates. It compares canonical source
identity within one campaign, not natural-language similarity or score. Explicit
replication uses trial/confirmation APIs rather than resubmitting the same source
as a new exploratory proposal. An invalid protected-file mutation is retained
without a trial; a malformed/unavailable bundle remains unconsumed because its
candidate identity could not be verified.

The same transaction removes the pending request, adds any admitted trial to
policy-owned active work, advances search revision, and appends its event. A
trial-limit, seed, capacity, or revision refusal rolls back every new candidate,
validation, trial, slot change, and transition together. It preserves the earlier
response and spent proposal-request count. No training or LLM call runs inside
the transaction.

Retry uncertain admission with the original key, payload, principal, and revision.
An accepted replay survives source cleanup failure, a competing delivery, or
subsequent control changes. A new key cannot consume the same response twice.
Cancellation after consumption cannot erase an admitted trial; use the trial's
existing worker/campaign controls instead.

An admitted trial still needs independent execution and authenticated evaluation.
Admission does not change the incumbent or label the candidate a win. Its search
slot remains occupied until policy observation and selection resolve that trial;
scientific completion alone does not silently advance policy state.
