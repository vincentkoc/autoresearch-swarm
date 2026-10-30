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

## Observe results and resolve a policy trial

Agents do not write their own scientific observations. `search.observe_trial`
reads an accepted trial and its evidence in one database snapshot. Send its
campaign/trial IDs with the current **trial revision** and `search.observe`
authority. The returned immutable record contains the source revision, spec,
attempt outcomes, evidence references, comparison context, and event watermark.
Identical evidence and comparison context return the same observation identity,
even with another delivery key. Observing does not advance search state.

| Observation | Meaning |
|---|---|
| `measurement` | An authenticated evaluator receipt and all execution dependencies are currently eligible; includes the primary metric and comparison digest |
| `execution_failed` | An accepted attempted stage failed; no scientific score |
| `cancelled` | Accepted attempted-stage evidence establishes cancellation; failed dependents do not turn it into model failure |
| `not_started` | Attempted stages never reached accepted execution; no invented exit or score |
| `execution_unknown` | Native/controller evidence remains unresolved; the policy slot stays occupied |
| `closed_unaccepted` | Operational closure was accepted while research results were explicitly withheld |
| `evaluation_invalid` / `evaluation_incomplete` | Evaluation did not produce a complete eligible panel |
| `evidence_invalidated` | A correction invalidates evidence that was previously recorded |
| `unrankable` | Evidence exists but current eligibility, artifact, or comparison requirements prevent ranking |

An unfinished trial or completed execution awaiting evaluator authority does not
become a score. The command returns an explicit pending/incomplete error. Closed
failures, cancellation, and unavailable evidence are retained as separate
categories, never assigned a zero loss. Attempt outcomes remain available for
mixed-stage diagnosis; the summary is not a causal claim about a failed model.

For a measurement, assess through the existing promotion API. Keep the original
observation ID and comparison context. Commit a positive assessment separately;
a rejected assessment needs no promotion. For a paired campaign, execute and
assess its sealed confirmation panel. Its fixed exposure and one-panel rules
remain in force. A single exploratory measurement cannot substitute for that
panel, and an inconclusive assessment cannot become a win.

Then call `search.resolve_trial` with the observation ID, optional decision ID,
current controller ID/epoch, and **search revision**. It requires `search.manage`.
The resolver rechecks the observed trial facts and current evidence dependencies.
Scientific resolution must match the observed candidate and original comparator
context. A positive decision must already be committed. A rejected decision
must be an intact assessment. Paired decisions must match the complete sealed
plan; observed-mode decisions must reference the exact exploratory receipt.

One transaction records the resolution transition, consumes its ordered
observation input, removes the active trial, advances the search revision, and
increments `generation`. In this first policy implementation, generation counts
resolved policy trials; it is not a count of validated improvements. Resolution
does not change an incumbent, accept native execution, or settle resource costs.
Unknown execution cannot resolve or free the policy slot.

The transition's outcomes are `promoted`, `rejected`, `inconclusive`, and
`non_scientific`. A closed failure requires no decision ID and records the last
category. Cancellation of a policy request is still separate from resolution of
an already admitted trial.

Exact command replay recovers the original resolution. Another key cannot consume
the same observation again, and concurrent search revisions cannot both advance.
If evidence changes after observation, preserve the old record and observe the
new revision. Affected assessments cannot resolve as valid wins. Existing
incumbent suspension continues to prevent new proposals.

The corresponding tools are `research_search_observe_trial` and
`research_search_resolve_trial`; the CLI uses the ordinary `command` route.
`search_observation`, `observation_consumption`, and `policy_transition` support
the existing bounded record queries. Campaign context includes recent observation
and transition summaries. A repeating deterministic search driver is the next
layer over these commands; external agents can already run this explicit loop.


## Advance a greedy research loop

Create a sealed greedy campaign with the optional bootstrap policy:

~~~sh
research run examples/cpu_bigram --key greedy-run --search-policy greedy --max-trials 10
research search plan CAMPAIGN_ID --proposer AGENT_ID --baseline-trial BASELINE_TRIAL_ID
research search advance CAMPAIGN_ID --proposer AGENT_ID --baseline-trial BASELINE_TRIAL_ID
~~~

`run` executes the baseline. `search plan` only reads state. `search advance`
performs at most 32 commands by default (maximum 128), returns JSON, and can be
invoked again after interruption. Use the same options after a restart. Existing
`run` invocations retain the external policy and original bootstrap identity.
Greedy campaigns declare a proposal-request allowance of `max_trials - 1`.

The driver assesses an explicitly selected baseline, requests a proposal from
its assigned agent, admits a durable response, observes the completed trial,
and advances independent selection. External agents still use `proposal get`,
`search.respond`, source capture/upload, or the corresponding SDK/MCP tools.
The driver does not call an LLM provider or supply an agent's response.

Workers and authorized evaluators execute separately. Run the local worker
between advances, or supply independent scoped workers and evaluator control.
Closing the CLI does not cancel native work. The driver performs no process
launch, worker claim, or evaluator publication.

The Python interface works with both local and HTTP clients:

~~~python
from research_swarm.orchestration.search import SearchDriver
from research_swarm.search.greedy import GreedyConfiguration

driver = SearchDriver(
    client,
    campaign_id,
    GreedyConfiguration(
        proposer_id=agent_id,
        baseline_trial_id=baseline_trial_id,
        first_seed=100,
    ),
)
next_action = driver.plan()
progress = driver.advance(max_steps=32)
# Async callers can await driver.advance_async(max_steps=32).
~~~

The planner is pure: the same snapshot and configuration produce the same
command and key. Reads are bounded; an incomplete evidence scan returns
`needs_review`. A lost response returns `delivery_uncertain`; the next invocation
reads accepted state before deciding what to do. Independent principals may
create equivalent assessments, but revision checks, deterministic observation
identities, and incumbent selection prevent duplicate work or policy progress.

Before assessment, `search.select_observation` pins the exact observation and
original comparator in an immutable policy transition. It requires current
controller/epoch, search revision, and `search.manage` authority. A later
observation against a newly promoted incumbent cannot replace that pin. Restart
between assessment, promotion, and resolution retains the original comparison.

Pause stops fresh search actions. A previously committed decision may finish
policy bookkeeping while paused. `target_reached` means the declared target is
met by the current eligible incumbent; paired targets use the selected replica
mean. `budget_exhausted` identifies the trial/request/resource allowance that
blocks more work. These results do not automatically complete the campaign.
UNKNOWN execution keeps its slot and returns `awaiting_execution`.

Paired campaigns wait for an explicitly sealed confirmation panel. Pending
realizations keep waiting; a settled failed panel is assessed as ineligible.
After a completed panel, the driver stops before requesting research that would
require a second panel. It never substitutes an exploratory score for paired
confirmation or creates extra protected exposures.

Corrections can invalidate a pin's trial or comparator. The driver stops for
review. An authorized caller can use `search.abandon_selection` with the pin ID,
current observation, reason, current controller/epoch, and search revision.
The controller requires closed non-scientific trial evidence, or independently
verified corrections to the pinned comparator. Comparator correction may leave
the candidate's measurement valid; that measurement stays recorded. Abandonment
appends its evidence and reason, consumes the observation, and releases only
the policy slot. It does not promote, rebase, move the incumbent, or settle
physical resource/budget custody.


## Evaluate work from independent workers

`research evaluation advance` supplies the controller/evaluator part of the
loop for workers that run separately. It never claims a job or launches a
process. Use the normal controller credentials, plus the credential of the
principal named by `--evaluator`:

~~~sh
research evaluation advance CAMPAIGN_ID --evaluator EVALUATOR_ID \
  --evaluator-token-file PRIVATE_EVALUATOR_TOKEN_FILE
~~~

The controller requires `read`, `evaluation.request`, and `artifact.upload`.
The evaluator requires `read` and `evaluation.publish`. If the evaluator token
file is omitted, the explicitly selected controller principal also serves as
the evaluator and must match the supplied evaluator ID. A failed evaluator
credential never falls back to another principal. Remote calls use the same
`--controller` and `--token-file` options as other CLI operations.

The driver first completes authorized scoring jobs whose original accepted
attempts succeeded. It then authorizes ready scoring jobs. Completed receipts
may be recovered while the campaign is paused; new scoring authority follows
running/draining admission rules. UNKNOWN or explicitly unaccepted trial
execution is not authorized. A sealed confirmation plan keeps its original
evaluator and exposure binding.

Use the same interface from Python with separate clients:

~~~python
from research_swarm.orchestration.evaluation import EvaluationDriver

evaluations = EvaluationDriver(controller_client, evaluator_client, campaign_id, evaluator_id)
result = evaluations.advance(max_pages=4, page_size=50)
while result.continuation is not None:
    if result.error_code is not None:
        break  # Inspect the reported transport or credential failure first.
    result = evaluations.advance(continuation=result.continuation)
# Call again without a continuation to scan newly eligible work.
~~~

Each call reads at most eight pages of fifty records. The returned continuation
pins the campaign, evaluator, phase, and bounded query cursor. Follow it to
avoid starving later work behind earlier unfinished requests. The CLI accepts
that continuation object through `--continuation-file`; it does not store
credentials or mutate the input file. `advance_async` exposes the same behavior
for an asyncio coordinator.

A lost reply retains the input cursor for its page. On resume, the driver adopts
already linked authorizations/receipts and processes the remaining rows. Failed
scoring and foreign evaluator ownership produce per-row findings while other
rows can continue. Credential/transport failures stop the affected control call.
A receipt with invalid or incomplete evidence is still an outcome; the driver
does not allocate retries, invent a metric, or promote a candidate. `idle` means
the scanned records needed no action, not that the campaign or workers finished.

Compose `SearchDriver`, `EvaluationDriver`, external proposal agents, and
independent worker processes to run the complete loop through the API. Their
execution, evaluation, proposal, and promotion authorities remain separate.


## Seal parallel research themes

The portfolio control contract is available through the existing campaign and
search commands. Set `campaign.create.search` to a configuration such as this,
replacing the agent IDs with registered principals:

~~~json
{
  "policy": "portfolio",
  "version": "portfolio-v1",
  "max_parallel_trials": 4,
  "portfolio": {
    "first_seed": 100,
    "themes": [
      {"theme_id": "optimizer", "proposer_id": "AGENT_A_ID", "instructions": "Explore optimizer changes"},
      {"theme_id": "architecture", "proposer_id": "AGENT_B_ID", "instructions": "Explore architecture changes"}
    ]
  }
}
~~~

Declare a cumulative `proposal_requests/count` budget as for other research
policies. `portfolio-v1` supports observed promotion and at most sixteen slots
and themes. Paired confirmation requires a separate allocation design and is
explicitly rejected for this version. Campaign validation and sealing agree on
these restrictions. Theme order is part of the immutable campaign contract.

For request ordinal `n`, use theme `n % len(themes)` and seed `first_seed + n`.
`search.request_proposal` must name the exact `theme_id`, proposer, instructions,
and seed. Issuance advances the ordinal only when its transaction commits.
Cancellation preserves the original request, response, artifact references,
and consumed request budget; it does not rewind theme allocation.

Pending proposals and admitted policy trials share the configured concurrency
slots. Pending proposals also reserve room under the total trial limit. Public
manual trial submission cannot consume that promised room. Atomic response
admission converts its own pending request into a trial in the same transaction.
Native device/resource admission remains independent.

Parallel execution has one scientific selection owner. Selecting an observation
atomically records `selection_trial_id` with its immutable pin. Other trials may
execute and be observed, but cannot select until the owner resolves or explicitly
abandons corrected evidence. `promotion.commit` enforces the same owner and exact
pinned evidence, including for direct API callers. Another authorized controller
can recover the owner; the owner identifies a trial, not a live process.

A newly initialized baseline may be committed before there is a selection owner.
Explicit replacement of suspended baseline evidence requires the current owner
to be resolved first. Neither path silently abandons an active selection.

When one trial promotes, still-pending responses retain their original parent
and comparator context. They cannot be admitted against the replacement
incumbent. Cancel the obsolete request explicitly, preserve the response, and
issue a fresh request. Already admitted trials keep their source and ancestry;
once selection becomes available, observe them against the current eligible
incumbent. The platform never relabels an old patch as a rebased proposal.

Use the same bounded driver commands to operate a sealed portfolio:

~~~sh
research search plan CAMPAIGN_ID --baseline-trial BASELINE_TRIAL_ID
research search advance CAMPAIGN_ID --baseline-trial BASELINE_TRIAL_ID --max-steps 32
~~~

Portfolio mode reads proposers, instructions, and seeds from the sealed themes.
It rejects CLI overrides for those fields. In Python, construct `SearchDriver`
with `PortfolioDriverConfiguration(baseline_trial_id=...)` from
`research_swarm.search.portfolio`. `portfolio_snapshot()` returns all pending
requests and active trial views; `PortfolioPolicy.plan(snapshot)` is pure.
The greedy driver and its existing command identities remain compatible.

Each invocation recovers the existing selection owner first. It then cancels
superseded pending requests, selects an eligible settled sibling, admits a
current response, or fills a free slot with the next sealed theme. It rereads
state after every accepted action. An unanswered proposal, UNKNOWN execution,
or incomplete bounded history scan retains only its own slot; it does not
prevent other eligible slots from progressing. A retained selection owner
prevents other promotions but does not prevent independent proposal admission.

There is no generation barrier. An admitted old-parent trial can finish after
another trial promotes; it keeps its ancestry and is compared against the new
incumbent. Late responses to cancelled requests remain evidence. The driver
never rebases a source tree or interprets cancellation as native process closure.

The driver rechecks campaign, search, and controller authority after reading
all slots. Contention returns `contended`; acknowledgment loss returns
`delivery_uncertain`. Recreate the driver and advance from durable state.
An accepted promotion can finish policy bookkeeping during a pause. New work
stops at the authoritative target, exhausted allowance, or lifecycle boundary.
Review-required evidence is never automatically abandoned. The driver neither
calls an LLM nor starts a worker: external agents, `EvaluationDriver`, and
independent workers continue to use their own scoped credentials.

The native portfolio integration test runs two registered HTTP CPU workers with
one shared two-slot resource registry and a separate evaluator principal. A
workload-created gate keeps candidate A alive while B completes, is evaluated,
and promotes. Candidate C then starts from B before A is released. The test
checks original native identities, independent receipts, source digests,
ancestry, single attempts, policy consumption, final closure, and restored
capacity. Gates precede active training; this demonstrates asynchronous native
workload overlap, not concurrent training throughput or physical fleet scaling.

## Allocate bounded child campaigns

Before sealing a delegating campaign, set `delegation.max_depth` and declare
cumulative `child_campaigns/count` and `trial_admissions/count` limits alongside
the normal execution budgets. Trial admission consumes one count, including
failed or cancelled work; a child allocation does not replenish that counter.
The funded trial allowance must not exceed the campaign's `stopping.max_trials`.
Campaigns without delegation or a declared trial counter retain their existing
contract shape and behavior.

`campaign.create_child` requires `campaign.manage` and the parent campaign's
current revision. The parent must be sealed, running, eligible, and below its
declared metric target. For example:

~~~python
from research_swarm.control.child_models import CreateChildCampaign
from research_swarm.control.models import SearchConfiguration
from research_swarm.domain.budgets import BudgetLimit

created = client.command(
    "campaign.create_child",
    CreateChildCampaign(
        parent_campaign_id=parent_id,
        name="Optimizer branch",
        limits=(
            BudgetLimit(dimension="attempts", limit=4, unit="count"),
            BudgetLimit(dimension="execution_ms", limit=50_000, unit="ms"),
            BudgetLimit(dimension="trial_admissions", limit=1, unit="count"),
        ),
        search=SearchConfiguration(policy="external", version="external-v1"),
        max_trials=1,
        remaining_depth=0,
    ),
    idempotency_key="optimizer-child-1",
    expected_revision=parent_revision,
)["result"]
child_id = created["campaign"]["id"]
~~~

Creation charges one `child_campaigns` admission and holds each granted amount
in the parent, atomically. The child starts as a draft and uses the usual
validate/seal/start commands. It inherits the exact project revision, protected
protocol, input artifacts, primary metric, promotion configuration, target, and
manual-completion policy. Its search configuration may differ, and its maximum
trial count may decrease. A positive `remaining_depth` enables further delegation
and requires a funded `child_campaigns` dimension; the global depth limit is eight.
No child request accepts project, data, evaluator, or credential overrides.

Each grant must match a parent dimension, unit, and accounting kind. Cumulative
and exposure budgets can be delegated. Native capacity and retained storage
keep their separate authorities. Confirmation exposure is inherited and funded;
a child ID does not create a fresh exposure allowance. Missing funds, ancestor
deficits, and trial allowance already promised to pending proposals reject the
whole creation transaction. Direct submissions and confirmation panels obey
the same funded trial counter. A valid response consumes its own pending promise.

Consumption and uncertainty propagate while the child runs. Unused grant funds
remain held until closure. Completing a child requires closed native work,
settled evaluation, zero reserved/uncertain balances, and closed descendants.
Completion writes the final report and releases the grant remainder atomically.
An exhausted funded trial allowance can satisfy a nonmanual stopping rule even
when descendants consumed it; all other closure requirements still apply.

`campaign.settle_child` is an explicit reconciliation operation. It accepts
`child_campaign_id`, no caller-supplied costs, and the current funding projection's
revision. Once settled, repeated delivery keys return the original immutable
receipt, even if the supplied revision is stale. Exact command replay still
requires the original payload and revision. `campaign.cancel_child` can cancel
only an unstarted draft/ready child with no admitted trials or open descendants.
It returns unused funding, but does not refund the creation count. It cannot
cancel a running native process.

Pause and completion act on the named campaign. Pausing a parent does not
implicitly cancel already allocated children. Workers, evaluation authority,
and credentials remain independently managed. Child funding grants no new
machine or API access.

`context` and JSON/Markdown reports expose incoming funding, bounded child
summaries, held funds, uncertainty, and settlement identities. Use the
`campaign_funding`, `funding_projection`, `funding_change`, and
`funding_settlement` record queries to drill into history. Parent budget totals
already include descendants: do not add child totals a second time.

This supports bounded recursive research execution. Policy-training artifacts,
held-out policy comparison, and a demonstrated RL improvement remain separate
capabilities; nested campaigns alone are not evidence of self-improvement.
