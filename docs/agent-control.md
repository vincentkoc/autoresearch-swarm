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

## Complete agent-controlled research cycle

`tests/test_agent_research_cycle.py` qualifies the full local path through an
external stdio MCP process. The research principal can capture source, propose,
admit, observe, select, and complete research. It has neither `worker.publish`
nor `evaluation.publish`; a separate enrolled worker and evaluator produce the
measurements and authenticated receipts.

The test establishes an evaluated CPU baseline, requests one optimizer hypothesis,
captures the changed training file through MCP, and admits its frozen trial. The
MCP process then exits. Native training and independent evaluation complete while
the research agent is disconnected. A new MCP process uses the same scoped
credential and replays the original admission intent. It receives the same command
ID and trial without another execution.

The returning agent reads the original evidence, pins its observation, assesses
and commits selection, consumes the observation, and creates the terminal report.
CLI replay under the same principal returns the same final report. Exactly two
trials and four native attempts are recorded, with settled reservations. A direct
attempt by the research agent to complete the evaluator receipt is denied.

This is a real CPU and transport integration test using a deterministic proposal.
It demonstrates that an external agent can drive the complete API workflow; it
does not measure LLM proposal quality. The measured improvement is reported as
`observed`, under the synthetic project's frozen comparison contract. No GUI or
provider-specific agent runtime is required.

## Read evaluation claims

`report.create` captures the same evidence for JSON and Markdown. Each recent
evaluation includes its authorized `measurement_trust`, `exposure_class`,
evaluator identity, evaluator contract and exposure authorization. Eligibility is
recorded separately and can change in a later report after an evidence correction.
An earlier report and its digest remain unchanged.

`evaluation_claim_support` explicitly marks protected and official evaluation as
unqualified. The supported evaluator profile is authenticated. A confirmed
selection means its declared replication gate passed; it does not upgrade the
panel's access policy or turn the evaluator into an official project evaluator.

The `research` section captures the declared objective, metric direction, timing,
mutation boundary and stopping rule in the same transaction as the report. Its
baseline comes from a committed baseline-initialization decision. The oldest
candidate or trial is not treated as a baseline. Baseline and incumbent candidate,
trial and evaluator references remain included outside the recent-history window.

Candidate details compare stored source manifests, including file digests and
parent identities. They do not read the live checkout. Trial and attempt tables
retain failure, incomplete evaluation and unknown execution states, native result
references, historical worker capabilities and phase timings. A capability
fingerprint does not identify a full GPU/driver descriptor unless that descriptor
has been recorded separately. Open reservations remain visible even when the
campaign's scientific work is complete.

Every record page gives `total`, `included`, `complete` and a detail query. Source
diffs include at most 16 changed paths for each of four immediate parents; omitted
parents and paths have their own completeness flags. Detail pages share a 512 KiB
budget. Report content has a 1 MiB ceiling and fails atomically if its required
context cannot fit. `evaluation_window` distinguishes recent receipts from the
explicitly pinned selection evidence. Missing rows never imply successful work
or resource closure.

Open questions are derived from captured unresolved records. Suggested next
actions navigate that evidence; they neither grant mutation authority nor execute
search, evaluation, promotion or worker reconciliation.

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

A separate HTTP integration test releases two independent, scoped proposer
clients from a shared barrier. Both responses persist, replay with their original
command identities and leave accounting unchanged on redelivery. Neither agent
can answer the other's request. Both distinct candidates then enter the same
portfolio through the ordinary admission API.

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

## Freeze learning episodes before proposal responses

The first learning profile makes one proposal request one episode. Its episode ID
is the proposal request ID. The controller freezes the complete ordered batch
before any member has a response. It records the policy, reward specification,
development/held-out split, source artifacts, seed, context, controller epoch,
incumbent revision and comparator evaluation. Every comparator must still be
eligible and use the same experiment seed as its proposal.

Action `source_digest` means the digest of the uploaded source artifact bytes.
It is distinct from the verified source-tree digest. Ordinary candidate validation
still checks the source tree and allowed mutation surface during admission.
Freezing a plan retains the declared artifacts; it does not validate or run them.

An operator with `learning.manage` creates the plan. These source references and
pending requests come from the existing source-upload and proposal APIs:

```python
from research_swarm.control.learning_models import FreezeLearningPlan, PlanEpisode
from research_swarm.domain.experiments import Direction
from research_swarm.domain.learning import CategoricalPolicy, PolicyAction, RewardSpec
from research_swarm.domain.types import new_id

sources = {"fast": fast_source, "slow": slow_source}
intent = FreezeLearningPlan(
    plan_id=new_id(),
    policy=CategoricalPolicy(
        actions=tuple(
            PolicyAction(id=name, source_digest=source.digest) for name, source in sources.items()
        ),
        logits=(0.0, 0.0),
    ),
    reward_spec=RewardSpec(
        metric_name="val_bpb",
        direction=Direction.MINIMIZE,
        scale=5.0,
    ),
    action_artifacts=sources,
    episodes=tuple(
        PlanEpisode(request_id=request_id, split="development")
        for request_id in pending_request_ids
    ),
)
plan = operator.command("learning.freeze_plan", intent, idempotency_key="freeze-learning-batch")[
    "result"
]
```

The command admits 1–256 distinct pending requests and 2–32 actions. All episode
bindings, artifact retentions and events commit together. A request cannot enter a
second plan or change its split. A changed policy, reward or episode list requires
a new plan over fresh requests. The original plan remains part of the evidence.

The assigned proposer needs only its existing `proposal.respond` scope to obtain
one controller-computed decision:

```python
from research_swarm.control.learning_models import CommitPolicyDecision
from research_swarm.control.search_models import RespondToProposal
from research_swarm.domain.evidence import ArtifactRef

committed = agent.command(
    "learning.commit_decision",
    CommitPolicyDecision(request_id=request_id),
    idempotency_key="sample-learning-episode",
)["result"]["data"]
agent.command(
    "search.respond",
    RespondToProposal(
        request_id=request_id,
        outcome="candidate",
        source_artifact=ArtifactRef.model_validate(committed["source_artifact"]),
        hypothesis="Evaluate the frozen policy's selected source",
    ),
    idempotency_key="respond-learning-episode",
)
```

The agent does not submit probabilities, a random draw or an action choice.
The controller derives and records them from the frozen policy and context.
Retries, including retries under a different command key, return the original
decision. Another proposer cannot sample it, and stale or corrected comparator
evidence blocks a new decision. A historical decision remains readable/replayable
after later changes; that replay grants no fresh execution or scientific authority.

Candidate responses for learning-bound proposals require the committed source.
Failures remain recordable, including failure before a decision or after
cancellation. Such episodes retain their bindings and cannot be sampled
retroactively or dropped into a smaller successful batch. Ordinary proposals
continue to use their existing response behavior.

Both commands use the standard SDK, HTTP and CLI command catalog. MCP exposes
`research_learning_freeze_plan` and `research_learning_commit_decision`.
Operators can inspect `learning_plan`, `learning_episode` and
`learning_decision` through regular evidence queries. Plan lists return bounded
summaries; a detail query returns the complete frozen contract.

This is decision provenance, not dataset qualification. It neither reserves trial
capacity nor admits candidates or promotes policies. Normal response admission,
job accounting and independent evaluation still apply. The numerical trainer in
[the CPU policy example](../examples/cpu_policy_learning/README.md) remains separate.
A controller-qualified trajectory dataset must still verify actual admission,
comparison eligibility, corrections and settled costs for every planned episode.
The held-out benchmark and explicit policy-adoption commands below complete this
path. A measured improvement still requires an actual passing native panel.

## Qualify a development dataset from recorded experiments

After candidate admission and independent evaluation, call the controller to
derive the dataset. The caller supplies IDs, not scores, rewards or a selection
of successful trials:

```python
from research_swarm.control.learning_models import QualifyTrajectoryDataset

qualified = operator.command(
    "learning.qualify_dataset",
    QualifyTrajectoryDataset(plan_id=plan["id"], dataset_id=new_id()),
    idempotency_key="qualify-development-batch",
)["result"]
dataset = qualified["data"]["dataset"]
```

This command requires `learning.manage`; MCP exposes
`research_learning_qualify_dataset`. Query `learning_dataset` for the immutable
receipt, complete typed dataset and frozen plan identity.

The controller follows each declared development episode through its owned
decision, original response, admission transition, candidate validation and exact
trial. Duplicate proposals cannot borrow a previous trial. It derives the one
eligible candidate evaluation and compares it with the plan's original eligible
same-seed comparator. Later incumbent changes do not change that comparator.

Cost includes the issued proposal charge, optional trial-admission charge, and
every attempt belonging to the admitted trial. It checks both attempt-linkage
directions and includes earlier accepted failures. Closed-unaccepted attempts use
their closure and submission evidence. Accepted not-started attempts use their
settled controller accounting; reservation bounds never stand in for actual cost.
The pre-existing comparator and subsequent confirmation campaigns are outside this
episode cost scope. Agent-reported model usage is not treated as verified billing.

Qualification rechecks effective corrections for both evaluations and all consumed
run results, including earlier attempts. It sums verified settlement values using
exact Decimal arithmetic and records their request/response identities alongside
the research record revisions and digests. Reward is derived from those raw
measurements and the frozen reward rule.

All declared development episodes must qualify in order. Missing decisions,
failed or duplicate proposals, pending evaluation, UNKNOWN execution, invalid
comparisons and incomplete settlement block the batch. The refusal identifies the
first blocking episode; original plans, responses and operational evidence remain
available. No partial dataset is published. Held-out episodes remain excluded
according to the original plan; callers cannot relabel them or nominate a smaller
development subset.

The initial profile admits at most 16 jobs and 16 total attempts per episode,
128 evidence references per trajectory, and 8 MiB of canonical dataset JSON.
Overflow refuses explicitly. Qualification reads and commits in one transaction,
including its evidence watermark and event.

A qualification receipt describes that transaction cut. Replaying its original
intent returns the historical receipt, even after a later correction. Server
code must call `require_current_dataset` again at training admission; it rechecks
the full admission/evaluation/cost chain and rejects changed evidence. A fresh
`dataset_id` requests a new qualification cut.

A qualification receipt does not launch training or adopt a policy. The approved
training admission below binds it to ordinary jobs. Publication of a linked
`TrainingJob` follows the output publication flow below. Held-out policy evaluation
remains required before claiming that the learned policy improves research.

## Prepare canonical policy-training inputs

A qualified dataset can be materialized directly from controller records:

```python
from research_swarm.control.learning_training_models import PreparePolicyInputs

binding = operator.command(
    "learning.prepare_training_inputs",
    PreparePolicyInputs(binding_id=new_id(), dataset_id=qualified["id"]),
    idempotency_key="prepare-training-inputs",
)["result"]["data"]
inputs = {
    "behavior": ArtifactRef.model_validate(binding["behavior"]),
    "trajectories": ArtifactRef.model_validate(binding["trajectories"]),
}
```

The command requires exactly `learning.manage`, `artifact.upload` and `read`.
MCP exposes `research_learning_prepare_training_inputs`; regular evidence queries
expose `learning_inputs`. These artifact references can be supplied as the normal
training campaign inputs.

The caller cannot provide replacement policy or trajectory bytes. The controller
revalidates the qualified dataset, serializes it and its frozen behavior policy,
then publishes those canonical bytes through the ordinary artifact lifecycle.
Byte I/O runs outside SQL. The final transaction rechecks the complete evidence
and exact content, retains both artifacts and inserts the immutable input binding.
A correction during publication prevents binding creation. Already published
unreferenced blobs remain visible to normal artifact lifecycle management.

The dataset retains its 8 MiB bound. Other controller-generated metadata keeps the
default 1 MiB inline limit; this preparation path explicitly selects the larger
bounded capacity.

Retries return committed bindings without repeating byte publication, including
recovery from an error after the database commit. A later correction does not
rewrite that historical binding. `require_prepared_inputs` verifies current
qualification, exact canonical hashes, sizes and artifact availability before a
future admission can rely on it.

Preparing artifacts does not launch a trial or authorize policy adoption.

## Freeze held-out seeds before fitting

The first benchmark profile is `categorical-seed-holdout-v1`: new training seeds
within one frozen research family and starting source. It does not claim transfer
to another dataset, action set or hardware class. Full-source categorical actions
are not portable patches; their protected project files remain fixed.

After approving a training profile, freeze the panel before admitting any training
from its development learning plan:

```python
from research_swarm.control.benchmark_models import FreezePolicyBenchmark
from research_swarm.domain.budgets import BudgetLimit
from research_swarm.evaluation.statistics import FixedPairedPolicy

training_admission_id = new_id()
benchmark = operator.command(
    "learning.freeze_benchmark",
    FreezePolicyBenchmark(
        benchmark_id=new_id(),
        training_admission_id=training_admission_id,
        inputs_id=binding["request"]["binding_id"],
        profile_id=profile["id"],
        gate=FixedPairedPolicy(seeds=(101, 102, 103, 104, 105, 106, 107, 108)),
        per_arm_limits=(
            BudgetLimit(dimension="attempts", limit=4, unit="count"),
            BudgetLimit(dimension="execution_ms", limit=300000, unit="ms"),
            BudgetLimit(dimension="proposal_requests", limit=1, unit="count"),
            BudgetLimit(dimension="trial_admissions", limit=2, unit="count"),
        ),
    ),
    idempotency_key="freeze-policy-holdout",
)["result"]
```

This command requires `learning.manage` and `read`; MCP exposes
`research_learning_freeze_benchmark`. It derives the original research contract,
starting source, comparison fingerprint, behavior policy and reward rule from the
qualified development evidence. All development episodes must share that family.
The two future arms have identical explicit caps: one proposal and two trials
(starting comparator and selected candidate). Attempt capacity must cover both
declared stage graphs. The objective is the original episode reward; incomplete
or invalid cases block assessment. Plan creation does not allocate campaign funds.

Reservations bind the learning-plan digest, exact prepared inputs, approved
training profile, future admission ID and ordered seed panel. Requalifying a
dataset changes its dataset identity, but cannot hide prior fitting or evade a
reservation on the original learning plan. Even an admitted training trial that
has not executed prevents retrospective benchmark creation for that plan. This
includes ordinary trials whose campaign contains a canonical prepared `behavior`
and `trajectories` pair, matched by named digest and size across every binding for
the plan. Adding another campaign input does not hide that pair.

Seed identity uses the **campaign** protocol digest, which includes protected
evaluation/data/timing and campaign input names, digests and sizes. It is not a
project registry ID. Any earlier trial admitted under that protocol and seed
disqualifies the case. Freeze and ordinary trial admission serialize atomically;
they cannot both claim a fresh case. The guard sits in the shared trial admission
path, covering direct, proposal-driven and confirmation submissions.

When admitting reserved training, use the saved `training_admission_id` and include
`benchmark_id=benchmark["id"]` in `AdmitPolicyTraining`. Omitting that field, changing
the input binding or choosing another admission ID does not bypass the reservation.
Numerical training without a reserved benchmark remains available; omitted fields
preserve the existing serialized command contract.

The shared trial path also recognizes those canonical prepared pairs. Once their
plan is reserved, it accepts them only through the qualified admission's internal
authority for the frozen future ID. Public `SubmitTrial` has no field that grants
that authority. Re-exported raw or independently transformed data outside canonical
prepared bindings is outside this controller-visible fitting claim.

Query `policy_benchmark`, `benchmark_plan`, `benchmark_training` and `benchmark_seed`
for immutable protocol and reservation records. The protocol freezes the current
SHA-256 sampling method; matching training seeds do not imply matching action
draws, because policy and proposal identities enter those draws. The fixed sign
gate counts ties as non-wins. Rejection or inconclusive evidence is a valid result;
do not replace the seed panel after seeing outcomes.

Reserved seeds remain unavailable to ordinary trial submission. The benchmark
commands below authorize planned comparator and candidate execution, complete
panel assessment and explicit policy adoption. Reservation proves
controller-observed ordering; it cannot certify that no external experiment
used the same data or seeds.

## Approve and admit ordinary policy training

The initial training profile supports one categorical REINFORCE update. An
operator with `learning.profile.manage` and `read` explicitly approves an exact
registered project revision:

```python
from research_swarm.control.learning_training_models import (
    RegisterPolicyTrainingProfile,
    AdmitPolicyTraining,
)

profile = operator.command(
    "learning.register_training_profile",
    RegisterPolicyTrainingProfile(
        profile_id=new_id(),
        project_revision_id=registered["project_revision"]["id"],
    ),
    idempotency_key="approve-policy-trainer",
)["result"]
```

This approval is a trust decision about code. Structural checks cannot prove
that an arbitrary evaluator implements the algorithm correctly. Review the
trainer, checker, support code and environment before granting approval. The
profile freezes the full resolved project revision, source artifact, configuration
and approving actor. Source bytes stay pinned through the approval transaction.

The supported graph has exactly two stages: a trainer and its dependent checker.
Both invoke `python <source-entry>`. Only the literal trainer entry may be in the
mutation allowlist; the checker entry must be explicitly protected. Support files,
the manifest, environment lock and training configuration cannot change in a
candidate. This excludes shell wrappers, module entry points and arbitrary
multi-stage training graphs from this initial profile.

The protected data manifest declares `one-step-reinforce-v1`, one epoch, a finite
learning rate, one expected panel and an update-error tolerance no larger than
`1e-12`. The checker consumes `behavior`, `trajectories`, `policy` and `update`;
its primary metric is `update_error`, minimized with maximum absolute residual
aggregation. The ordinary campaign must supply exactly the two prepared inputs.
`policy` and `update` come exclusively from the trainer's outputs.

Create, seal and start a regular campaign for that exact project revision, using
`primary_metric="update_error"` and the prepared input references. Propose and
validate a candidate through the ordinary API. Then an actor with `learning.manage`,
`trial.submit` and `read` admits it:

```python
admission = operator.command(
    "learning.admit_training",
    AdmitPolicyTraining(
        admission_id=new_id(),
        profile_id=profile["id"],
        inputs_id=binding["request"]["binding_id"],
        campaign_id=campaign["id"],
        candidate_id=candidate["id"],
        validation_id=validation["id"],
        seed=17,
    ),
    idempotency_key="admit-policy-training",
    expected_revision=campaign["revision"],
)["result"]
```

Admission revalidates the full development evidence and canonical input binding,
approved profile, campaign revision, candidate validation and budget allowance.
Trial, jobs, accounting, admission receipt and events commit together. Ordinary
workers execute those jobs; normal evaluator authorization and receipts apply.
There is no learning-specific scheduler or subprocess service.

The shared command catalog exposes both operations through SDK, HTTP, CLI and
MCP. Query `learning_profile` and `learning_admission` for their immutable records.
Concurrent requests with the same admission ID and intent create one trial.
Exact replay returns its historical receipt; it does not revalidate later evidence
or authorize another run. A new admission ID requires current evidence again.

A later correction preserves the executed trial and its costs. It blocks new
publication or current qualification based on that training input. Admission does
not run held-out evaluation or adopt the learned policy. A valid numerical update
is not proof of better research.

## Publish a checked policy checkpoint

After the trainer and checker finish, an actor with `learning.manage` and `read`
can publish their linked training receipt:

```python
from research_swarm.control.learning_training_models import PublishPolicyTraining

publication = operator.command(
    "learning.publish_training",
    PublishPolicyTraining(admission_id=admission["id"]),
    idempotency_key="publish-policy-checkpoint",
)["result"]["data"]
training = publication["training"]
learned_policy = publication["policy"]
```

The request contains only the admission ID. The controller derives the trial,
trainer/checker jobs, current attempts, evaluation authorization, evaluator receipt
and named output references. It verifies accepted successful execution, exact
settlement evidence for those attempts, the original input bindings and current
development evidence. It also requires the checker's reported `update_error` to
lie between zero and the approved tolerance. A valid evaluator receipt alone does
not satisfy this additional threshold.

The controller reads `policy` and `update` through the normal artifact service,
with a 64 KiB limit per file. Both read pins remain live through commit. Parsed
outputs must agree on the policy, parent policy, ordered actions, dataset, reward
rule, algorithm, learning rate, epoch and complete batch size. Numerical
correctness remains the approved independent checker's responsibility; parsing a
report is not another execution of the trainer.

The final transaction rechecks all evidence and exact output references, retains
both artifacts, and inserts one immutable `learning_publication` record and event.
The embedded `TrainingJob.job_id` and `attempt_id` identify the trainer;
`evaluator_receipt_id` identifies the checker's receipt. `update_report` is the
trainer's policy-update artifact, separate from the evaluator's workload report.
The publication includes the parsed learned policy, update digest, evidence-binding
digest and checker identities. It creates no new job, charge or incumbent change.

MCP exposes `research_learning_publish_training`; regular evidence queries expose
`learning_publication`. There is one publication per admission. Concurrent calls
and retries return the same receipt, including recovery after a lost committed
reply. Historical replay remains available after later evidence corrections or
byte-service failure. Server consumers must call `require_current_training`
before relying on that receipt as current evidence; it revalidates research data,
execution, settlements, checker eligibility and publication bindings.

This receipt qualifies a checked numerical update. Held-out evaluation and
explicit adoption use the separate commands below. Its attempt proofs do not claim
to aggregate every historical training retry or total campaign cost; those remain
available through the ordinary ledger.

## Start the fixed benchmark arms

Once the reserved training admission has a current publication, start its frozen
benchmark inventory:

```python
from research_swarm.control.benchmark_execution_models import StartPolicyBenchmark

execution = operator.command(
    "learning.start_benchmark",
    StartPolicyBenchmark(
        benchmark_id=benchmark["id"],
        proposer_id=assigned_agent_id,
    ),
    idempotency_key="start-policy-benchmark",
)["result"]["data"]
```

The command requires `learning.manage`, `campaign.manage`, `candidate.propose`,
`candidate.validate`, `trial.submit` and `read`. MCP exposes
`research_learning_start_benchmark`. This operation admits executable work.

The controller revalidates the published checkpoint, original inputs and profile,
frozen benchmark and every seed reservation. It verifies and pins the initial
source outside SQL, then repeats evidence checks inside one transaction. That
transaction creates exactly two independent campaigns per seed: baseline policy
and learned policy. Each has the same frozen research protocol, input artifacts,
starting source, per-arm caps and two-trial stopping limit. They are independent
budget accounts; this command does not imply parent/child funding or a workspace
aggregate spending cap.

Setup seals and starts each ordinary campaign, proposes and validates its initial
candidate, and admits only the initial comparator trial. Ordinary workers can claim
those jobs immediately. Normal independent evaluation and observed baseline
promotion establish each arm's incumbent. No evaluated incumbent is fabricated
during setup, and no proposal is issued before that evidence exists.

The immutable `benchmark_execution` binds the checkpoint and complete arm-ID
inventory. Each `benchmark_arm` fixes its side, seed, policy digest, proposer,
campaign contract, initial candidate/validation, both trial IDs, proposal ID and
held-out learning-plan ID. `benchmark_trial_slot` records comparator consumption
and the reserved candidate slot. All campaigns, budgets, candidates, trials,
slots, events and the execution binding commit together or roll back together.

Private comparator authority is bound to the exact arm, execution digest, source,
seed and unconsumed slot. It is not a field on public `SubmitTrial`. Ordinary trial
submission inside an arm is refused even when it requests a different seed.
Ordinary proposal requests are also refused so they cannot spend the arm's one
reserved proposal allowance. These restrictions do not block ordinary worker,
evaluator, pause or cancellation APIs.

Retries and concurrent setup calls return the same immutable inventory. A changed
proposer is a conflicting setup request; restarting cannot regenerate sampling
identities or select another learned checkpoint. Historical replay remains
available after byte-service failure. Later evidence corrections preserve work
and costs while preventing current qualification at later decision gates.

This operation starts and qualifies the comparator path. Use the planned-proposal
command below after the arm's starting comparator is promoted.

## Issue a planned held-out proposal

Each arm can progress as soon as its own starting comparator is ready. The
controller issues its one reserved proposal and freezes the assigned policy,
action sources, reward rule and held-out split in the same transaction:

```python
from research_swarm.control.benchmark_execution_models import RequestBenchmarkProposal

prepared = operator.command(
    "learning.request_benchmark_proposal",
    RequestBenchmarkProposal(arm_id=arm_id),
    idempotency_key="prepare-held-out-arm",
)["result"]
request_id = prepared["data"]["proposal_request_id"]
```

The command requires `learning.manage`, `search.manage` and `read`. It derives
the proposer, seed, request ID, plan ID and policy from the immutable arm. The
baseline arm uses the original behavior policy; the challenger uses the checked
learned policy. Both use the original action sources and reward contract.

Before issuing work, the controller revalidates training provenance and the
arm's exact promoted comparator trial, seed and comparison fingerprint. The
ordinary request charge, search-state transition, source retentions, learning
plan, episode binding and `benchmark_proposal` receipt commit together. An agent
cannot observe a pending request without its frozen plan.

The assigned proposer uses `learning.commit_decision` and `search.respond` as
usual. Candidate responses require the committed action source. The held-out
episode cannot become development data. The API accepts no replacement policy,
seed or proposer at this step.

Query `benchmark_proposal` for the immutable preparation receipt. MCP exposes
`research_learning_request_benchmark_proposal`. Concurrent requests and retries
return the same receipt without another charge. After cancellation, the receipt
remains historical evidence; replay does not create a replacement episode.

## Execute the reserved policy action

After the assigned agent commits a decision and responds, use the ordinary
`search.admit_response` command with the current search revision and controller
fence. The controller revalidates the training publication, prepared proposal,
held-out plan, exact starting comparator and committed response source.

Successful admission creates the arm's preallocated `candidate_trial_id`, using
the fixed seed and a new candidate whose parent is the starting comparator.
The candidate, validation, trial graph, admission charge, consumed slot and
search transition commit together. A correction during source verification
leaves the slot reserved and creates no trial. Ordinary workers execute and
evaluate these jobs through the same API as other research trials.

Fixed benchmark actions receive a fresh trial even when their source already
exists in the arm. This includes a precommitted no-op action: suppressing it as
a duplicate would remove part of the policy distribution from the comparison.
Ordinary search retains its duplicate filtering.

Failed agent responses remain `agent_failed` transitions. They do not consume
the candidate-trial allowance, produce a fabricated reward or permit another
proposal. Invalid candidates and cancelled work also remain incomplete cases.
Direct trial submission and confirmation cannot consume reserved benchmark
slots. Replaying an admitted command returns its original result; a fresh
admission cannot spend an already consumed slot.

Completed cases can be qualified against their fixed policy, reward rule,
same-seed comparator and exact settled costs. These trajectories retain the
held-out split and remain excluded from training datasets.

## Assess the complete held-out panel

After every declared arm has completed its comparator and candidate trials:

```python
from research_swarm.control.benchmark_assessment_models import AssessPolicyBenchmark

assessment = operator.command(
    "learning.assess_benchmark",
    AssessPolicyBenchmark(benchmark_id=benchmark_id),
    idempotency_key="assess-fixed-policy-panel",
)["result"]
```

The command requires `learning.manage` and `read`; MCP exposes
`research_learning_assess_benchmark`. There is no caller-supplied case list,
reward, confidence threshold or direction. The controller reconstructs every
case from the frozen inventory and qualifies the original plan, decision,
response, admission, trial and evaluator evidence.

Each case preserves the raw compared metric values and the declared episode
reward. Its reward cost covers the candidate episode. Separate comparator cost
proofs include all earlier comparator attempts and its trial admission. The
combined cost must match every arm budget account, have no outstanding or
uncertain reservations, and stay within the original cap. A case cannot hide
an earlier failure or omit its starting-comparator cost from total reporting.

One `benchmark_case` record per arm and one `benchmark_assessment` commit
together. Missing, failed, cancelled or corrected cases block the entire
assessment. No partial gate or fabricated failure score is published. An
inconclusive complete panel is a valid result and does not authorize more seeds.

The frozen paired sign gate compares challenger and baseline episode rewards,
with higher reward better. Ties count as non-wins. Its exact p-value and effect
statistics apply to this predeclared seed panel within one research family.
The sampler includes request and campaign identities, so matching seeds do not
imply identical policy sampling draws or common random numbers. This assessment
does not establish transfer to a different dataset, hardware class or policy.

Historical retries return the original assessment. A later decision gate must
requalify all current evidence and compare the exact case digests; replaying an
old successful assessment cannot bypass a correction.

## Adopt and pin a research policy

Initialize a policy binding from a benchmark's frozen baseline. This is an
explicit baseline choice, without a claim of improvement:

```python
from research_swarm.control.policy_models import AdoptPolicy, CreatePolicyBinding

origin = operator.command(
    "learning.create_policy_binding",
    CreatePolicyBinding(binding_id=binding_id, benchmark_id=benchmark_id),
    idempotency_key="initialize-research-policy",
)["result"]
binding = operator.get("policy_binding", binding_id)
```

Creation requires `learning.manage` and `read`. It retains the original policy
artifact and action sources, creates the active `policy_binding`, and records
an immutable `policy_binding_origin`. The command returns that origin; replay
continues to return it after later adoption. Query the active binding to see its
current policy and record revision.

A separate actor with `learning.adopt` and `read` can adopt a passing challenger:

```python
adoption = operator.command(
    "learning.adopt_policy",
    AdoptPolicy(binding_id=binding_id, benchmark_id=benchmark_id),
    expected_revision=binding["revision"],
    idempotency_key="adopt-qualified-policy",
)["result"]
```

The transaction requalifies the full assessment, training publication and prior
adoption chain. It requires a passing gate against the binding's exact current
baseline, with the same research protocol, comparison fingerprint, reward rule,
ordered action identities and starting-source content. It retains the checked
checkpoint, records `policy_adoption`, and updates the binding with a revision
check. Concurrent challengers cannot both replace one baseline. Model-candidate
incumbents and already frozen learning plans do not change.

New learning plans can pin the active binding:

```python
from research_swarm.control.policy_models import PolicyBinding, PolicyBindingRef

row = operator.get("policy_binding", binding_id)
current = PolicyBinding.model_validate(row["data"])
intent = FreezeLearningPlan(
    plan_id=plan_id,
    policy=current.policy,
    reward_spec=current.family.reward_spec,
    action_artifacts=current.action_artifacts,
    episodes=planned_episodes,
    policy_binding=PolicyBindingRef(
        binding_id=binding_id,
        revision=row["revision"],
        policy_digest=current.policy.digest,
    ),
)
```

The controller verifies the supplied revision, adoption evidence and episode
family before freezing. The plan stores the exact binding snapshot. Future
decisions, admissions and dataset qualification use that snapshot, not the
latest pointer. Advancing the binding preserves old plans; correcting evidence
under a pinned adoption blocks new uses of that adoption. Exact command replay
remains historical evidence and never silently performs another adoption.

The first profile supports up to 16 linked adopted-policy generations in one
ancestry proof. It rejects cycles and deeper ancestry explicitly. Shared
ancestors are verified once within a proof walk, with no cache across operations
or transactions. This bound applies to policy ancestry, not experiment count.

MCP exposes `research_learning_create_policy_binding` and
`research_learning_adopt_policy`. Policy bindings, origins and adoptions are
available through the same evidence query API. A successful adoption mechanism
does not establish a native policy win: report the actual held-out gate outcome,
including rejection or inconclusive results.
