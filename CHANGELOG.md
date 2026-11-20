# Changelog

## Unreleased

- The standalone R2 adapter verifies and opens stored blobs through the same
  generation check. Existing storage imports and staging directories are supported.

- Trainer requests, manual responses and completed policy calls share the same
  run-binding and control revision checks. Existing reactive journals reopen as before.

- Campaigns can set `full_fidelity_work` and `max_search_work` with `max_attempts: 1`
  to cap search work across rungs. Each admitted trial reserves the rounded-up
  cost of a fresh fit, including the initial recipe and promotions. Queued and
  completed trials keep their reservations across resume.

- Interrupting a native run closes the owned workload and propagates the
  interruption. A cancelled proposal or an unexpected operator error reaches
  the caller. Completed providers release their slots before the next request.
  A malformed model response still counts as a rejected response.

- Campaign and artifact storage now share the same durable directory sync. Private
  file handling moves from `state.private_files` to `swarm.private_files`.

- Update the locked versions of FastAPI, filelock, boto3 and botocore, along
  with Ruff, Hypothesis, pytest-asyncio and the psutil type stubs.

- Run local workloads, transport commands and native project stages in owned
  process groups. A command can finish even when a descendant holds its output
  pipes open. Native runs wait for their guard to exit within the cleanup deadline
  before reporting closure. Advisory jobs retain their independent deadline and terminal report.

- Report the known cost subtotal and missing rates when recorded usage is only
  partly priced. Cost per improvement stays unavailable until the total is known.
  Reopening a campaign accepts equal integer and floating-point price rates
  and preserves the original panel study binding.
  Direct campaign registration rejects negative prices before writing to the queue.

- Campaigns, project manifests and registered engines share portable record definitions in
  `swarm.records`. Existing Python workload imports and serialized records remain compatible.

- Add a generated raw-window example that fits per-user channel statistics on
  unlabeled reference windows and applies the saved state at inference.

- Project capture and manifest resolution now share one implementation. A read in
  progress finishes on its original bundle if a newer capture replaces the path.

- Compare the three benchmark policies at equal executed search work. Record
  program cases, optimizer steps, held-out evaluations and local model usage.

- Use a common WAL connection policy for the queue, panel studies and journals.
  Concurrent writers share a busy timeout, and schema upgrades commit atomically.
  Campaign reports and equal-work benchmark finalist checks open existing databases read-only.

- Reject a benchmark campaign paired with an incompatible open panel study
  before reusing a score or spending a look.

- Resume partial panel calibration using the recorded proxy and target scores
  for each recipe and seed. An unscored reservation stays spent and stops the
  fit. Recovered fits report unavailable throughput when earlier timing is unknown.
  Preserve tiny seed deviations and recover underflowed noise from stored target
  scores before new comparisons. Recorded decisions keep their original outcomes.
  New comparisons check retained target rows against the declared recipes and seeds
  before using a cached positive noise estimate.

- Validate the selected backend before creating or migrating the campaign queue.
  A rejected backend configuration leaves the queue untouched.

- Reject trial seeds outside the signed interoperable JSON integer range on
  campaign admission, proposal submission and resume, before recording new work.

- Emit one campaign alert when two consecutive finished attempts fail or are
  lost. A successful attempt resets the streak. Concurrent and restarted
  watchers share the same alert and hourly digest journal.

- End a local trial's process group when its worker is killed, so an orphaned
  workload does not keep running alongside its retry.

- Clarify local worker cleanup, GPU memory accounting, score ties and
  panel study reuse in the campaign design.

- Document how to stop and resume a local campaign with its original queue
  and panel study, and where to find trial, proposal and advisory timeout diagnostics.

- Correct the quickstart help command and graph smoke prerequisites. Fix the
  reactive CPU example's test command and live training journal path.

- Enforce each advisory job's process-group deadline after its caller exits and
  retain a terminal status beside the abandoned heartbeat.

- Report advisory responses received at or after the job deadline as timed out
  and discard their advice.

- Remove the retired worker journal's `launch_deadline_ns` argument from
  `run_native`. Native commands use their timeout and optional absolute deadline
  during both preparation and execution.

- Preserve the original advisory role and deadline in the timeout report if the
  heartbeat file disappears. If launch metadata is missing or invalid at expiry,
  including for older jobs with only a heartbeat, the timeout report marks the
  role and deadline unknown and records the error.

- Share committed-manifest resolution between explicit and manifest-selected
  Git revisions in project capture and registration.

- Use the same exclusive output and disk-sync handling when capturing source
  bundles from Git and from a working directory.

- Report the fraction of returned proposer candidates admitted to a new campaign,
  including duplicate and invalid entries in the denominator. Earlier campaigns
  retain an unavailable rate.

- Calculate campaign cost per 0.01 improvement from selections at the current
  trial fidelity, including the measured cost of screening trials.

- Label metered budget amounts in campaign currency in budget responses,
  campaign snapshots and status-page budget details.

- Plot and rank campaign trial scores by fidelity. Screening scores and
  recorded selections stay on the rung where the trial ran.

- Leave a gap in the recorded-selection curve when an incumbent has no measured
  score. The curve resumes at the next measured selection.

- Show eight campaign KPIs in live snapshots and offline reports, including the
  evidence behind each value and the reason a measurement is unavailable.

- Report calibration slope, target seed noise and confirmation pass counts from
  the panel outcomes recorded for each campaign, counting replayed comparisons once.

- Show the confirmation pass rate as unavailable when a recorded confirmation
  gain is a JSON boolean. A numeric zero still counts as a measured gain.

- Pass the dispatched trial fidelity to native workload stages so training can
  use the budget assigned by the campaign scheduler.

- Report time-weighted GPU utilization for opt-in dedicated local CUDA devices,
  including coverage gaps, idle intervals and allocation recovery evidence.

- Preserve managed-job preemption evidence across worker restarts so the retry
  uses on-demand placement even if the first worker exited before polling.

- Stabilize the offline SkyPilot reconnect test by launching its synthetic CLI
  through the current Python interpreter.

- Document the remaining MLP search gap and freeze a search-only comparison
  of coordinate coverage around the incumbent.

- Publish the prospective CPU MLP results. Diversifying local coordinates tied
  the corrected control on all three seeds under the 12-trial budget.

- Schedule one-coordinate finite search moves by the campaign trial budget,
  counting screened and promoted trials once each.

- Count categorical levels explored at lower fidelities when the finite mutation
  operator proposes its next recipe in a multi-fidelity campaign.

- Guide finite mutation with coordinate effects learned separately at each
  search fidelity. Screened recipes can inform later proposals before promotion.
  The search portfolio's default bandit exploration coefficient is now 1.0.

- Publish the three-workload CPU comparison of the operator portfolio and ASHA
  against random and greedy search, with paired held-out scores and trial records.

- Keep finite parameter campaigns running when both default bandit operators propose
  previously tried recipes. The next available grid recipe fills the open slot.

- Add a five-minute local CPU quickstart with resume and report export. Document
  the 12 top-level CLI commands, 23 executable paths and six campaign tables.
  Update report and MLX examples to use the current CLI.

- Document the repeated-failure alert and when missing inputs or budget
  require an operator to stop a real-workload acceptance run.

- Correct the campaign browser startup and local artifact commands. Link the
  retired controller's R2 setup and qualification at its matching revision.

- Show operators how to inspect calibration reservations before resuming an
  interrupted campaign and how to identify a saved calibration summary.

- Document how to reconcile proposal, confirmation, calibration and GPU time
  in a campaign report with its recorded queue evidence.

- Correct the operator guide's panel query for the supplied YAML campaign and
  show where to read its held-out score and promotion decision.

- Document the evidence behind the eight campaign KPIs and scope the operator
  guide's queue, event and attempt queries to the requested campaign.

- Apply revision-pinned source diffs to native training stages. The original
  evaluator scores the artifacts produced by the edited training code.

- Remove the retired controller command-ID error envelope and unused error codes.
  Bounded JSON input for offline reports and reactive proposals now reads only
  from their required file paths. The public CLI syntax is unchanged.

- Set `proposer: research_swarm.swarm.operators.bandit:BanditProposer` to allocate
  trials between parameter mutation and crossover by observed gain per cost.
  Custom proposer factories can supply other operators. The bandit restores pending
  recipes on resume and returns completed siblings when a model proposal times out.

- Model editors receive only the campaign's declared search metric in proposal
  history. Worker results retain their auxiliary measurements for reporting.

- Resuming a held-out comparison reuses its completed selection measurements.
  An interrupted selection look stays consumed. A different comparison gets
  its own paired measurements.

- SQLite campaigns using a panel study remember which study supplied their
  calibration and confirmation. Restarting with a different study or reusing
  a campaign ID with a changed search plan is rejected before evaluation.

- `research swarm notify` journals hourly digests, selected-score targets and low
  trial throughput in the operator's timezone. Concurrent watchers share the journal.

- Make the local SSH keeper test record its shutdown command with a native executable
  while checking activity locking, the idle deadline, and rejection of new work.

- Run captured native project stages as a swarm trial with
  `python -m research_swarm.swarm.native`. Use `--registration` for a registered
  interpreter and `--outputs` to keep per-attempt reports and reactive traces.
  Evaluation stage metrics feed the queue directly. Legacy evaluation receipts,
  worker journals and controller promotion commands are retired. The workload,
  engine and reactive guides now show how to launch these stages from campaign YAML.

- Add an optional bounded reporter to hourly digests and a CPU campaign
  [operations runbook](docs/campaign-ops.md) for notification, diagnosis and recovery.

- Verify that a watcher with a broken stdout pipe preserves its digest and score
  alert in the notification journal, and that restarting it does not repeat the alert.

- Remove the old controller dispatcher, historical-import API and controller
  benchmark harness. Archived benchmark results and standalone project receipt
  readers remain available.

- `research swarm advise` runs a local proposer, diagnoser or reporter against
  campaign observations. Each request has a process deadline and live heartbeat.
  The deterministic fake runs offline.

- Keep hourly notifications distinct across half-hour daylight-saving rollbacks
  and deduplicate equivalent numeric score targets when a watcher restarts.

- Enforce each advisory provider's deadline inside its process so a stalled
  model request ends even if its supervising CLI is killed.

- **Breaking:** `research init` opens a private six-table campaign workspace.
  Use `research swarm run` for execution. Projects, artifacts, reports, budgets,
  HTTP campaign reads and local MCP now use `swarm.sqlite`. The SDK provides
  synchronous and asynchronous campaign reads. Open older controller databases
  with their original release and choose a fresh directory for new campaigns.
  Archived report JSON still verifies its original digest. The controller
  signing keys, legacy funding ledger and schema migration code are retired.

- Local campaigns honor `options.backend` directly. Set `options.backend.device`
  to `mps` or `cuda` for GPU workers.

- Serve a campaign worker with a separate private bearer and a fixed campaign
  and worker identity. Claimed attempts can heartbeat, publish observations,
  and upload immutable artifacts through the same SQLite queue.

- `research swarm run` reports the winner's held-out `confirmed_score` when
  the panel policy can read it. `selected.score` keeps the search ranking.

- Resuming an exhausted SQLite campaign reuses the recorded result until new
  observations arrive. Pass `research swarm run --retry-exhausted` to explicitly
  request another proposal against the same observations.

- Completed campaigns keep their stop reason when reopened after the wall-clock
  deadline. The deadline still prevents dispatching queued work or requesting a retry.

- Serve authenticated campaign reads from SQLite with `serve --db`, inspect
  a selected database with `mcp --db`, or query it through `CampaignReader`.

- **Breaking:** Retire parent-funded child campaigns and their allocation commands.
  Run each campaign with its own dispatch budget. Finish or inspect existing
  delegated workspaces with their original release and start new runs in fresh state.
  New control databases also omit the allocation posting journal.

- Render local hill-climb reports from campaign SQLite state with `report view`
  or `report plot --db DB --campaign ID`. Read measured cost and active dispatch
  estimates with `budget ID --db DB`.

- Use `--db` with local project capture, registration, and artifact upload or
  download to keep project snapshots and artifact bytes beside a swarm campaign.

- Remove the unused settlement-evidence verifier and its reservation lookup index
  from new control databases.

- Opening a legacy control database as a swarm campaign now reports how to
  start in a fresh database path and preserves the existing state.

- Install the optional `skypilot` extra to provide `uvx` for the isolated SkyPilot CLI.

- **Breaking:** Retire `campaign.capture_obligations`, `campaign.complete_scientific`
  and `campaign.close_resources`. New swarm campaigns persist the selected trial
  and its score as the loop runs. Historical completion reports remain readable.

- Retire the serial greedy and portfolio controller planners, their search and
  population commands, and assigned proposal reads. Run campaigns with
  `research swarm run`. Set `max_workers: 1` and `top_k: 1` for sequential
  selection within a fixed trial budget. Existing report snapshots remain readable.

- Run the GitHub CI workflow on manual dispatch. Pull requests use the local merge gate.

- Require a PR description explaining which contracts need coverage when a
  change adds more than 400 net lines of tests or test support.

- Count tracked text fixtures in the local test-line budget even when a PR changes
  their diff attributes or replaces a binary fixture with text.

- **Breaking:** Remove the policy-learning, held-out benchmark and policy-adoption
  subsystem: the 13 `learning.*` commands, their scopes, record kinds and MCP tools,
  and the `learning_cycle` and `cpu_policy_learning` examples. No real research run
  used it.

- Contain unexpected local failures to one attempt in `ScopedWorkerDriver`. The
  driver keeps heartbeats and sibling work running, reports `EXECUTION_UNKNOWN` and
  `LOCAL_FAILURE` findings, fences admission, and retries the exact attempt on the
  same worker. Authority loss still stops the driver.

- Check that drained workers retain their identity and advance their heartbeat
  when resumed, including on runners where several heartbeat intervals elapse.
  The cancellation test observes the next driver's heartbeat directly while
  the worker is drained.

- Consolidate canonical serialization tests around the frozen cross-language corpus
  and remove obsolete signed-time conformance fixtures.

- Remove the unused `evidence_receipt` helper and `DeadlinePolicy` model, and the
  disabled Windows CI jobs. Windows runtime code stays; qualification remains paused.

- Add asynchronous successive halving, seed-aware promotion and diverse parent
  selection for multi-fidelity swarm searches.

- Keep continuous search watchers running across retryable controller query outages,
  with paced fresh-state reads. Preserve explicit stops for uncertain command delivery
  and non-retryable errors; expose the original retryability in one-shot results.

- Pause Windows CI and native qualification; retain Linux/macOS checks and all
  test assertions unchanged.

- Add opt-in continuous search driving through `SearchDriver.run_async` and
  `research search advance --watch`, with paced durable-state reads and explicit
  policy, budget, review and transport stops. Async cancellation now drains the
  current bounded search pass before releasing the client.

- Generate swarm candidates with parameter mutation, optional Optuna sampling,
  crossover, ensemble growth, seed replication and deadline-bounded local Ollama edits.
  Ensembles keep individual member recipes when replicated, promoted or resumed.

- Allocate swarm proposal slots by observed gain per cost, learn mutations from
  categorical trial results and promote replicated recipes at full fidelity.
  A model editor can propose a corrected recipe when the initial workload fails
  after its last retry and the campaign has no scored parent. SQLite attempt IDs
  stay out of model prompts so replaying the same failure preserves its input.

- Define the campaign, candidate, trial, attempt and result records and plugin
  protocols for capacity-driven research campaigns.

- Persist swarm campaigns and their trial queues in SQLite. Workers claim and
  reserve cost atomically. Expired attempts retain their costs and observations
  when the queue makes the trial available for retry. By default a failed or
  lost attempt can retry once. Set `max_attempts: 1` to run each trial once.

- Run a capacity-driven campaign from YAML with `research swarm run`. Completed
  attempts feed the proposer as slots free up. The population ranks successful
  candidates even when a sibling crashes and records each selection's score
  and replicate count. Workers can defer queued trials until token capacity
  replenishes. Set `max_hours` to stop dispatch at a campaign deadline that
  survives restarts.

- Add a reproducible synthetic research atlas with 600 records and 2,028 links,
  including branching trials and cross-area references, for the README graph example.

- Include pytest-xdist in the locked development environment for parallel local
  contract runs. Give Chrome time to start cold in the real browser smoke tests
  and report its launch errors.

- Check the supported engine inventory through the CLI and exercise corrupt
  registration handling without first probing a valid environment.

- Apply newly requested graph layouts while reduced motion or Pause is active,
  without accepting queued animation frames.

- Add reproducible program, MLP and causal language model benchmarks with random
  and greedy baselines, local Ollama program and training-code proposals, and
  small CPU/MPS and full CUDA training profiles.

- Calibrate workloads on separate proxy and target cases before search. Count
  panel exposures across restarts and require a held-out confirmation gain to
  replace the reference candidate in benchmark campaigns.

- Add a preregistered second benchmark protocol with fresh data streams,
  larger model search spaces, and paired scores read from the held-out study.
  Publish all 27 arms. The frozen portfolio beat both baselines on the program
  and MLP tasks and had higher nanoGPT loss than both baselines. Run the
  hosted benchmark workflow only by explicit dispatch.

- Replace the controller design manual with the swarm loop contract, operator
  and campaign guides, and an executable panel-aware CPU campaign example.
  Retire fifteen controller manuals and link to their versioned references.

- Share sidebar width, collapse and section-height resizing across campaign and
  inventory views; retain widths when switching views and support keyboard dividers.

- Add atomic CPU and LLM slot admission and GPU packing based on observed per-fit
  VRAM, with a utilisation target and a batch size for multi-fit workloads.

- When a fit reports a higher peak VRAM, count that peak for comparable fits
  still running on the same device before admitting more work.

- Report the watchdog timeout duration when a swarm worker stops a stalled attempt.

- **Breaking:** Retire `research evaluation advance` and its evaluator-token option.
  Run stage commands with `research swarm run` and publish measurements through `SWARM_METRICS`.

- **Breaking:** Retire `confirmation.create` and `confirmation.assess`. Swarm
  campaigns compare candidates on selection and held-out confirmation panels.

- **Breaking:** Retire the controller promotion and evidence invalidation commands.
  The swarm loop records its selected trial, policy score and replicate count in SQLite.

- Verify a score-only selection update against one migrated campaign history
  and run the worker packing test with the public campaign specification.

- Add a resident swarm worker that heartbeats, retries lost trials and records
  attempt failures. Dispatch reserves estimated cost and reports charges from
  elapsed compute time and measured input and output tokens. Validated usage
  remains billable when a task returns invalid objective metrics. A worker reports
  the retry delay when a rolling LLM token quota temporarily prevents dispatch.

- Dispatch tagged SkyPilot managed jobs across multiple locations. A lost spot
  attempt requeues with on-demand placement inside the campaign retry budget.
  AWS placements name each eligible region for scoped resource cleanup.

- Add a layout selector for connected clusters, category groups and a symmetric
  ring plus a broad spread-out force view, retaining filters, selection and recorded ancestry across layout changes.

- Move graph layout to an offline D3 Barnes–Hut worker, batch edge drawing and
  render only on changes. Default to topology-led layout without circular category
  anchors; category grouping remains optional.

- Stage a campaign dataset revision once per location. Workers copy it in parallel
  to local storage, verify the content digest, and bind it read-only on Linux.

- Read back each new location or worker copy and verify its digest before
  publishing the revision. A damaged copy can be retried at the same location.

- Recheck the bytes of cached dataset revisions before reuse and before local
  dispatch with managed stage inputs. Staged directories now protect their
  entries against accidental creation, replacement and removal.

- Refresh selected graph connection visibility annotations when search or category
  filters change, including when filters are cleared.

- Run trials on dedicated SSH pool hosts with EC2 termination protection, idle
  shutdown to the stopped state, and verified read-only worker-local data.

- Add a standalone, offline force-directed experiment inventory with up to 5,000
  nodes, category layout, search/filter, pan/zoom, pinned-node dragging and explicit
  causal ancestry inspection. Source references stay non-causal; inventory size
  does not expand the bounded report measurement window.

- Run swarm trials in bounded local subprocess slots with CPU, MPS, and CUDA
  device selection. Timeout and cancellation clean up the attempt's process group.

- Finish local trials when the workload command exits. Helpers that inherit
  its output pipes are stopped, preserving the command's metric or exit status.

- Record when local and SSH workers acknowledge a workload launch, including
  attempts later lost. Backend failures carry a machine-readable class when
  known. Existing queues remain readable with their original measurements.

- Add Hill climb / Graph tabs within the resizable campaign workspace. Hill climb
  retains chronological receipts and a separate running best per selected comparable
  campaign. Graph arrows use explicit parent IDs only, fade older generations and
  highlight the inspected ancestry without implying a new scientific selection.

- Make the campaign workspace the only HTML view; remove the Time/Tree switch.
  Serve a read-authenticated, paginated campaign picker alongside the existing API,
  with direct loading for single-campaign stores and exports. Refreshing the view
  reads bounded evidence without creating reports, events or changing research state.

- Add a Swarm GitHub link and optional project GitHub attribution to the campaign
  view, with draggable and keyboard-resizable sidebars plus collapse controls.
  Pass `report view --project-repository https://github.com/OWNER/REPO` to supply
  project attribution without modifying captured scientific evidence.

- List and collect tagged dataset revisions with `research campaign gc`. Keep
  selected revisions and active mounts, and resume an interrupted deletion.
  Workers can copy the same revision concurrently while collection waits.

- Extend campaign cleanup to managed jobs, EC2 instances, and EBS volumes.
  Recheck ownership before removal and report partial failures for retry.

- Keep campaign graph coordinates finite for extreme scores.

- Rework the campaign view into a compact three-column workspace: narrow context
  rail, score-positioned candidate ancestry with muted branches, and a stacked
  leaderboard/activity rail. Preserve unscored candidates on a separate rail and
  support keyboard inspection without inventing scores or agent attribution.

- Qualify the synthetic offline report in real headless Chrome CI with desktop/mobile
  layout, campaign selection, page-error checks and captured browser screenshots.

- Add an offline interactive campaign HTML export using the same bounded score
  projection, with off-white styling, captured lineage, qualified-selection versus
  observed-path labels, candidate scores, budgets and honest historical activity.

- Export native, dependency-free hill-climb SVGs from immutable reports, with
  exact metric/comparison selection, comparable-run overlays, original baseline
  references and bounded-history disclosure. Capture controller evaluation event
  times and campaign creation time without inferring training time or confirmed wins.

- Accept identical package metadata from editable installations during engine
  probing while preserving rejection of conflicting installed versions.

- Expose a bounded Git source-capture timeout through the sync/async SDK,
  project CLI and local MCP tool, shared across manifest reads and bundling.

- Allow hosted delivery to read scientific completion and resource closure records when deciding whether a campaign is terminal.

- Preserve safe SQLite cancellation-read failure codes and close worker-journal connections when setup fails.

- Qualify Windows fixtures with byte-stable sources, private registration roots and pinned-interpreter recovery diagnostics.

- Keep synchronous and asynchronous delivery available after scientific completion until resource obligations are explicitly closed.

- Treat positively closed, never-released work as unstarted for launch and evaluation-exposure accounting, preserving supervisor diagnostics.

- Preserve rejected Cloudflare recovery-turn acknowledgments across retries and lost replies.

- Validate settled reactive policy-call identities, limits and terminal evidence before accepting transcripts.

- Declare the expected evaluation panel in the project-manifest template and explain receipt completeness.

- Consolidate project manifest compatibility checks and reduce fixture setup
  for release artifact verification.

- Ignore directory symlinks outside source selections while rejecting selected symlink paths.

- Report interrupted artifact and worker downloads as retryable transport failures, with partial-file cleanup.

- Return structured capability errors for unknown HTTP commands instead of uncertain-delivery advice.

- Remove the unused controller test launcher and its self-tests. Keep real CLI
  checks for UTF-8 output and clean errors when an archived report is malformed.

- Check upload and read permissions before creating artifact uploads or reserving storage.

- Add an ordinary CPU project that demonstrates pending-decision training,
  bounded intervention, a fixed-control baseline and independently evaluated replay.

- Replay settled reactive control schedules at their recorded application steps,
  with bounded transcripts, separate provenance and non-resumable mutation traces.

- Bind optional reactive declarations to protected project protocols, sealed
  training contexts and reused worker preparations without changing legacy digests
  or loading native journal infrastructure into portable controllers.

- Expose one explicitly selected local reactive session through a shared Python
  client, JSON CLI and dedicated bounded stdio MCP tools for agent responses.

- Add an opt-in Chat Completions HTTP decision policy with explicit model/config
  fingerprints, bounded strict JSON, external credentials and no automatic retries.

- Add a provider-neutral asynchronous decision runner with durable call claims,
  deadlines, late-result evidence and callback or threshold policies. The runner
  stops serving closed training sessions without waiting for resistant callbacks.

- Add a cooperative training hook that applies bounded control changes at update
  boundaries, records readback and stops intervention after uncertain mutation.

- Persist local reactive decisions in an append-only SQLite journal with one
  outstanding request, exclusive training ownership and uncertain-application recovery.

- Add versioned training-time reactive contracts with declared numeric controls,
  diagnostic visibility, policy identity, bounded decisions and separate application receipts.

- **Breaking:** Remove the unqualified Cloudflare hosted adapter (`adapters/cloudflare`,
  its workerd tests and CI jobs), `orchestration.cloudflare`, the `AsyncR2Transport`
  port and hosted activation proofs. It was never deployed. Native R2 artifact storage
  through the optional `s3` extra is unchanged.

- **Breaking:** Remove the optional Temporal adapter, the `temporal` extra and its
  saved-history fixtures and CI job. Local delivery is unchanged and never loaded
  the Temporal SDK.

- Drop Windows-native runtime support. Linux and macOS workers retain process-group
  supervision. Stored Windows process and path evidence remains readable.

- Replace signed time samples, witness servers, and worker clock channels with
  local monotonic launch and execution deadlines. A supervisor that misses its
  deadline before claiming the launch leaves a not-started attempt and frees its
  resource slot. Deployments that used calibrated time require fresh controller
  and worker state directories.

- **Breaking:** Retire durable turns, recovery snapshots, backup elections and
  worker recovery routes. Lost swarm attempts return to the queue under the
  same trial ID. A superseded worker cannot replace the accepted result.
  Use the original release for inactive legacy workspaces.

- Add a local campaign dry run for the local, SSH pool and SkyPilot backends
  and a [real-GPU qualification checklist](docs/gpu-qualification.md).

- Check the selected reference recording before applying a generated user's
  saved raw-window normalization. Older fitted states have unverified provenance.

## 0.1.0 - 2026-11-01

### Packaging and maintenance

- Align the Cloudflare Worker runtime lock with Pydantic 2.14.1 and its
  matching WASM core wheel.
- Add build-once release previews, exact-artifact installation checks and
  tag-gated PyPI Trusted Publishing with attestations and release checksums.
- Pin release tooling, limit source distributions to package inputs, and
  document publisher setup and partial-release recovery.
- Compare release artifacts with raw Git blobs so Windows line-ending settings
  cannot alter source identity checks; bound large batch reads to avoid pipe stalls.
- Add dependency updates, inactive-discussion labels without automatic closure,
  release-note drafts, issue templates and shared editor settings.
- Group tests by component while preserving frozen conformance fixtures.

### Research and agent control

- Add local research campaigns with immutable source bundles, explicit budgets,
  independent evaluation and evidence-linked promotion.
- Expose one command service through HTTP, the Python SDK, JSON CLI and MCP.
- Support external proposal agents, greedy search, parallel portfolios,
  candidate populations and parent lineage.
- Add confirmation policies, evaluation-exposure budgets and explicit
  comparisons. Keep failed attempts and missing measurements visible.
- Support nested campaigns, policy-training stages, held-out assessment and
  explicit policy adoption.

### Execution and recovery

- Run ordinary repository manifests through registered CPU, PyTorch and MLX
  environments. Keep model code and framework dependencies outside the core.
- Add native process supervision, private worker journals, scoped enrollment,
  shared resource reservations and recovery after interrupted delivery.
- Preserve original deadlines and execution identity across retries. Require
  native closure before releasing uncertain resource reservations.
- Add verified artifact storage, bounded transfer, retention and replayable
  source capture.
- Fix isolated Windows architecture probing on Python 3.11 while preserving
  environment isolation and native architecture under emulation.

### Infrastructure adapters

- Avoid shadowing Python's standard library when launching Cloudflare tooling.
- Organize Cloudflare source by platform, research, delivery and recovery roles.
  Preserve generated module names and document the shared adapter ports.
- Add Temporal delivery and replay against the shared research authority.
- Add Cloudflare Workflow delivery, Durable Object authority and R2 storage.
- Support fenced checkpoint recovery, target activation, clock requalification
  and recovery of errored Workflows without deleting their history.
- Preserve idempotent results, original timer thresholds and accounting across
  lost acknowledgments and runtime restarts.

### Examples and documentation

- Add CPU bigram, MLX, synthetic pose, pinned upstream adoption and policy-learning
  examples with explicit evaluation contracts.
- Support historical metadata import without granting imported records current
  execution, budget or promotion authority.
- Replace specification-writing history and private-project background with
  public contracts, a local quickstart and task-oriented guides.

### Qualification

See the [qualification register](docs/implementation.md) for tested revisions,
platforms and provider runtimes. Local workerd execution, native accelerator
qualification and deployed Cloudflare operation are separate claims.
Synthetic examples and integration tests do not establish scientific improvement.
