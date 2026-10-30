# Changelog

## Unreleased

- Admit the source-elected native restore into a signed recovery phase after full checkpoint verification. Bind admission to the exact database, receipt, request, source pin, epoch and watermark through an internal non-wire capability.
- Atomically fence orchestration and drain running campaigns while preserving dedicated actors, pending turns, unresolved attempts and reservations. Keep the epoch unchanged, replay committed admission without its bundle, and leave ordinary mutation and activation unavailable.

- Pin source custody per control epoch and irreversibly elect one inactive restore target through signed native requests and claims. Persist the claim with its source key, preserve exact replay and refuse missing or substituted custody.
- Expose native claim request/issuance commands, keep elected targets inactive and document the source-custody rollback trust boundary. Use Windows write-through private-file replacement with explicit failure propagation.

- Restore verified native checkpoints into a distinct inactive epoch, preserving artifact generations, acknowledgments, budgets and unresolved execution while archiving and fencing original worker sessions.
- Publish admission barriers before destination creation, resume exact private staging/SQLite interruptions, compare preserved ledger contents and prevent recovery events from waking old workflows. Keep activation unavailable pending source and worker reconciliation.

- Export and verify sealed native SQLite/filesystem checkpoints with complete generation inventories, private identity material, source-only custody keys and signed manifest digests.
- Publish the manifest last, preserve exact completed-export replay, reject corrupt/incomplete bundles and restrict credential-bearing exports to full operators. Add exclusive private-directory creation on POSIX and Windows.

- Add a shared workspace seal for backup admission: freeze new commands, credential changes, enrollment and notification acknowledgment while preserving exact prior replies and unresolved execution obligations.
- Refuse sealing during uploads, deletions or retained artifact reads. Serialize deletion cleanup through its terminal commit so completed command replay cannot mutate storage after the cut.

- Preserve the R2 verified-reader binary-file contract across POSIX and Windows temporary-file implementations; fresh Windows type checks now accept the delegating wrapper.

- Add a reproducible local controller/CPU benchmark with independent connection loads, budget races, exact command replay and paired one/two-lane native throughput. Retain failed/censored cells, evaluator receipts and exact comparison identities.
- Preserve original attempts on interrupted benchmark resume and report incomplete timing explicitly. Keep full measurements opt-in and CI focused on correctness.
- Remove an unnecessary R2 type-ignore comment exposed by fresh CI type checking.

- Persist filesystem/R2 selection in runtime identity; initialize through `research init --storage-config` and reopen it through existing SDK, HTTP and agent tools. Preserve legacy filesystem identities and refuse backend changes or missing staging.
- Check optional dependencies before new state, keep partial initialization fail-closed, and reject oversized worker uploads before quota admission.
- Qualify native R2 publication, restart, reader pins, delayed-write fences, deletion accounting and corruption checks against a real local R2 binding; hosted S3 deployment remains separate.

- Add a native R2 byte adapter with pinned destination/staging identities, resumable local parts, immutable upload-generation keys and conditional single-PUT publication.
- Verify complete remote content before publication or reader access; retire generations with permanent same-key markers so delayed writes cannot resurrect bytes after quota release.
- Enforce per-object limits before upload admission, preserve cleanup accounting and private-file compatibility, and add optional S3 SDK contract CI.

- Add a separate Cloudflare research API facade with no privileged credentials, preserving caller authentication, exact command bytes, idempotency and revision semantics across the native API.
- Generate its command allowlist from the shared catalog; bound request bodies before dispatch, distinguish query recovery from command replay, refuse redirects and preserve artifact lengths through fixed-length streams.
- Retain stream cleanup, enforce bounded idle reads, and document the local proxy disconnect limitation without claiming immediate hosted cancellation.
- Reuse existing SDK, CLI and stdio MCP tools through the facade, with local workerd qualification for command replay, scoped identities, binary transfer, revocation and event streaming.

- Add a native async Cloudflare bridge with exact binding/instance checks, bounded private HTTP control and destination-specific durable wakeup delivery.
- Keep wakeups pending after uncertain or malformed provider replies; never forward credentials through redirects or start a Workflow from notification delivery.
- Drain admitted native outbox calls on cancellation and qualify the bridge against real local Workflows, including concurrent starts and persisted acknowledgment.

- Add a Cloudflare hybrid Workflow driver over the native delivery gateway, with immutable Durable Object generation records, exact pending-work handoff and provider-safe instance IDs.
- Recover duplicate starts and lost replies without resetting research work; preserve observed-history markers, recheck generation on each callback, and explicitly nudge saved local history after a runtime restart.
- Qualify actual local Workflow delivery around CPU training/evaluation, native fencing, redirect refusal and unsupported hosted-authority rejection; add pinned TypeScript/formatting checks and provider-boundary CI tests.

- Add an optional native delivery gateway for remote orchestration adapters, with exact campaign registration, separate scoped credentials and bounded versioned references around the existing delivery kernel.
- Preserve committed-action replay, cancellation custody and transport uncertainty across HTTP; sanitize malformed inputs and provider failures, including deep JSON nesting.
- Document the deployed Cloudflare clock constraint and the hybrid delivery direction without claiming authoritative hosted execution qualification.

- Separate artifact reader ownership from native process inspection with an explicit synchronous provider and immutable owner records; preserve legacy local reader recovery.
- Permit portable controller construction with conservative opaque ownership, retain abandoned pins when closure is unqualified, and recheck session ownership/revision before admitting reads or releasing references.

- Inject synchronous Ed25519 primitives per service while preserving canonical assignment, clock and timing records; load native cryptography only for the default provider.
- Qualify the same signed-envelope codecs through Cloudflare's local Noble adapter, with native verification in both directions, transaction-local signing and explicit rejection of provider failures, malformed outputs and async results.

- Separate portable journal evidence from the native worker database and defer native capture, process-inspection and clock imports until their operations run.
- Accept absolute POSIX and Windows worker paths independently of the controller OS while preserving sealed path strings; reject ambiguous relative paths and verify controller reconciliation with synthetic foreign path metadata.

- Inject controller authority time through a synchronous provider bound to state and signing authority; keep fresh observations inside calibrated admission, input and acceptance decisions.
- Install the native provider in the local runtime while preserving lazy UTC compatibility, shared process epochs, clock poisoning and fork refusal; refuse missing or mismatched calibrated providers.

- Complete service-layer SQL callback migration for artifact authorization, upload lifecycle, evaluation admission, result publication and late-evidence reconciliation.
- Preserve replay-before-preflight, pinned evidence and commit-time revalidation; exercise real CPU stages and original late closure with native connection access forbidden and blob I/O checked outside transactions.

- Move credential management, research/scoped-worker queries, proposal ownership, orchestration inspection and recovery reads onto storage callbacks without expanding caller permissions.
- Preserve atomic credential installation, query snapshots and bounded worker-query scanning; add callback-only service tests covering ownership, revocation and snapshot expiry.

- Run the shared command kernel through a real Durable Object SQL adapter, preserving command replay, event/outbox transactions and rejection fencing across local workerd restart.
- Share schema migrations, reject unversioned/unsupported state, normalize SQL constraints and binary/NULL values, and bound authority counters before JavaScript precision can change their meaning.

- Extract synchronous SQL store/session contracts and a native SQLite adapter while preserving one command kernel, exact acknowledgment replay, nested rollback and conservative rejection fencing.
- Replace affected-row assumptions with `RETURNING`, distinguish required from optional rows, and identify shared transaction authority independently of filesystem paths; keep unported native operations explicitly unavailable to hosted stores.

- Qualify Python Durable Object runtime primitives in local workerd: nested transaction rollback, durable rejection acknowledgments, duplicate contention, process restart and synchronous Ed25519 FFI verified against the native cryptography library.
- Add isolated, locked Cloudflare tooling and an opt-in native fixture without changing core dependencies or claiming the full hosted command/storage deployment is complete.

- Add original glue to adopt an exact, locally supplied Karpathy autoresearch revision without distributing upstream code; freeze data, dependency and kernel identities and restrict mutations to four learning-rate literals.
- Export non-executable tensor checkpoints for an independent frozen-reference scorer, with CPU adoption/contract tests and explicit native CUDA/parity qualification limits.

- Qualify controller-process restart during a live native workload, preserving its original assignment, deadline and custody through explicit late-result reconciliation.
- Extend end-to-end proof for post-access scoring timeouts, exhausted-budget restart, unavailable isolation admission, revoked-session publication and retained artifacts after campaign completion; reconcile the contract ledger with existing native and transport evidence.

- Add a reproducible CPU benchmark comparing funded greedy and portfolio searches under equal allowances, with frozen development/held-out starting seeds and a fresh final audit of each selected candidate.
- Measure timed no-op confirmation panels through authenticated evaluations and the existing paired gate; preserve missing cells, actual costs, optimizer steps, immutable reports and descriptive uncertainty.
- Add replayable discretionary artifact retention/release to the shared agent API, preserving required evidence ownership, and allow a local driver to reduce concurrency within its worker's admission.

- Add an original MLX Metal bigram workload with a platform-specific hash lock, explicit GPU stream/materialization, discarded warmup state, and separate active-training timing.
- Reuse the independent standard-library scorer and qualify real registered-worker training/evaluation on Apple Silicon, including a fixed-step float32 differential check and two injected seeds.

- Add bounded controller-side result acceptance leases, distinct from immutable launch and native execution deadlines, with explicit heartbeat renewal and scoped reads.
- Fence expired publication without releasing custody, quarantine original late evidence, and record the lease in explicit reconciliation; preserve cancellation acknowledgments and original accounting.
- Upgrade SQLite to schema 3 without rewriting old signed records or acknowledgments; unresolved legacy attempts require reconciliation rather than invented lease authority.

- Add a second ordinary CPU research project with fictional pose data, deterministic training, protected evaluation, and two explicit historical receipt readers.
- Qualify historical import followed by fresh native replay, preserving checkpoint and draw-order identity, missing observations, separate panels, and distinct macro/pooled error denominators without a project-specific engine plugin.

- Add generic immutable historical-evidence import with exact raw-byte retention, schema-specific source identity, bounded normalized observations, and shared HTTP/SDK/CLI/MCP access.
- Preserve missing measurements and provenance without minting current attempts, evaluator receipts, budget authority, or promotion eligibility; reject changed content under an existing historical identity.

- Qualify built-wheel installation in a fresh environment outside the checkout, with locked runtime dependencies and no optional accelerator or Temporal packages.
- Exercise installed CLI/native training, independent evaluation, MCP selection/reporting, and source-free replay; add dedicated Linux, macOS, and Windows package jobs.

- Qualify a complete external MCP research cycle across agent-process restart, with independent native execution/evaluation, exact admission replay, observed improvement, and an immutable terminal report.
- Verify that the scoped research agent cannot publish evaluator receipts and that same-principal CLI replay preserves the original completion without extra execution or cost.

- Add a frozen cross-language canonical corpus covering Unicode, numbers, decimal strings, ordering, optional fields, and domain separation.
- Independently verify Python hashes and synthetic Ed25519 assignment signatures in Node, reject tampering, and require this conformance check in the supported-platform CI matrix.

- Add explicit scientific completion with immutable reviewed obligations, preserving UNKNOWN custody and child funding while freezing the final research report.
- Reconcile original late evidence without launching successor work or reopening selection; keep operational-only closure distinct from accepted scientific results.
- Close resources separately after all original obligations settle, with atomic child-funding settlement, replayable receipts, and current operational context through HTTP, SDK, CLI, and MCP.

- Share one provider-neutral delivery kernel between the default local asyncio driver and Temporal Activities, preserving the existing Temporal import surface.
- Persist local startup generations and exact pending references before API delivery, recover lost acknowledgments after reopening, and reject competing steps or checkpoint ownership changes.
- Keep local delivery independent of the Temporal SDK and preserve ready-wait evidence and native custody when its loop stops.
- Drain an admitted API step before propagating async cancellation, so local and Temporal owners cannot close clients or relinquish a checkpoint while that call is still running.

- Add an optional Temporal delivery adapter with one Workflow per campaign epoch, frozen turn/wait Activity inputs, signal-assisted polling, and bounded history continuation.
- Keep agent planning and native execution outside provider retries; recover transferred startup from stable snapshot intents and preserve running CPU work when a Workflow is canceled.
- Sanitize recorded failures, reject foreign Workflow references and conflicting duplicate startup, and qualify real server/worker restart, timeout retries, and history replay in an opt-in suite and dedicated CI job.

- Transfer paused campaigns to fresh driver principals through an immutable verified checkpoint, preserving scientific identities, worker authority, queued work, and child funding.
- Reconcile original target acknowledgments or record distinct absent-ack closure before advancing the driver epoch; require each successor to acknowledge checkpoint recovery before issuing turns or waits.
- Bound checkpoint manifests by canonical byte size and entry count, verify required retained artifacts, and refuse unresolved native or retiring-evaluator work without partial ownership changes.

- Close fenced pending actions without a committed target acknowledgment using a distinct immutable closure, preserving the difference from accepted or rejected research commands.
- Reject delayed original dispatch before it can create a replacement acknowledgment; recover closed ordinals over the same API without replanning, releasing budgets, or changing native custody.

- Add explicit event-history gaps, consistent event watermarks, and frozen paginated recovery snapshots through HTTP, Python, CLI, and MCP.
- Pin catch-up intervals during recovery, verify bounded snapshot chunks, and preserve permanent event and wait evidence independently of logical stream retention.
- Bound snapshot storage, reclaim expired content with terminal tombstones, prevent clock rollback from reviving expired recovery, and migrate SQLite schema transactionally under concurrent openers.

- Persist event/timer waits with exact planning watermarks and destination-bound wakeups in the domain transaction; recover commit-to-notification crashes without losing conditions.
- Add API-only async wait polling and at-least-once delivery, preserve acknowledgments separately from evidence consumption, and wake parent campaigns when child allocations change or close.
- Prevent stale rejected preparations from poisoning a later valid turn, and preserve old wait destinations when orchestration ownership is fenced.

- Persist single-command orchestration turns with dedicated control/evaluator routes, deterministic ordinals, exact action keys, and authoritative acknowledgment receipts.
- Recover accepted or rejected actions across controller gaps without replanning; enforce driver fencing inside target transactions while preserving original acknowledgments and owned read-pin cleanup.
- Protect reserved action keys and physical artifact staging from bare driver calls, and provide an API-only synchronous/async turn driver for future provider adapters.

- Add agent-accessible child creation, unstarted-child cancellation, and replayable funding settlement through the shared API, SDK, CLI, and MCP command registry.
- Inherit the parent's frozen project, evaluator, data inputs, promotion contract, and stopping constraints; enforce bounded depth, cumulative child/trial counters, and pending-proposal allowance across descendants.
- Close allocations atomically with final reports, preserve funds while native work remains unknown, and expose bounded funding context without double-counting descendant costs.

- Add transactional child-allocation accounting with immutable funding identities, live ancestor projections, exact signed ledger entries, and idempotent remainder settlement.
- Preserve commitments and uncertainty after overshoot, block new reservations against ancestor deficits, and keep unused grants held until explicit closure.

- Qualify asynchronous portfolio progress with two registered HTTP CPU workers sharing native capacity: a third candidate enters its workload while the original candidate remains alive, after an independently evaluated sibling promotes.
- Verify original process identity, distinct source lineage, separate evaluator authority, accepted closure, exact attempt counts, and restored shared capacity in the native integration test.

- Advance sealed portfolios through the shared API and CLI without a generation barrier; refill free slots while unresolved siblings retain their own custody.
- Recover the durable selection owner, preserve old-parent trial ancestry, and cancel superseded pending proposals without erasing late responses or consumed request costs.
- Bound plural evidence reads, recheck controller/search revisions, and keep greedy command identities stable through shared pure planning primitives.

- Add the sealed `portfolio-v1` API contract with ordered research themes, exact proposer/instruction/seed admission, and bounded concurrent slots.
- Protect pending trial allowance from manual submission and keep cumulative proposal costs and ordinals after cancellation.
- Serialize portfolio selection through a durable trial owner enforced by observation pinning, resolution, abandonment, and direct promotion commits; retain admitted candidate ancestry across incumbent changes.
- Preserve legacy canonical shapes by omitting absent portfolio fields; reject unsupported paired-confirmation combinations explicitly.

- Add API-driven evaluation control with separate controller/evaluator clients and an `evaluation advance` CLI command for independently running workers.
- Recover authorization and receipt acknowledgment loss through bounded resumable scans, preserving unprocessed page tails and exact evaluator ownership.
- Keep failed scoring, UNKNOWN execution, drain pause, confirmation exposure, and credential failures distinct; complete a real HTTP greedy improvement cycle with a scoped CPU worker and separate evaluator credentials.

- Add a pure greedy planner and bounded local/HTTP search driver, with read-only planning, restart-safe advancement, independent worker/evaluator execution, and CLI bootstrap selection.
- Pin observations before selection, preserve original comparison context across promotion, and recover lost acknowledgments and competing principals without duplicate trial admission or policy progress.
- Respect pause, declared metric targets, budgets, UNKNOWN custody, and fixed confirmation-panel limits; record settled panel failures instead of waiting indefinitely.
- Add explicit correction abandonment that preserves original pins and measurements, records affected comparator evidence, and releases only the policy slot.

- Record immutable typed research observations from accepted trial/evaluator evidence, preserving comparison context and separating cancellation, failure, incomplete evidence, and execution uncertainty from scores.
- Resolve policy trials atomically after an independent promotion, rejection, complete paired-panel assessment, or closed non-scientific outcome; preserve observation consumption and exact search-state revisions.
- Recheck corrected evidence before resolution, retain UNKNOWN policy slots, and recover the gap between promotion and policy progress without repeating selection.

- Admit recorded agent responses atomically with candidate validation, trial graphs, policy-state changes, and immutable transition receipts.
- Preserve proposer attribution, reject stale parent/epoch contexts, retain invalid mutations, and deduplicate same-campaign source trees without implicit replication.
- Share manual and policy candidate operations while keeping campaign and search revision checks distinct; recover accepted admissions despite later source preflight or cleanup failures.

- Add durable external proposal requests, assigned-agent responses, and cancellation through the shared API, SDK, CLI, and MCP contracts.
- Freeze bounded research context and parent identity, enforce search-state revision CAS and cumulative request budgets, and retain late agent output before candidate publication.
- Preserve exact response replay and reported model usage without granting execution, evaluation, or promotion authority; expose bounded pending research in campaign context.

- Add resumable `worker provision-replacement` and `worker install-replacement` commands with persisted fresh identities and exact command replay.
- Verify scoped replacement receipts, preserve original installations and environment registrations, and pin the shared machine registry before first connection.
- Resume interrupted native initialization without copying old driver state or launching probes; replacements remain draining until explicitly activated.

- Add explicit `worker.fence` and `worker.replace_session` commands with immutable receipts, original-binding checks, and atomic worker revisions.
- Require fresh replacement identities, preserve historical evidence authority and occupied resources, and reset presence with admission draining until activation.
- Preserve prior operational projections in replacement history and reject stale fences, reused principals, and competing replacements without changing original experiment accounting.

- Add `research worker recover` for journal-only evidence delivery from an original installation after execution authority changes.
- Reconcile native closure and capacity before network delivery, preserve uncertain snapshots and acknowledgment keys, and keep controller disposition separate from local custody.
- Open pinned native stores without creating replacements; recover without the original interpreter or project checkout and without launching, claiming, cancelling, or accepting research results.

- Add explicit native closure reconciliation with a final `closed_unaccepted` disposition, separate from execution acceptance and scientific eligibility.
- Settle original costs and evaluation exposure despite unusable research outputs, stop undispatched successors, and preserve admitted sibling custody and terminal campaign history.
- Keep nonacceptance sticky across sibling uncertainty and completion; resume local drivers without attempting forbidden evaluator receipts.

- Add immutable quarantined attempt submissions and explicit operator reconciliation under current controller authority.
- Validate ordinary and recovered observations through one contract, preserve original grants and digests, and settle costs once without issuing evaluator receipts.
- Keep incomplete and invalid recovery evidence retained, separate operator permissions from worker credentials, and preserve exact acknowledgments across concurrent preflight failures and epoch changes.

- Preserve immutable worker session records at enrollment and expose explicit historical session and pending-attempt reads.
- Let the authenticated original session finish scoped evidence transfers after an epoch change while current inputs, launch, and scientific publication remain fenced.
- Preserve legacy session reads without implicit migration and reject reused session IDs or unavailable historical authority.

- Poll scoped worker controls independently of experiment execution, with dedicated control threads and durable sequenced heartbeat replay.
- Apply offline cancellation before native receipt, gate fresh launch on completed synchronization, and keep controls flowing while owned work drains.
- Preserve one bounded heartbeat state file across restart and acknowledge control receipts only after the controller accepts their exact heartbeat.

- Persist worker cancellation receipts atomically with assignment journaling, including cancellation delivered before its assignment.
- Keep cancellation effective after acknowledgment and restart, preserve terminal publication bytes, and separate local repair cancellation from remote sequencing.
- Verify cancellation during preparation and native release, calibrated origin authorization, and owned descendant shutdown with real subprocess tests.

- Add independent worker presence and admission records, sequenced heartbeats, and transactional drain control without changing existing grants.
- Persist attempt-scoped cancellation with bounded worker control delivery; distinguish request, journal acknowledgment, and accepted native closure.
- Preserve successful completion racing cancellation, keep UNKNOWN reservations unresolved, and expose the controls through the shared API/SDK/CLI/MCP catalog.

- Execute registered Python environments through frozen, fingerprinted launch adapters inside the attempt's native deadline and resource claim.
- Apply environment-contract admission before reservation, require exclusive accelerator slots, and share MPS/MLX physical device and unified-memory pools.
- Pin registrations in local and HTTP worker enrollment, verify exact demands before native release, and recover publication without reopening worker launch files.
- Add real separate-venv training/evaluation and HTTP execution tests, plus drift failure, launch tampering, shared-device quarantine, and interpreter-loss recovery checks.
- Add optional CPU, PyTorch CUDA/ROCm/MPS, and MLX Metal environment probes without importing or installing frameworks in the controller.
- Register selected interpreters and authorized environment contracts privately, verify observed drift, and expose JSON CLI discovery, probe, registration, and verification commands.
- Separate physical device identity from framework profiles and label environment observations independently from lock, workload, and scientific qualification.
- Add an outbound scoped HTTP worker driver and `research worker start`, with persistent polling, bounded backoff, and a one-pass drain mode.
- Recover durable command intents and native journals before new admission, refill available slots asynchronously, and preserve worker ownership through cancelled HTTP calls and native drains.
- Fence new execution behind observed uncertainty, retain claims accepted during an in-flight failure, and reconcile late publication independently of slower active attempts.
- Add durable CPU worker descriptions, operator provisioning, and pinned HTTP connections through `research worker describe`, `provision`, and `connect`.
- Persist identities before provisioning, export restricted private bundles, and resume interrupted enrollment without rotating tokens or sessions.
- Reject changed profiles, native storage identities, connection bundles, and revoked or missing completed credentials; redact private-record validation failures.
- Execute explicitly calibrated CPU assignments through a live private origin and atomic journal claim/release gates.
- Recheck deadlines after journal lock waits and immediately before native release; keep the hard-deadline watchdog active while authorization is blocked.
- Settle failures before launch without stranding capacity, retain uncertainty after a claimed launch, and preserve fixed deadlines when the origin process dies.
- Invalidate worker sampling origins permanently after native clock faults and reject reconstructed origins for previously attached grants.
- Add a private authenticated supervisor channel with one-use pipe bootstrap, fixed absolute deadlines, bounded frames, and claim/release sequencing.
- Reject stale, mismatched, malformed, or extended permits and close accepted connections atomically with transport shutdown.
- Persist signed timing sidecars and conservative deadline evidence in worker journals without changing legacy admission hashes.
- Encode native counter values as canonical decimal strings, reject precision loss and overflow, and prevent cap refresh from extending authority or changing origins.
- Preserve timed not-started publication after clock failure while refusing any fallback through unqualified local launch gates.
- Share one process-owned clock between scoped time samples, timed assignment issuance, and input-expiry checks.
- Preserve clock invalidation across runtime reopening and reject inherited or mismatched epochs without resetting accepted grants or evidence.
- Expose signed time queries through HTTP, SDK, async SDK, and MCP; keep calibrated enrollment explicit.
- Define separately signed controller-time replies and exact-assignment clock bindings without changing existing assignment signatures.
- Bound time estimates by full roundtrip delay, relative clock-rate assumptions, sample age, and measured read uncertainty; derive conservative absolute caps.
- Bind one-use clock challenges to a live sampling origin and preserve their original anchors on replay.
- Count host suspension in native execution and cleanup deadlines using explicit Linux, macOS, and Windows clock profiles.
- Preserve an absolute supervisor cutoff through preparation and the release barrier; expired allowance cannot become a fresh positive timeout.
- Detect suspend, wall-clock changes, and counter reversal in clock samples for future remote authority-time qualification.
- Publish scoped worker outputs with attempt, role, actor, and session provenance even when bytes deduplicate to another publisher's artifact.
- Transfer required evidence retention atomically from upload receipts to accepted attempt observations without granting workspace read access.
- Run project-scoped CPU workers through HTTP and preserve accepted publication acknowledgments after enrollment revocation.
- Serialize upload lifecycle decisions with byte operations, preserve committed finalization across cleanup failures, and expose resumable duplicate cleanup.
- Authorize exact attempt inputs and retain their storage generation in one transaction without granting workspace read access.
- Stream assigned inputs through HTTP and local/async SDKs with digest and size verification, credential rechecks, and pin cleanup after revocation or failure.
- Restrict enrolled workers to explicit project sets and enforce that scope before job budget reservation.
- Add bounded worker-owned assignment recovery and eligible-job discovery through HTTP, SDK, async SDK, and MCP without workspace-wide read authority.
- Preserve durable accepted/rejected command acknowledgments across local and HTTP replay, including rejection command IDs.
- Distinguish uncertain mutation delivery from domain rejection and query transport failure; require exact replay after lost acknowledgments.
- Require HTTPS outside explicit loopback, verify custom-CA hostnames, and reject malformed command replies before treating them as success.
- Persist a separate controller signing key and pin its identity; reject lost or replaced authority during restart.
- Sign admitted assignments transactionally, execute new local enrollments with public-only verification, and preserve the full signed grant through native journals and result publication.
- Define complete Ed25519-signed worker assignments with public-only verification, enrollment fencing, and canonical interoperability fixtures.
- Add `research run` to freeze a project, create an external-agent campaign, and execute its baseline through the local driver.
- Persist source and bootstrap identity across interrupted uploads and commands, without recapturing changed or missing checkouts.
- Return bounded evidence context and explicit awaiting-proposals, awaiting-completion, or review outcomes without claiming research completion.
- Drive local campaign stages concurrently through persisted API commands and automatic scoped enrollment.
- Recover lost command acknowledgments and pending receipts, retain uncertain capacity, and drain native threads before releasing driver ownership.
- Add `research worker run` with bounded JSON outcomes and explicit blocked exit status.
- Bind initialized local enrollment to persistent journal and machine-registry identities; reject missing or replaced native state before resume.
- Persist separate local worker and evaluator credentials before enrollment and resume interrupted setup without changing their authority.
- Refuse missing or revoked enrollment state, changed execution profiles, and conflicting machine ownership.
- Drain active stage graphs before pausing and preserve queued work for explicit resume.
- Complete only settled research under its stopping rule and atomically capture its final report.
- Generate bounded JSON and Markdown from one evidence snapshot, freeze completed selection, and preserve later corrections.
- Assess complete preselected confirmation panels, preserve failed realizations, and commit statistically qualified selections atomically.
- Keep every paired receipt as a promotion dependency and suspend selection when supporting evidence is corrected.
- Continue bounded native closure observation after a cleanup signal error instead of abandoning draining and reaping.
- Seal complete paired confirmation plans and create all seed-bound candidate/comparator trials atomically.
- Reserve evaluation exposure separately, retain uncertain access, and release only on positive no-access evidence.
- Require planned stage fingerprints and evaluator authority before confirmation scoring can dispatch.
- Persist promotion assessments before atomic incumbent selection, with explicit observed baseline adoption.
- Compare actual training and evaluator profiles, preserve conflicting assessments, and retain rejected candidates.
- Append evidence corrections without rewriting receipts and suspend affected incumbent eligibility.
- Recover a suspended incumbent through explicitly authorized baseline assessment using fresh evidence.
- Add a fixed paired sign gate with exact probabilities, practical-effect checks, and explicit incomplete-panel outcomes.
- Reject infeasible confirmation panels and verify the gate through exhaustive synthetic null signs.
- Authorize evaluation inputs, seeds, panels, and evaluator identities before worker admission.
- Derive authenticated receipts from accepted native reports and preserve incomplete or invalid evidence.
- Advertise supplementary command scopes and publish bounded controller metadata through resumable artifact storage.
- Execute registered CPU stage graphs through durable preparation, native supervision, and verified artifact publication.
- Accept worker observations against their original grants, settle each attempt once, and advance stage dependencies.
- Preserve uncertain execution, retry interrupted publication without relaunch, and distinguish unstarted work from failed execution.
- Enroll explicit worker sessions and atomically reserve ready jobs, attempt budgets, and dispatch intents.
- Enforce worker authority, engine requirements, dependency completion, and campaign/worker concurrency at admission.
- Bind sealed workload contexts, declared interpreters, and live machine claims to native supervisor release.
- Settle cancelled or expired unstarted grants even when their prepared context is missing.
- Add a JSON CLI and typed stdio MCP tools over the shared command and query service.
- Verify replay, structured errors, bounded summaries, and credential revocation through real agent subprocesses.
- Add local and HTTP Python clients, async convenience methods, and durable private runtime initialization.
- Resume artifact transfers through their recorded lifecycle and verify downloaded content.
- Expose authenticated research commands, bounded context, event streams, and verified artifact transfer over HTTP.
- Keep credential authority server-side and validate canonical expiry and safe artifact media types.
- Register immutable project revisions and seal campaign contracts with explicit input and budget rules.
- Preserve candidate validation evidence and create complete trial job graphs atomically.
- Replay committed command responses before transient preflight, with current authorization and atomic rechecks.
- Allow bounded native job-accounting settlement after primary process exit.
- Add a language-neutral workload context/report protocol with bounded artifact verification.
- Train and evaluate an original standard-library CPU language model from immutable project source.
- Reject FIFO inputs before reading project reports, outputs, or source files.
- Arbitrate host resource pools across worker processes and retain uncertain capacity until verified closure.
- Preserve independently observed native closure when a competing deadline signal fails.
- Persist authenticated worker dispatches, launch barriers, cancellation intent, and recoverable observations.
- Supervise bounded native process groups and Windows jobs with capped output and independent deadlines.
- Recover native completion after publication failure without repeating the workload.
- Upgrade FileLock to the patched 4.2 series and retain artifact concurrency coverage.
- Resolve repository manifests from immutable bundles with separate protocol and source identities.
- Share stage-graph validation between project manifests and sealed experiments.
- Flush staged artifacts through writable descriptors for native Windows publication.
- Publish resumable, verified artifacts with workspace access boundaries and fenced storage generations.
- Preserve required evidence and reconcile crashed readers without treating a timeout as process closure.
- Account for staged, retained, and deleting artifact content until verified cleanup completes.
- Capture deterministic source bundles and reject undeclared or protected candidate mutations.
- Materialize isolated source without changing a project's working tree.
- Establish the versioned research domain and canonical hashing contract.
- Separate immutable experiment specifications from mutable attempt dispatch.
- Persist idempotent commands, transactional events, and a recoverable SQLite outbox.
- Account for reservations, uncertainty, settlements, and overshoot without float rounding.
- Validate evaluator identity, input provenance, seeds, panel coverage, and metric semantics.
