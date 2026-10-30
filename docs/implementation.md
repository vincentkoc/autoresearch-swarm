# Implementation and qualification

This is the completion ledger for the design contract. Pending means unproven,
not implicitly supported.

| Area | Implementation | Local verification | Native/provider qualification |
|---|---|---|---|
| Domain schemas and JCS hashing | Implemented foundation | Golden/validation tests pass | Cross-language pending |
| SQLite transactions, events, outbox | Transaction substrate implemented | Restart, independent connections, rollback, watermark tests pass | Foundation CI passes on Linux, macOS, Windows, Python 3.11–3.13 |
| Exact budget arithmetic | Implemented foundation | Exact arithmetic, uncertainty, settlement, overshoot tests pass | Hierarchical accounting pending |
| Evaluator receipts | Pre-execution authorization and authenticated completion implemented | Scoped credentials, exact attempts, replay, incomplete panels, invalid metrics | Local CPU pipeline passes; statistical promotion remains separate |
| Artifact lifecycle | Local verified catalog, generations, retention, quotas, recovery | Corruption, interrupted commit, duplicate upload, retain/delete race, native reader crash tests | Artifact tests pass on Linux/macOS/Windows with patched FileLock; hosted stores pending |
| Local worker and supervision | Authenticated dispatch, durable preparation/journal, native barrier, independent supervisor, bounded output, registered CPU stage execution | Real cancellation, launch race, deadline, transport loss, publication recovery, complete stage graph | Integrated pipeline passes Linux/macOS/Windows CI, Python 3.11–3.13 |
| Machine resource admission | Shared native registry, atomic reservations, quarantine, closure-bound release | Independent-process contention, cross-controller rejection, restart, stale closure, duplicate release tests | Synthetic pool proof; physical device qualification pending |
| Engine environment registry | Optional CPU/PyTorch CUDA/ROCm/MPS/MLX Metal probes, private interpreter registrations, explicit environment contract authorization | Real selected-venv probe, native timeout/descendant closure, bounded output, secret stripping, concurrent registration, drift rejection, synthetic accelerator distinctions | CPU environment probe locally verified; accelerator execution integration, lock reproduction, and workload qualification pending |
| Registered native engines | Frozen launch adapter, selected interpreter, pre-workload environment gate, controller environment allowlist, exact native resource demands, local/remote enrollment | Real empty-venv CPU training/evaluation and scoped HTTP execution; failed drift gate, missing-interpreter publication recovery, file/demand tampering, synthetic MPS/MLX device contention | Local CPU execution proof; physical CUDA/ROCm/MPS/MLX execution and workload qualification pending |
| Project manifests and source isolation | Source bundles, mutation boundary, TOML manifest and immutable resolution implemented | Deterministic capture, pinned Git source, traversal, corruption, dirty-checkout preservation, protected dependency/protocol tests | Linux/macOS/Windows CI passed, including the original CPU workload |
| Workload protocol and original CPU project | Sealed file context, bounded report/output verification, real training and separate evaluator | Immutable source execution, two injected seeds, checkpoint identity, improvement against uniform model | Workload CI passes on Linux/macOS/Windows; integrated controller pipeline matrix pending |
| API, SDK, CLI, MCP | HTTP, local/remote Python SDK, async methods, JSON CLI, stdio MCP, private runtime | Real loopback HTTP and agent subprocesses, shared replay, bounded transfer/context, revocation, runtime restart | Linux/macOS/Windows transport CI passes |
| Controller commands | Project revisions, campaigns, candidate validation, trial graphs, worker admission, settlement, evaluation, promotion | Replay, frozen contracts, atomic claims, uncertainty, late closure, observed and paired selection | Local CPU pipeline and paired controls pass |
| Campaign lifecycle and reports | Drain pause, explicit resume, stopping gates, atomic finalization, bounded JSON/Markdown snapshots | Real stage drain, incomplete/uncertain work rejection, target/count semantics, report rollback, immutable corrections | Native matrix pending for this slice |
| Local enrollment | Persisted worker/evaluator identities, resumable provisioning, host registry ownership | Concurrent startup, partial installation, revocation, missing state, changed epoch/profile, cross-controller rejection | Local tests pass; current native matrix pending |
| Remote enrollment | Persistent CPU description, operator-provisioned scoped bundle, pinned HTTP connection, native storage identity | Concurrent provisioning, lost installation/enrollment/export acknowledgments, restricted HTTP execution, missing/revoked credential, changed bundle/profile/store, real CLI subprocesses, secret-safe errors | Loopback HTTP/native proof; physical host enrollment and current native matrix pending |
| Local async driver | Parallel stage delivery, durable command intents, native recovery, independent receipt completion, CLI | Lost acknowledgment, restart, cancellation ownership, same-batch capacity recovery, uncertain execution, real parallel stages | Local CPU tests pass; native matrix pending |
| Scoped HTTP worker driver | Outbound asynchronous scheduling, worker-only command intents, recovery fences, bounded polling, foreground CLI | Independent controller/driver/supervisor processes, running driver death/restart with same native identity, asynchronous slot refill, lost claim/publication replies, control-call cancellation ownership, unknown during awaited RPC | Local macOS and loopback proof; physical swarm, remote lifecycle signals, and current native matrix pending |
| One-command local run | Frozen source, replayable bootstrap, automatic worker/evaluator enrollment, external-agent baseline and context | Every creation/upload acknowledgment boundary, missing checkout, corrupt archive, same-key contention, option conflict, pause preservation, real CLI resume | Local tests pass; native matrix pending |
| Signed assignment authority | Complete payload signing, pinned durable keys, atomic admission, public-only native execution, journal preservation | Canonical fixture, separate verifier process, tampering, rollback, restart, missing/replaced keys, legacy migration | Local scoped HTTP and clock admission pass; physical remote qualification pending |
| Native deadline clocks | Suspend-inclusive Linux/macOS/Windows primitives, awake companion, absolute supervisor caps | Native cross-process readings, discontinuity arithmetic, delayed-release and injected-suspend subprocess tests | Local macOS passes; native matrix and actual host-suspend qualification pending; remote authority calibration remains separate |
| Authority-time protocol | Domain-separated signed replies and assignment bindings, one-use sampler, conservative intervals and caps | Golden signatures, public-only subprocess verification, asymmetric-delay/rate bounds, stale/tampered responses, sticky origin faults | Integrated live-origin native gates locally verified; physical clock-rate qualification pending |
| Controller time admission | Process-owned clock, explicit enrollment mode, scoped endpoint, atomic sidecars and timed input pins | Real HTTP/async sampling, reopened runtime, native fork refusal, independent-process epoch mismatch, clock failure/replay/input-pin tests | Local calibrated CPU execution verified; multi-controller and physical remote worker qualification pending |
| Durable worker timing | Signed sidecar admission, precision-safe counter records, non-extending cap attachment, static publication verification | Canonical golden above safe-integer limit, tampered bounds, concurrent attachment, legacy admission replay, timed not-started publication after clock failure | Integrated live-origin admission locally verified; platform matrix pending |
| Private origin transport | One-use bootstrap, authenticated loopback frames, fixed deadline, two phases, non-extending permits | Independent-process exchange, malformed peers, close/accept race, fragmentation, suspend, origin disconnect and changed-reply rejection | Transport and native integration locally verified; platform matrix pending |
| Calibrated native execution | Live sampler inside journal claim/release transactions, immediate launch-cap check, independent held/running watchdog | Scoped HTTP CPU execution, lock delay at both gates, lost permits, startup failure, cancelled held child, actual origin death before/after release | Local macOS proof; native matrix, physical clock drift/suspend, and cross-host provisioning pending |
| Remote delivery semantics | Durable acknowledgments, uncertainty classification, HTTPS policy and trusted custom CA | Real post-commit timeout/replay, rejection replay, malformed responses, URL boundaries, certificate and hostname tests | Local HTTP/TLS qualification; physical remote worker pending |
| Scoped worker discovery | Project admission restrictions and own-assignment queries through HTTP/SDK/MCP | Scoped HTTP claim, forbidden reads, project rejection before reservation, bounded cursor isolation, pause and stale-session tests | Independent driver proof; remote lifecycle signals and physical swarm pending |
| Scoped worker inputs | Exact assignment references, authorization in pin transaction, retained-generation HTTP/SDK streams | Real restricted downloads, unrelated input rejection, revocation before pin and content request, admitted-stream cleanup, grant expiry | Independent scoped driver proof; physical-host qualification pending |
| Scoped worker outputs | Role-bound upload receipts, canonical dedup, atomic evidence retention, guarded upload lifecycle, HTTP native execution | Restricted CPU stage graph, separate evaluator authority, wrong-role rejection, duplicate logs, abort race, cleanup recovery, replay after revocation | Independent scoped driver and publication-recovery proof; physical-host qualification pending |
| Search policies and atomic promotion | Observed/paired assessment, atomic selection, correction, explicit baseline recovery; search pending | Real CPU evidence, incumbent CAS, rejected control, profile mismatch, invalidation before/after commit | Equal-budget research-policy comparison pending |
| Statistical gates and exposure | Fixed paired sign gate, sealed plans, exposure accounting, complete-panel assessment | Exact null signs, atomic panel rollback, seed reservation, profile binding, no-access release, actual paired CPU control | Independent workload variance and held-out generalization pending |
| Existing-project adoption | Pending | Pending | Private replay retained separately |
| Nested campaigns and policy benchmark | Pending | Pending | Held-out comparison pending |
| Temporal | Pending | Pending | Real server pending |
| Cloudflare | Pending | Pending | Hosted deployment pending |
| Actual reward-driven policy learning | Pending | Pending | Training/evaluation pending |
| Optional UI | Pending | Pending | Browser workflow pending |
| Rust interoperability | Pending | Pending | Component gate remains explicit |

## Contract scenarios

The suite covers schemas, artifacts, source, manifests, budgets, native supervision,
journals, resources, workloads, controller commands, evaluation, selection, and
SDK transports. It includes real process, deadline, race, and restart scenarios.
CI targets Linux, macOS, and Windows with Python 3.11–3.13. Results qualify the
tested revision and platform; local success does not establish provider or
scientific behavior. Native qualification is tracked separately.

The design defines C01–C42. Unit coverage alone does not qualify their complete
transport, process, or provider behavior.

- **C01** — Submit the same command twice. Pending end-to-end evidence.
- **C02** — Reuse an idempotency key with changed payload. Pending end-to-end evidence.
- **C03** — Two workers claim one exclusive device. Pending end-to-end evidence.
- **C04** — Two campaigns reserve the final available budget. Pending end-to-end evidence.
- **C05** — Controller crashes after state commit but before notification. Pending end-to-end evidence.
- **C06** — Result arrives twice. Pending end-to-end evidence.
- **C07** — Old worker session publishes after a new fence. Pending end-to-end evidence.
- **C08** — Lease expires while the child still runs. Pending end-to-end evidence.
- **C09** — Workflow times out after dispatch. Pending end-to-end evidence.
- **C10** — Cancellation is requested for an offline worker. Pending end-to-end evidence.
- **C11** — Worker reconnects after controller restart. Pending end-to-end evidence.
- **C12** — Artifact upload succeeds but result commit fails. Local CPU pipeline recovers publication without re-execution or duplicate settlement; remote-provider proof pending.
- **C13** — Required artifact is corrupt or absent. Pending end-to-end evidence.
- **C14** — Candidate changes a protected evaluator dependency. Pending end-to-end evidence.
- **C15** — Crashed run has no metric. Pending end-to-end evidence.
- **C16** — Candidate and comparator use different forbidden protocols. Pending end-to-end evidence.
- **C17** — Two promotions reference the same incumbent revision. Local concurrent commands select one incumbent and preserve the conflicting assessment.
- **C18** — Child campaigns reserve concurrently. Pending end-to-end evidence.
- **C19** — Child finishes and releases unused budget. Pending end-to-end evidence.
- **C20** — Policy receives a duplicate observation. Pending end-to-end evidence.
- **C21** — Controller crashes after a proposal-model response. Pending end-to-end evidence.
- **C22** — Seed changes between declared replicates. Pending end-to-end evidence.
- **C23** — Evaluation returns an incomplete panel. Local pipeline records incomplete evidence and does not make it rankable.
- **C24** — Protected evaluation times out after access. Pending end-to-end evidence.
- **C25** — Adapter ownership changes. Pending end-to-end evidence.
- **C26** — Event cursor falls outside retained stream history. Pending end-to-end evidence.
- **C27** — Worker lacks a required isolation feature. Pending end-to-end evidence.
- **C28** — Process spawns a child and cancellation occurs. Pending end-to-end evidence.
- **C29** — Native clocks differ. Pending end-to-end evidence.
- **C30** — API/MCP/CLI perform the same operation. Shared SDK/HTTP replay and real CLI/MCP command replay pass locally; integrated execution remains pending.
- **C31** — Controller restarts with budget exhausted. Pending end-to-end evidence.
- **C32** — Campaign completes with an unresolved resource. Pending end-to-end evidence.
- **C33** — A late valid result returns after lease expiry. Pending end-to-end evidence.
- **C34** — An already accepted result replays after epoch change. Pending end-to-end evidence.
- **C35** — A training worker fabricates an evaluation ID. Local pipeline rejects worker publication and substitution of a training attempt.
- **C36** — Two controllers enroll the same exclusive device. Pending end-to-end evidence.
- **C37** — Concurrent policy computations use one state revision. Pending end-to-end evidence.
- **C38** — Previously promoted evidence is invalidated. Local correction preserves the receipt and suspends the incumbent without selecting an ancestor.
- **C39** — Events arrive while a client recovers from a cursor gap. Pending end-to-end evidence.
- **C40** — Retained artifacts outlive a completed job. Pending end-to-end evidence.
- **C41** — A child budget settles after repeated notifications. Pending end-to-end evidence.
- **C42** — Python and another implementation hash edge-case fixtures. Pending end-to-end evidence.
