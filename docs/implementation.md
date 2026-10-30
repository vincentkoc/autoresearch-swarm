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
| Project manifests and source isolation | Source bundles, mutation boundary, TOML manifest and immutable resolution implemented | Deterministic capture, pinned Git source, traversal, corruption, dirty-checkout preservation, protected dependency/protocol tests | Linux/macOS/Windows CI passed, including the original CPU workload |
| Workload protocol and original CPU project | Sealed file context, bounded report/output verification, real training and separate evaluator | Immutable source execution, two injected seeds, checkpoint identity, improvement against uniform model | Workload CI passes on Linux/macOS/Windows; integrated controller pipeline matrix pending |
| API, SDK, CLI, MCP | HTTP, local/remote Python SDK, async methods, JSON CLI, stdio MCP, private runtime | Real loopback HTTP and agent subprocesses, shared replay, bounded transfer/context, revocation, runtime restart | Linux/macOS/Windows transport CI passes |
| Controller commands | Project revisions, campaign activation, candidate validation, atomic trial graphs, worker admission, result settlement, evaluation authority | Concurrent replay, frozen baseline, mutation boundary, atomic claims, stage progression, uncertainty, late closure | Local CPU execution and evaluation pass; promotion service pending |
| Remote workers and scheduling | Pending | Pending | Physical swarm pending |
| Search policies and atomic promotion | Observed assessment, atomic selection, append-only correction implemented; search pending | Real CPU evidence, concurrent incumbent CAS, rejected control, training-profile mismatch, invalidation before/after commit | Paired plan integration and equal-budget research pending |
| Statistical gates and exposure | Pure fixed paired sign gate implemented; exposure service pending | Exact null-sign enumeration, direction symmetry, incomplete panels, practical threshold, numeric extremes | Independence and workload variance pending |
| Existing-project adoption | Pending | Pending | Private replay retained separately |
| Nested campaigns and policy benchmark | Pending | Pending | Held-out comparison pending |
| Temporal | Pending | Pending | Real server pending |
| Cloudflare | Pending | Pending | Hosted deployment pending |
| Actual reward-driven policy learning | Pending | Pending | Training/evaluation pending |
| Optional UI | Pending | Pending | Browser workflow pending |
| Rust interoperability | Pending | Pending | Component gate remains explicit |

## Contract scenarios

The foundation suite has 53 tests. Artifact, source, and manifest tests extend it to
116, qualified on Linux/macOS/Windows and Python 3.11–3.13. Native supervision,
journal, resource, workload, controller, and SDK tests extend the suite to 212 passing local tests, including
real process, deadline, race, and recovery coverage. This does not
establish provider or scientific behavior. The integrated CPU worker adds eleven
local execution/recovery scenarios; native qualification is tracked separately.

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
