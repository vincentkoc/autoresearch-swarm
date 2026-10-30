# Implementation and qualification

This is the completion ledger for the design contract. Pending means unproven,
not implicitly supported.

| Area | Implementation | Local verification | Native/provider qualification |
|---|---|---|---|
| Domain schemas and JCS hashing | Implemented foundation | Golden/validation tests pass | Cross-language pending |
| SQLite transactions, events, outbox | Transaction substrate implemented | Restart, independent connections, rollback, watermark tests pass | Cross-platform CI pending |
| Exact budget arithmetic | Implemented foundation | Exact arithmetic, uncertainty, settlement, overshoot tests pass | Hierarchical accounting pending |
| Evaluator receipts | Validator implemented | Identity, seed, provenance, and panel tests pass | Authenticated transport pending |
| Artifact lifecycle | Pending | Pending | Pending |
| Local worker and supervision | Pending | Pending | Each supported OS pending |
| Project manifests and source isolation | Pending | Pending | Original workload pending |
| API, SDK, CLI, MCP | Pending | Pending | Real transport pending |
| Remote workers and scheduling | Pending | Pending | Physical swarm pending |
| Search policies and atomic promotion | Pending | Pending | Equal-budget research pending |
| Statistical gates and exposure | Pending | Pending | Workload variance pending |
| Existing-project adoption | Pending | Pending | Private replay retained separately |
| Nested campaigns and policy benchmark | Pending | Pending | Held-out comparison pending |
| Temporal | Pending | Pending | Real server pending |
| Cloudflare | Pending | Pending | Hosted deployment pending |
| Actual reward-driven policy learning | Pending | Pending | Training/evaluation pending |
| Optional UI | Pending | Pending | Browser workflow pending |
| Rust interoperability | Pending | Pending | Component gate remains explicit |

## Contract scenarios

The first foundation suite has 53 passing tests on Python 3.12/macOS, with Ruff
and strict source type checks. It exercises the underlying ledger and validators.
It does not yet establish complete worker, API, provider, or scientific behavior.

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
- **C12** — Artifact upload succeeds but result commit fails. Pending end-to-end evidence.
- **C13** — Required artifact is corrupt or absent. Pending end-to-end evidence.
- **C14** — Candidate changes a protected evaluator dependency. Pending end-to-end evidence.
- **C15** — Crashed run has no metric. Pending end-to-end evidence.
- **C16** — Candidate and comparator use different forbidden protocols. Pending end-to-end evidence.
- **C17** — Two promotions reference the same incumbent revision. Pending end-to-end evidence.
- **C18** — Child campaigns reserve concurrently. Pending end-to-end evidence.
- **C19** — Child finishes and releases unused budget. Pending end-to-end evidence.
- **C20** — Policy receives a duplicate observation. Pending end-to-end evidence.
- **C21** — Controller crashes after a proposal-model response. Pending end-to-end evidence.
- **C22** — Seed changes between declared replicates. Pending end-to-end evidence.
- **C23** — Evaluation returns an incomplete panel. Pending end-to-end evidence.
- **C24** — Protected evaluation times out after access. Pending end-to-end evidence.
- **C25** — Adapter ownership changes. Pending end-to-end evidence.
- **C26** — Event cursor falls outside retained stream history. Pending end-to-end evidence.
- **C27** — Worker lacks a required isolation feature. Pending end-to-end evidence.
- **C28** — Process spawns a child and cancellation occurs. Pending end-to-end evidence.
- **C29** — Native clocks differ. Pending end-to-end evidence.
- **C30** — API/MCP/CLI perform the same operation. Pending end-to-end evidence.
- **C31** — Controller restarts with budget exhausted. Pending end-to-end evidence.
- **C32** — Campaign completes with an unresolved resource. Pending end-to-end evidence.
- **C33** — A late valid result returns after lease expiry. Pending end-to-end evidence.
- **C34** — An already accepted result replays after epoch change. Pending end-to-end evidence.
- **C35** — A training worker fabricates an evaluation ID. Pending end-to-end evidence.
- **C36** — Two controllers enroll the same exclusive device. Pending end-to-end evidence.
- **C37** — Concurrent policy computations use one state revision. Pending end-to-end evidence.
- **C38** — Previously promoted evidence is invalidated. Pending end-to-end evidence.
- **C39** — Events arrive while a client recovers from a cursor gap. Pending end-to-end evidence.
- **C40** — Retained artifacts outlive a completed job. Pending end-to-end evidence.
- **C41** — A child budget settles after repeated notifications. Pending end-to-end evidence.
- **C42** — Python and another implementation hash edge-case fixtures. Pending end-to-end evidence.
