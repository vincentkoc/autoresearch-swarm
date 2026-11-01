# Changelog

## Unreleased

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
