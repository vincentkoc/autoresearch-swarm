# Project instructions

Read the relevant design and tests before changing a public contract.

- Keep domain records independent of transports, provider SDKs, and project model code.
- Use immutable source/spec identities and separate per-attempt dispatch authority.
- Keep state, events, budget entries, and outbox effects in one transaction.
- Never treat lease expiry as proof of process termination.
- Keep training measurements separate from authenticated evaluator receipts.
- Do not hide failed attempts or silently change comparison semantics.
- Do not add vendor integrations as mandatory core dependencies.
- Ordinary research repositories use manifests and commands, not platform subclasses.
- Run targeted tests, Ruff, and type checking for changed code.
- Use real subprocess tests for supervision and independent connections for database races.
- Never claim hardware/provider qualification from mocked execution.
- Keep public fixtures synthetic. Do not include personal paths, private data, or credentials.
- Add user-visible changes to CHANGELOG.md with their implementation.
