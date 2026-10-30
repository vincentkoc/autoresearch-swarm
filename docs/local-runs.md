# Local research runs

Run a repository that contains a `research.toml` contract:

~~~sh
research run .
~~~

For the bundled CPU project, use:

~~~sh
uv run research run examples/cpu_bigram --key first-bigram
~~~

The command initializes local state, enrolls a CPU worker and separate evaluator,
captures immutable source, and creates a campaign through the shared API. It then
validates and executes the baseline candidate. It leaves the source checkout unchanged.

The campaign uses the `external-v1` search policy. External agents supply proposals.
The JSON result includes campaign, candidate, and trial
IDs, a source digest, bounded execution results, and current research context.
An authenticated baseline receipt is an observation. The runner does not promote
it or claim a validated improvement.

## Resume the same work

Repeat the command with the same key and options. Without `--key`, the normalized
source path identifies the run. An explicit key also works after the checkout moves
or disappears. Source selection happens only when the run first captures its bundle.

The run plan records the archive digest and size, source tree digest, resolved Git
commit when applicable, options, and delivery authority. Commands retain their
original payload, revision, and idempotency key. An interrupted acknowledgment
does not create a second project, campaign, candidate, or trial.

Later edits to source leave an existing run unchanged. Use a new key for a new source
snapshot. Changed options under an existing key produce an idempotency conflict.
The runner does not override an operator's pause or restart a terminal campaign.

A missing local archive can be restored from its exact verified artifact. A corrupt
archive requires recovery. An interrupted capture with no complete archive requires
a new key or explicit recovery. Neither case silently captures later checkout bytes.

## Budgets and compute

`--max-trials` defaults to 10. The initial attempt budget equals that limit times
the number of stages. The execution budget includes every stage's timeout and
native cleanup allowance for each planned trial. All execution uses these accounts.

`--cpu-slots` defaults to 1. The same value controls local CPU admission and campaign
trial parallelism. The current one-command path requires compatible Python CPU
profiles. Unsupported engine, isolation, network, or resource requirements fail
without rewriting the project contract. GPU engine qualification remains separate.

Use `--seed`, `--primary-metric`, `--manifest`, or `--source-revision` to set frozen
inputs. A project with several metrics requires `--primary-metric`. Use
`--inputs-file` for a JSON mapping of external input names to verified artifact
references. Missing required inputs prevent campaign sealing and trial submission.

The global `--state-dir` option selects the local controller. Keep its directory
outside the source tree. `--machine-registry` selects an explicitly managed native
registry. It must not serve as a way to bypass another controller's device ownership.

## Continue through the agent API

`awaiting_proposals` means the baseline has valid evidence and trial budget remains.
Inspect the returned context, propose a candidate, validate its mutation boundary,
and submit a trial through the same API used by the CLI and MCP tools. Run
`research worker run CAMPAIGN_ID` to execute additional submitted work.

`awaiting_completion` means the trial count reached its declared limit. It does not
mean the campaign is complete. Inspect evidence, make any authorized selection,
and call `campaign.complete` when its stopping and settlement conditions permit it.

`needs_review` preserves a failed or invalid baseline. `blocked` identifies work
that cannot progress under current execution or ownership conditions. Both return
exit code 3. Command errors return 2. Other outcomes return 0. An interrupt returns
130 after the local driver drains owned native threads.

The foreground runner exits when no more admitted work can progress. It does not
leave a background scheduler running. State remains available through local SDK,
CLI, and MCP calls. Run `research serve` separately for authenticated HTTP access.
See [native workers](native-workers.md) for cancellation and recovery semantics.
