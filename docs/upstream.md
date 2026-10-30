# Pinned upstream adoption

`examples/upstream` contains original glue for an operator-supplied checkout of
[Karpathy's autoresearch at 228791fb](https://github.com/karpathy/autoresearch/tree/228791fb499afffb54b46200aca536f79142f117).
It creates an ordinary project with the same source, artifact, worker and evaluator
contracts as the other examples. It adds no project-specific controller API.
Upstream source, data, tokenizer files and kernel binaries are not distributed here.
The MIT license covers this repository's original glue, not supplied upstream assets.

## Supported experiment

The initial mutation surface consists of four literal assignments in `train.py`:
`EMBEDDING_LR`, `UNEMBEDDING_LR`, `MATRIX_LR` and `SCALAR_LR`. Each must be a finite
number between 0 and 10. The normalized AST must otherwise match the frozen reference.
Changes to model architecture, batching, imports, evaluation, data order, seeds,
schedules or clocks fail before candidate execution. Comments and whitespace may change.
This is an intentionally narrow compatibility study, not arbitrary upstream code search.

The wrapper preserves seed 42, the 300-second active-training clock, the original
completed-step stopping rule, and the 11 retained updates excluded from that clock.
Startup, warmup, the candidate's diagnostic evaluation and checkpoint export still
consume native execution time. The final update can overshoot the active budget.
These semantics come from the
[pinned training loop](https://github.com/karpathy/autoresearch/blob/228791fb499afffb54b46200aca536f79142f117/train.py).
They differ from the original MLX bigram example's discarded warmup.

Training emits safetensors weights and strict JSON metadata. The independent scoring
process reconstructs the model from protected reference code, initializes the original
dtypes and rotary buffers, checks tensor names/shapes/dtypes, and loads weights strictly.
It uses the frozen preparation evaluator, validation shard, batch size and byte accounting.
The candidate's printed `val_bpb` is a diagnostic; it cannot become the evaluation receipt.

The comparison class is `karpathy-228791fb-lr-v1`. A Swarm result is not an upstream
parity claim: the additional wrapper, checkpoint, environment and independent scoring
path must first be qualified on the selected hardware. Training time remains
`project_reported`, and evaluator trust is `authenticated`, not hostile-code isolation.

## Prepare an adoption

Provide an existing local checkout containing the pinned commit. Adoption reads
immutable Git objects; uncommitted work and the current branch are not used or changed.
It verifies both source hashes before creating the destination. An existing destination
is refused, including an empty directory. No upstream module is imported during adoption.

Prepare three operator-owned files:

1. `data-manifest.json`: schema version `1.0`, `vocab_size: 8192`, and a `files` array.
   Each entry has `input`, `path`, `digest` and `size_bytes`. Input names start with
   `data_`; paths are `data/shard_NNNNN.parquet`, `tokenizer/tokenizer.pkl` or
   `tokenizer/token_bytes.pt`. Include at least one training shard, validation shard
   `06542`, and both tokenizer files. Digests are `sha256:` plus 64 lowercase hex digits.
   No absolute local paths belong in this manifest. At most 64 files, each at most
   4 GiB, are admitted by this example.
2. `execution.json`: schema version `1.0`, a `packages` mapping from installed
   distribution name to exact version, and a `kernels` mapping. Declare at least
   `torch`, `kernels`, `safetensors`, `numpy`, `pyarrow`, `rustbpe`, `tiktoken` and
   `requests`; Torch must be 2.9.1, including any installed build suffix. Both
   `varunneal/flash-attention-3` and `kernels-community/flash-attn3` require full
   40-character commit revisions. Runtime checks every declared installed version.
3. `environment.lock`: the lock used to prepare the selected CUDA environment.
   This example freezes its bytes; it does not resolve dependencies or prove that
   an arbitrary lock reproduces installed binaries. Supply a real transitive,
   hash-verified lock and qualify it with the registered worker.

Tokenizer pickle and Torch token-byte files are trusted bootstrap inputs supplied
by the operator. They are hash-verified before preparation loads them. Do not substitute
untrusted files. Candidate weights use safetensors, not pickle.

The pinned kernel loader injects explicit revisions into `get_kernel`; see the
[official loader contract](https://huggingface.co/docs/kernels/v0.12.2/en/api/kernels).
Choose a compatible pinned loader version and kernel artifacts. Later loader releases
changed repository types, so a newer package version is not automatically compatible
with the older upstream repository names. Pre-cache qualified kernels if offline
execution is required. `network_policy=inherit` makes no network-denial claim.

From this repository, with the core development environment installed:

```sh
uv run python -m examples.upstream.adopt ./upstream-checkout ./private-study \
  --data ./data-manifest.json --execution ./execution.json --lock ./environment.lock \
  --memory-bytes 34359738368 --accelerator-memory-bytes 85899345920 \
  --scratch-bytes 68719476736 --cpu-slots 8 --timeout-ms 1800000
```

These capacities are an illustrative admission for a large CUDA device, not measured
requirements. Select actual supported capacities. Both stages reserve one exclusive
CUDA device. Scratch admission must cover copied inputs, outputs and compiler work;
resource admission does not impose an OS filesystem or memory quota.

## Execute through the shared API

Capture the generated project with `capture_project`, resolve its environment contract,
and register the prepared CUDA interpreter as described in [engine profiles](engines.md).
Upload each data file through `ResearchClient.upload`; verify its returned digest and
size against the data manifest. Construct `input_artifacts` keyed by each entry's
`input` name. Use `RunOptions(seed=42, max_trials=1, input_artifacts=input_artifacts)`
with `run_project(..., registration=registration, cpu_slots=8)` for a local baseline.
Use the same artifact mapping in `campaign.create` when controlling remote workers.

Each stage receives only declared artifacts. Training receives all selected shards;
scoring receives the tokenizer, token-byte table, validation shard and training outputs.
The runtime copies verified bytes into its own declared layout before loading them.
It rewrites the preparation cache assignment before module definitions execute, so
captured default arguments cannot discover the ambient cache. See the
[pinned preparation code](https://github.com/karpathy/autoresearch/blob/228791fb499afffb54b46200aca536f79142f117/prepare.py).

External agents continue to use ordinary candidate submission, validation, trial,
comparison and promotion operations. Keep seed 42 in this initial compatibility mode.
Do not claim multi-seed confirmation from repeated identical seeded trials. A genuine
multi-seed profile requires a separately versioned adaptation and parity qualification.

## Qualification

CPU tests qualify exact Git-object adoption, wrong-hash rejection before writes,
checkout preservation, destination refusal, the allowed AST surface, frozen dependency
closure, cache default binding, context/checkpoint validation and data-byte verification.
An in-memory read of the pinned public sources additionally matched both source hashes
and the expected AST extraction boundary. No upstream files were retained in this repository.

An optional static test consumes an existing checkout without fetching it:

```sh
RESEARCH_UPSTREAM_CHECKOUT=./upstream-checkout \
  uv run pytest tests/test_upstream_adoption.py
```

Native CUDA execution, kernel loading, memory fit, checkpoint roundtrip, evaluator
parity, reproducible environment installation and scientific improvement remain
unqualified. Complete those checks on prepared hardware before treating this example
as a supported benchmark. No raw upstream console score or CPU fixture closes that gap.

The registered bootstrap probes Torch before running project code. Although the
wrapper applies the upstream allocator setting before its own Torch import, it cannot
establish that the allocator saw that setting before bootstrap initialization. Native
parity qualification must include this ordering; no allocator equivalence is claimed.
