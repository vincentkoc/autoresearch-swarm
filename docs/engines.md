# Execution environments and engine profiles

Engine profiles describe the environment that executes an ordinary project command.
They do not import project model classes or translate tensors between frameworks.
The framework and device backend remain separate fields.

| Profile | Framework | Device backend | Current probe |
|---|---|---|---|
| `python-cpu` | Python | CPU | Interpreter, OS, processor, installed package inventory |
| `pytorch-cuda` | PyTorch | CUDA | CUDA build, device availability and selected device properties |
| `pytorch-rocm` | PyTorch | ROCm | HIP build, device availability and selected device properties |
| `pytorch-mps` | PyTorch | MPS | Apple Silicon, built support and runtime availability |
| `mlx-metal` | MLX | Metal | Apple Silicon, Metal availability and device information |

The swarm's native runner can use a registered interpreter for manifest stages.
The CPU and reactive examples run locally. The original MLX bigram workload
also has a historical native Metal training and independent scoring record on
an Apple M5 Max. That result applies to its recorded runner and hardware.
[Archived MLX qualification](https://github.com/vincentkoc/autoresearch-swarm/blob/b8b44174b925483c8b202163fef12b733517fa49/docs/mlx-workload.md)

## Historical Windows qualification

The [engine qualification run](https://github.com/vincentkoc/autoresearch-swarm/actions/runs/41242674788)
passed at `cb3779b7121c4fb05cc251109da8582eb4ef946c`:

| Python | Result | Duration |
|---|---|---|
| 3.11 | 58 passed, one POSIX-only skip | 182.97 seconds |
| 3.12 | 58 passed, one POSIX-only skip | 165.78 seconds |
| 3.13 | 58 passed, one POSIX-only skip | 139.30 seconds |

This covers native architecture probing, parent-environment exclusion, selected
virtual environments, registered CPU stages and HTTP workers, concurrent worker
refill, publication recovery, replacement and missing-interpreter recovery.
Python 3.11 previously failed because its platform helper depended on stripped
environment variables. The native query fixes that failure; bootstrap test
helpers now also use the production supervisor's isolated environment.
This run does not qualify accelerator workloads or a physical multi-host fleet.

## Probe an installed interpreter

~~~sh
research engine list
research engine probe python-cpu
research engine probe pytorch-cuda --python /opt/research/venv/bin/python --device-index 0
research engine probe mlx-metal --python /opt/research/mlx/bin/python
~~~

The command returns versioned JSON. Exit code `0` means the selected environment
was observed as available. Exit code `3` reports an unavailable or failed probe;
invalid requests use the ordinary structured error and exit code `2`.

Statuses distinguish `dependency_missing`, `platform_unsupported`,
`backend_unavailable`, `probe_failed`, `timeout`, and `output_limit`.
A missing package is different from an installed package whose import fails.

The selected Python runs with `-I` in an empty private working directory.
Only operating-system environment necessities cross the process boundary.
Caller credentials, `PYTHONPATH`, and inherited device-visibility settings are
excluded. The native supervisor bounds runtime, captures at most 512 KiB per
stream, and closes its process scope. Partial output, malformed JSON, nonzero
exit, or missing native closure cannot produce an available descriptor. Import
diagnostics are not returned. Capture files are removed after proven closure.
No package installation or tensor workload occurs.

Probe custody lives under the machine's private
`~/.local/state/autoresearch-swarm-engine-probes` directory. `--probe-dir` selects
an explicit alternative; registrations remember it for verification. The custody
record is written before native creation and includes the held identity before
release. If closure is unresolved, the command returns `EXECUTION_UNKNOWN`, keeps
bounded output and native evidence, and blocks subsequent probes using that
directory. Restart does not clear the guard. Inspect `pending.json` and establish
native closure through explicit recovery; changing directories or deleting the
record is not closure evidence. A proven closed receipt permits cleanup recovery.

The interpreter invocation path is preserved. A virtual environment's Python
may be a symlink to a base interpreter; resolving that symlink before invocation
would select the wrong package environment.

## Register an environment

From the repository root, register the CPU bigram project to obtain its
environment contract digest. The command below registers the interpreter used
by `uv run`. To use another installed environment, pass its Python executable
with `--python`.

~~~sh
uv sync --locked --group dev
contract=$(uv run --no-sync research project register examples/cpu_bigram --db .research/projects.sqlite | \
  python3 -c 'import json, sys; print(json.load(sys.stdin)["manifest"]["environment_contract_digest"])')
uv run --no-sync research engine register python-cpu \
  --file .research/engines/bigram.json --environment-contract "$contract" \
  --probe-dir .research/probes
uv run --no-sync research engine verify --file .research/engines/bigram.json
~~~

`--environment-contract` may be repeated. Registration persists an immutable
private record before returning its public description. Concurrent identical
registrations return the same identity. Changed inputs fail without overwriting
the record. Verification checks the binary hash, interpreter prefixes, package
inventory, framework build, device, OS, and other observed descriptor fields.
Unavailable or changed observations require a new explicitly selected registration.

`--memory-bytes` and `--scratch-bytes` are operator-admitted capacities. The probe
does not claim a kernel-enforced memory limit or verify free scratch space.
`--precision` and `--compilation` record declared execution semantics. They do not
rewrite project code or prove that a command obeyed them. The generic worker
accepts only `project_declared` for these fields; explicit policies need a
qualified workload implementation. CPU fallback policy is `forbid`. The MPS
launch environment disables PyTorch's automatic CPU fallback, but arbitrary
project code can still explicitly choose CPU operations. That is not proof of
accelerator use or scientific eligibility.

The registration labels dependency provenance as `installed_names_versions`
and lock qualification as `operator_authorized_unverified`. Names and versions
cannot detect every modified package file, editable checkout change, native
library replacement, or injected system dependency. An authorized environment
contract is not proof that its lock was reproduced. Stronger environment-image
and lock receipts must be separate, explicit evidence.

The public fingerprint includes the environment descriptor, interpreter binary,
admitted capacities, execution policy, and the frozen launch adapter's code digest.
It excludes local paths, worker IDs,
candidate source, seeds, and the list of authorized contracts. A different
candidate or seed can therefore remain comparable under the same registered
execution profile. The registered bootstrap checks interpreter identity before it runs a stage.
Unobserved runtime or driver versions remain `null`.

## Execute a registered project

Start with the [CPU bigram campaign YAML](workload-protocol.md#original-cpu-example).
Save a copy as `registered-bigram.yaml`. Keep the original task directory, metric,
candidates and limits. Replace the campaign ID and command with these two fields.

~~~yaml
campaign_id: registered-bigram
command: [python, -m, research_swarm.swarm.native, --registration, ../../.research/engines/bigram.json]
~~~

~~~sh
uv run --no-sync research swarm run registered-bigram.yaml --db .research/registered-bigram.sqlite
~~~

The worker starts the command in `task_dir`, so registration and output paths in
that command are relative to the project directory. The native runner reads
`research.toml` there by default. Use `--manifest FILE` for another manifest
inside the captured project and `--outputs DIR` to keep the stage files.

The runner checks the registered environment contract and each stage's resource
requirements. It freezes the launch adapter and probe code for the attempt. The
registered Python then verifies its environment under `-I` and runs the stage's
script, `-m module` or `-c code` with its declared arguments. Stage dependencies
must be installed in that interpreter. The workload can read the engine profile,
logical device, fingerprint and resource demands in
`WorkloadContext.parameters["engine"]`. An unregistered launch supports the
`python-cpu` profile with default stage resource requirements.

## Physical device identity

A registered accelerator stage requires one exclusive device and sufficient
registered capacity. For unified memory, stage admission counts host and
accelerator byte requests against the registered host memory. The runner checks
the worker's device allocation against the registered backend. For CUDA and
ROCm it also checks a supplied `CUDA_VISIBLE_DEVICES` ordinal. The stage sees
the selected device as logical `cuda:0`. MPS and MLX use their native device
conventions.

Earlier releases used signed assignments, worker enrollment, and a machine
registry. Their launch and recovery qualifications are preserved in the
[archived engine guide](https://github.com/vincentkoc/autoresearch-swarm/blob/b8b44174b925483c8b202163fef12b733517fa49/docs/engines.md#execute-a-registered-project).

## Qualification boundaries

Keep these claims separate: environment availability; numerical correctness;
workload smoke; evaluator parity; variance; performance; scientific comparison
eligibility. A registration records only the first. A mocked accelerator probe
qualifies parsing and routing, not physical accelerator execution.

ROCm uses the `torch.cuda` API, so the probe checks `torch.version.hip` separately
from CUDA build identity. [PyTorch HIP semantics](https://docs.pytorch.org/docs/2.15/notes/hip.html)

MPS requires both built support and runtime availability.
[PyTorch MPS backend](https://docs.pytorch.org/docs/2.15/notes/mps.html)

MLX Metal availability is checked explicitly; importing MLX is insufficient.
[MLX Metal API](https://ml-explore.github.io/mlx/build/html/python/metal.html)
