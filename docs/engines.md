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

These are environment probes. Accelerator worker execution and workload
qualification are separate implementation steps. Existing CPU workers keep
their original enrollment and signed-record interpretation; registering a new
environment does not replace an active worker.

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

Use the `environment_contract_digest` from the resolved project revision to
authorize that contract for the selected environment. Replace the variable below
with that complete `sha256:` digest:

~~~sh
research engine register python-cpu \
  --python /opt/research/venv/bin/python \
  --file /opt/research/private/environments/cpu.json \
  --environment-contract "$ENVIRONMENT_CONTRACT" \
  --cpu-slots 2

research engine verify --file /opt/research/private/environments/cpu.json
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
rewrite project code or prove that a command obeyed them. CPU fallback policy is
`forbid`; launch enforcement and scientific eligibility need their own checks.

The registration labels dependency provenance as `installed_names_versions`
and lock qualification as `operator_authorized_unverified`. Names and versions
cannot detect every modified package file, editable checkout change, native
library replacement, or injected system dependency. An authorized environment
contract is not proof that its lock was reproduced. Stronger environment-image
and lock receipts must be separate, explicit evidence.

The public fingerprint includes the environment descriptor, interpreter binary,
admitted capacities, and execution policy. It excludes local paths, worker IDs,
candidate source, seeds, and the list of authorized contracts. A different
candidate or seed can therefore remain comparable under the same registered
execution profile. The private interpreter identity is checked on local resume.
Unobserved runtime or driver versions remain `null`.

## Physical device identity

The selected device ordinal is not a physical resource identity. An available
device without a stable reported identity can be described and registered, but
`device_pool` refuses to admit it. There is no automatic model-name fallback or
operator override that could accidentally multiply one exclusive device.

MPS and MLX Metal on Apple Silicon share the machine-local identity
`apple-integrated-0`. Their pool key excludes framework, interpreter, worker ID,
driver version, and admitted capacity. Unified memory uses the admitted host
memory capacity; it is not a second independent GPU memory pool. This does not
yet reserve a device: native worker integration must use the key atomically.

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
