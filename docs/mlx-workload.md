# Native MLX bigram project

The original bigram example has two ordinary project manifests. The CPU manifest
uses standard-library SGD. `research_mlx.toml` selects an MLX Metal training payload
while reusing the exact same synthetic dataset, checkpoint format, and independent
standard-library scorer. Neither project needs an engine-specific model interface.

MLX performs automatic differentiation of the same single-transition loss as the
CPU implementation. Training explicitly selects the GPU stream, materializes each
completed update, and synchronizes before counting its elapsed time. These are
the relevant [MLX differentiation](https://ml-explore.github.io/mlx/build/html/python/_autosummary/mlx.core.value_and_grad.html),
[evaluation](https://ml-explore.github.io/mlx/build/html/python/_autosummary/mlx.core.eval.html),
and [stream synchronization](https://ml-explore.github.io/mlx/build/html/python/_autosummary/mlx.core.synchronize.html)
APIs. A successful import alone is not the execution proof.

## Contract

| Property | MLX example |
|---|---|
| Mutation surface | `train_mlx.py` |
| Framework/device | MLX 0.32.4 / Metal, explicit GPU stream, no CPU fallback |
| Initialization and data order | Python `Random(seed)`, same construction and sampling order as CPU |
| Numerical precision | Float32; CPU reference uses Python double-precision values |
| Optimizer | SGD, learning rate 0.35 |
| Training budget | 1,000 ms active training; final materialized update may overshoot |
| Startup policy | One disposable update, no RNG consumption or retained warmup weights |
| Compilation | Eager MLX operations; startup shader work excluded by the declared warmup |
| Evaluation | Separate process, protected source, standard-library CPU scorer |
| Resource admission | One exclusive Metal device and 512 MiB admitted unified memory per stage |
| Trust | Authenticated execution in `trusted_local`; no hostile-code isolation claim |

The scorer uses CPU arithmetic inside the same registered MLX interpreter. Its
stage reserves the selected device because this worker installation binds one
complete accelerator profile. A CPU scoring worker could be scheduled separately
under a different declared project contract.

The MLX and CPU manifests have different comparison classes, environment contracts,
precision, and time budgets. Their scores therefore do not share an automatic
leaderboard. The fixed-step differential test checks update semantics within
float32 tolerance; it does not claim identical wall-clock training trajectories.

## Install and run

The supplied hash lock selects CPython 3.12 / macOS 26+ / arm64 wheels for
[`mlx`](https://pypi.org/project/mlx/0.32.4/) and
[`mlx-metal`](https://pypi.org/project/mlx-metal/0.32.4/). Other targets need a
separately qualified lock. Keep the controller environment separate:

```sh
uv venv --python 3.12 .research/mlx/venv
uv pip install --python .research/mlx/venv/bin/python --require-hashes \
  -r examples/cpu_bigram/mlx-macos-cp312.lock
```

This SDK example resolves the environment contract without creating a preliminary
project, registers the selected interpreter, and starts one bounded campaign:

```python
import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from research_swarm import ResearchClient
from research_swarm.engines.registration import register_environment
from research_swarm.orchestration.bootstrap import RunOptions
from research_swarm.orchestration.run import run_project
from research_swarm.projects.capture import capture_project
from research_swarm.projects.manifest import resolve_project

source = Path("examples/cpu_bigram")
root = Path(".research/mlx").absolute()
with TemporaryDirectory() as temporary:
    bundle = Path(temporary) / "source.zip"
    capture_project(source, bundle, manifest_path="research_mlx.toml")
    resolved = resolve_project(bundle, manifest_path="research_mlx.toml")

registration = register_environment(
    root / "engine.json",
    profile="mlx-metal",
    executable=root / "venv/bin/python",
    environment_contracts=(resolved.environment_contract_digest,),
    memory_bytes=1024 * 1024 * 1024,
    probe_directory=root / "probes",
)
with ResearchClient.local(root / "state") as client:
    result = asyncio.run(
        run_project(
            client,
            source,
            key="mlx-baseline",
            options=RunOptions(manifest_path="research_mlx.toml", seed=7, max_trials=1),
            registration=registration,
        )
    )
    print(result.model_dump_json(indent=2))
```

The default machine registry remains shared across installations. A conflicting
existing admission is a refusal, not permission to create another independent
device pool. Registered capacity is scheduling admission, not an OS memory limit
or a claim that unrelated native workloads are absent.

The result remains `awaiting_completion`: an agent or operator selects its baseline
and completes the campaign through the same API used by every other project.
After setup, resume through the CLI with the same state, key, options and registration:

```sh
uv run research --state-dir .research/mlx/state run examples/cpu_bigram \
  --manifest research_mlx.toml --key mlx-baseline --seed 7 --max-trials 1 \
  --registration .research/mlx/engine.json
```

## Local qualification

The native test is opt-in and fails when explicitly enabled without its selected
interpreter. Default CPU CI does not import or install MLX:

```sh
RESEARCH_MLX_TESTS=1 RESEARCH_MLX_PYTHON="$PWD/.research/mlx/venv/bin/python" \
  uv run pytest -q -s tests/test_mlx_workload.py
```

Local verification on 2026-10-30 used an Apple M5 Max, macOS 26.4, Python 3.12.13,
and a fresh hash-locked MLX 0.32.4 environment. The real registered worker completed
training and independent evaluation for seeds 7 and 8. Both produced finite
authenticated receipts, closed native attempts, and settled reservations. The
32-update GPU/CPU differential test passed a maximum absolute error threshold of
`1e-6`. Observed timing and scores are diagnostic; reruns need not repeat step counts.

This establishes the trusted-local MLX execution path for this small workload.
It does not establish upstream CUDA parity, multi-Mac scaling, relative backend
speed, statistically confirmed improvement, or immutable installed dependency
contents. Installation verified the selected wheel hashes; runtime registration
still reports its explicit `installed_names_versions` provenance boundary.
