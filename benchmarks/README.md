# Research benchmarks

Run these workloads from a source checkout. The program benchmark needs the
core dependencies. The two training workloads use the existing Torch extra.

~~~sh
uv sync --locked --group dev
uv run --no-sync python -m benchmarks program --budget 12 --seed 11
uv sync --locked --group dev --extra torch
uv run --no-sync python -m benchmarks mlp --device cpu --size small
uv run --no-sync python -m benchmarks nanogpt --device mps --size small
~~~

| Workload | Candidate | Search metric |
| --- | --- | --- |
| `program` | Four instructions that normalize a label | Fixed unit-test pass rate, maximize |
| `mlp` | Hidden width, activation and learning rate of a Torch MLP | Held-out quadrant classification accuracy, maximize |
| `nanogpt` | Three edited lines of a causal transformer's training program | Held-out next-token cross entropy, minimize |

Each model trains from scratch. Synthetic data generation is deterministic and
requires no external dataset. The device option selects CPU, MPS or CUDA. On a
provisioned GPU, use `--size full --device cuda` for the larger training budget.

The default output contains random and greedy runs. Random samples the search
space uniformly. Greedy mutates one field of the current incumbent and prefers
untried neighbors. Both start from the same candidate. Each proposal consumes
one trial, including an invalid response or timeout. The JSON includes each
candidate, raw score, running best, failure and elapsed seconds.

For a local LLM run, start Ollama with an installed model and pass its name.

~~~sh
uv run --no-sync python -m benchmarks program --strategy llm --model LOCAL_MODEL
uv run --no-sync python -m benchmarks nanogpt --strategy llm --model LOCAL_MODEL --device cpu
~~~

The program evaluator runs a candidate's instructions on fixed test cases.
The training-code evaluator applies the model's chosen source lines to the
optimizer and schedule. It trains a causal attention block on synthetic token
sequences. The edit vocabulary gives every search policy the same space.
Ollama receives search scores and previous edits. The report includes its
measured input and output tokens.

Compare policies with 12 trials per arm on seeds 11, 23 and 47. Fix the
profile and device across arms. Report all paired differences, ties and
failures with the running-best curves. This comparison controls the trial
budget. Elapsed time and LLM token usage are separate measurements.

Use `benchmarks.suite.run_search(task, policy, budget=12, seed=11)` to supply
another proposer. It receives the task, trial history and a seeded RNG, and
returns one candidate dictionary. For a campaign worker, use
`python -m benchmarks.trial TASK`. It reads `SWARM_TRIAL`, evaluates the
candidate's `params`, and writes one numeric metric to `SWARM_METRICS`.
