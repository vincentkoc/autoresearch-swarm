# Reactive CPU example

This ordinary project trains a two-parameter linear model for 48 gradient updates.
It uses the installed research package and standard Python arithmetic. No GPU,
dataset download or model-provider account is needed.

From a checkout with reactive support installed:

~~~sh
uv run research run examples/reactive_cpu --key reactive-cpu --max-trials 1
~~~

The protected manifest defines four stages:

| Stage | Result |
|---|---|
| `train` | Observes training MSE at step 8, continues updating while a callback waits until step 12, then applies its bounded learning-rate proposal at a later update boundary. |
| `baseline` | Keeps the initial learning rate for all 48 updates. This is labelled `fixed_initial_controls`, without a live policy invocation. |
| `replay` | Starts a distinct admitted attempt from the same seed and applies the recorded control schedule. |
| `evaluate` | Independently scores all three models on the protected evaluation inputs. Only this stage supplies the primary metric. |

Training emits `model`, `transcript` and `trajectory` artifacts. Replay emits
`replay_model` and `replay_trace`. The replay model and report include explicit
replay mode, source transcript digest and new run binding. The baseline score and
replay score are evaluator diagnostics; `mse` is the primary score. No stage
promotes a candidate.

The 1-second active-training limit is a cap, not a performance benchmark. This
fixture stops at 48 updates. It demonstrates execution and control equivalence;
one deterministic synthetic run does not establish scientific improvement.
Exact model equality in this fixture does not imply replay restores arbitrary
model, optimizer, data or random state.

Run the bounded registered-worker proof:

~~~sh
uv run pytest -q tests/reactive/test_cpu_example.py
~~~

`train.py` is the candidate mutation surface. Data, policy, evaluator and manifest
are protected. The only declared reactive control is `learning_rate`, bounded
between 0.001 and 0.3. Its initial value is 0.01. Only training MSE is exposed to
the decision policy. The workload remains trusted Python code, not a sandbox that
proves diagnostic labels are truthful.

## Use an external LLM or agent

The bundled callback makes the smoke run deterministic. A separate process can
serve the same session through `DecisionClient`, JSON CLI, stdio MCP or
`DecisionRunner`; it needs no training-owner lock.

For an HTTP policy, choose and fingerprint configuration **before** capturing a
project. Set `[reactive].policy_digest` to `config.digest`, update its policy name,
and select `--external` in the live training stage's `argv`. The worker receives
the sealed declaration. Supply the API key only to the separate runner:

```python
import asyncio
import os
from pathlib import Path

from research_swarm.reactive.chat_completions import (
    ChatCompletionsConfig,
    ChatCompletionsPolicy,
)
from research_swarm.reactive.journal import DecisionJournal
from research_swarm.reactive.runner import DecisionRunner

config = ChatCompletionsConfig(
    endpoint="https://api.openai.com/v1/chat/completions",
    model="MODEL_ID",
)
policy = ChatCompletionsPolicy(config, api_key=os.environ["OPENAI_API_KEY"])
journal = DecisionJournal.open(Path("SELECTED_ATTEMPT_OUTPUT/reactive.sqlite"))
runner = DecisionRunner(journal, policy)
asyncio.run(runner.run())
```

Select one live attempt path explicitly. A mismatch with the admitted policy
digest is rejected. Never put keys in source, the manifest or the journal. This
48-step CPU fixture usually finishes before a remote LLM could respond; use a
longer admitted training workload and suitable finite decision limits for live
provider experiments. Training does not wait for the caller or extend its budget.
This example's tests make no network calls. The HTTP adapter is qualified with
fixtures, not a live provider.

See [reactive controls](../../docs/reactive.md) for API contracts, replay limits
and the distinction from the outer research loop.
