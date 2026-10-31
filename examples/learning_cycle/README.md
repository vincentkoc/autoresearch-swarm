# Native learning cycle

Run one reward-driven categorical policy update through the public research API:

~~~sh
uv run python examples/learning_cycle/run.py --root /tmp/research-learning-cycle
~~~

The default run performs 82 CPU trials and 164 subprocess stages: eight paired
development episodes, one independently checked policy-training trial, sixteen
paired held-out policy comparisons (two research trials per arm), and one future
binding probe. It makes no LLM calls. This example runs sequentially; separate
worker qualification covers parallel execution.

`definition.json` freezes the seeds, two learning-rate actions, weak baseline
logits, reward scaling, training rate and gate before execution. The initial
categorical policy favors the slower action. The trainer makes one REINFORCE
update from controller-qualified development evidence. The controller reserves
the held-out panel before admitting training and assesses every original arm.
Failed or missing cases block assessment; the runner does not replace them.

The actual paired sign gate decides whether adoption is permitted. A passing
panel advances the policy binding; any other completed outcome retains the
baseline. The final probe freezes a new plan against the current binding and
commits a decision, then cancels that proposal. It does not execute another
candidate or claim another reward. Both outcomes are valid demonstrations of
the API; a passing gate establishes only this declared comparison on the tiny
synthetic workload, not general superiority of a research agent.

The state directory contains private credentials, original command envelopes,
immutable input hashes, artifacts and the completed `result.json`. Keep the
directory private. The completed result contains case measurements, digests,
the gate, exact held-out costs and total costs including development, training
and the probe. Settled execution milliseconds differ from project-reported
active training time. Wall elapsed time includes interruptions.

Reopening the same root with unchanged inputs returns the historical result
without another trial. Changed code or definitions require a different root;
never present a newly chosen run as the original experiment. Current policy
use still performs controller evidence checks. The CLI uses the shared local
machine resource registry; tests may explicitly supply a disposable registry.

Run the smaller native recovery check with:

~~~sh
RESEARCH_BENCHMARK_TESTS=1 uv run pytest tests/test_learning_cycle_example.py
~~~
