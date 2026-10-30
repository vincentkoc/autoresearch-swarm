# One-step research-policy training

This ordinary CPU project updates a categorical policy from a complete frozen
development batch. It trains the policy's logits, then checks that update in a
separate protected evaluation stage. It does not modify the underlying research
model or automatically adopt the learned policy.

This is the numerical training layer. The current example accepts structured
input artifacts; parsing those artifacts does not establish that their claimed
research evidence is authentic. [Controller-side decision precommitment and
episode/split ownership](../../docs/agent-control.md#freeze-learning-episodes-before-proposal-responses)
are available through the regular API. The controller also provides
[complete dataset qualification](../../docs/agent-control.md#qualify-a-development-dataset-from-recorded-experiments).
Binding that qualification to training admission and evaluating the learned policy
on held-out research remain separate requirements before an improvement claim.

## Contract

The two campaign inputs are:

- `behavior`: a `domain.learning.CategoricalPolicy` JSON artifact. It fixes
  2–32 ordered action IDs, their expected source digests, and finite logits.
  The maximum logit spread is 16, which keeps every action well above the
  53-bit sampler's resolution throughout this profile's supported dimensions.
- `trajectories`: a `domain.learning.TrajectoryDataset` JSON artifact. It retains
  the complete ordered development episode list, frozen behavior policy, raw
  comparable measurements, evidence references, exact consumed costs and rewards.

Use the same interpreter environment that contains this package's learning module.
The environment file declares the development package version; deployment must
also record the installed build and worker fingerprint. A development version
string alone does not pin installed bytes.

Upload both files through the normal artifact API, then use the ordinary project
bootstrap with `RunOptions(primary_metric="update_error", input_artifacts=...)`.
`orchestration.bootstrap.prepare_run` creates the regular campaign/trial/stage
graph. Registered workers claim and execute the train and evaluation stages;
an independently authorized evaluator completes the evaluation receipt.
No learning-specific job scheduler or accelerator framework is required.

The training stage publishes `policy` and `update` JSON artifacts. The protected
checker reads those artifacts plus both original inputs. It independently
recomputes each reward and parameter update, verifies provenance, and reports
`update_error`, the maximum absolute parameter/report residual. A changed
checkpoint fails evaluation instead of receiving an excellent zero score.
This metric verifies execution of the algorithm; it does not measure research
quality or held-out policy improvement.

## Algorithm

For one-step categorical actions, this implementation specializes the
[REINFORCE family introduced by Williams](https://link.springer.com/article/10.1007/BF00992696)
to a single batch gradient-ascent update:

```text
p = softmax(theta)
gradient = mean(reward_i * (one_hot(action_i) - p))
theta_next = theta + learning_rate * gradient
```

The fixed profile is `one-step-reinforce-v1`. It uses Python binary64 arithmetic,
one epoch, no reward normalization, no importance weighting, and no learned
baseline. `training.json` protects the learning rate and validation tolerance.
The output records the behavior-policy, dataset and reward-spec digests, gradient,
sample count, mean reward, and parent policy digest.

The versioned reward rule uses the declared comparable-metric difference, direction,
positive scale, explicit execution-millisecond penalty and symmetric clipping.
It never accepts an executable reward expression. Invalid, missing, censored or
UNKNOWN outcomes remain in the data and block the entire batch. They are not
silently dropped or converted to successful scores.

Decision traces contain the context, behavior policy, probabilities, chosen action,
source and reproducible SHA-256-derived draw. The recorded probabilities must sum
to one within `1e-12`; each must match the recomputed distribution within relative
`1e-12` or absolute `1e-16`. This accommodates small platform math differences.
The action is replayed against the recorded distribution, and that same
distribution enters the gradient. Context, policy, draw and source identities
remain exact. An update outside the admitted logit range fails explicitly.

The adapter rejects mixed policy
versions, changed action/source contracts, altered rewards, held-out rows, missing
episodes and a second update that tries to treat the original batch as on-policy
data for the learned checkpoint. Calling the same pure update twice returns the
same result; job execution and publication use the normal idempotent platform API.
The dataset also rejects reused trial IDs and candidate evaluation IDs. Record
evidence carries its record revision and digest; budget settlement evidence uses
the settlement's immutable request and response digests instead of inventing a
record revision for a separate accounting table.

## Qualification

Tests use an analytic two-action task and an independent finite-difference check.
They also execute both project stages through real native CPU jobs and a distinct
evaluator, retrieve the learned policy from the artifact store, and reject a
trainer that changes its checkpoint and report consistently but incorrectly.
The trajectory evidence in those tests is synthetic. They establish a real
reward-driven numerical update and protected checker, not a trained policy's
improvement on held-out research or recursive self-improvement.
