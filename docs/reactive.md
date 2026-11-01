# Training-time reactive decisions

A reactive session records bounded decisions inside one training attempt. It
does not create research trials, choose an incumbent, or authorize evaluation.
The immutable contract declares numeric controls, initial values, bounds,
training/development diagnostics, a policy fingerprint and decision limits.

## Local session API

`DecisionJournal.create(path, binding, contract)` creates a new SQLite session.
`open(path)` opens an existing session without changing its state. A journal
offers `inspect`, `get`, `pending`, `respond` and paginated `events`. A response
must identify its original request and control revision. Repeated identical
submissions return the recorded result; conflicting submissions fail.

The training process holds `journal.trainer()` as a context manager. That
exclusive native owner can `request`, `claim_application`, `record_application`,
`settle` and `close`. Only one request can be outstanding. Claiming application
persists the intent before the workload changes controls; the receipt must match
the request, proposed values, actual application step and control revision.
The journal does not mutate a model or verify workload readback itself.

Each transition appends an event and updates the current projection in one
transaction. Failed, expired and cancelled requests remain visible. A response
that arrives after settlement is retained as late and cannot reactivate the
request. Closing a session cancels pending decisions and prevents new requests.

## Recovery and trust

Readers never recover a training session. Another training owner must acquire
the native lock before recording interruption. An interrupted application
becomes uncertain; no completion or new control revision is inferred. Its
controls field retains only the last confirmed values. The session cannot
resume or repeat the application. A new training attempt needs a new session.

These APIs assume trusted local workloads, agents and filesystem access. The
diagnostic allowlist is a declaration, not proof that workload code has not
mislabelled a metric. It is not a Python sandbox or evaluator boundary. No cloud
adapter or hosted authorization is implied by the local journal API.

## Cooperative training hook

`ReactiveHook(journal, target)` owns a fresh session with the declared initial
controls. The workload provides synchronous `target.read()` and `target.apply()`
methods. They must be short operations on the declared controls. They must not
perform inference, block on network calls or replace optimizer implementations.

Call `await hook.boundary(observation)` between completed updates. The hook
yields once to cooperative tasks, polls for a response and returns without
waiting for a decision. A due observation creates a request with bounded recent
history. Delayed decisions expire at the declared step limit. An explicit
`step` argument records a later request/application boundary than the observation.

Only the boundary can apply a response. The hook checks current controls,
applies a changed target and reads back actual values before recording success.
Readback is exact in this numeric protocol: there is no implicit rounding,
tolerance or framework dtype conversion. Framework-specific control targets
require their own tests; this hook does not qualify MLX or PyTorch targets.
A keep response or a set response with identical values performs no mutation.
Exceptions, unexpected control drift and readback mismatches record uncertainty
and stop later interventions. They do not restore model or optimizer state.

Use the hook as a context manager or call `close()` explicitly. Closing cancels
pending requests without waiting for inference. The workload still owns and
enforces its training deadline; the hook does not extend that deadline or bound
the execution time of arbitrary workload code.

## Decision policies and runner

`DecisionRunner(journal, policy)` can run beside training or in a separate local
process that opens the same session. It never acquires training ownership.
`poll()` schedules available work and returns immediately; `await run()` serves
the session until it closes. The policy implements `digest` and an async
`decide(request, contract)` method. Its fingerprint must match the admitted
contract. `ThresholdPolicy` hashes its configuration; `CallbackPolicy` requires
the caller to declare the version fingerprint of its own callback.

The runner persists a call claim before invoking a policy, outside the database
transaction. Another runner cannot repeat a recorded call. There is at most one
in-flight local invocation per session. A monotonic deadline fences a late result and requests
task cancellation. If a callback resists cancellation, its claim stays in flight
and prevents replacement calls. Closing requests cancellation without waiting.
Call outcomes include generic failure codes rather than exception messages that
could contain credentials.

Callbacks must cooperate with the event loop. Arbitrary blocking Python code
cannot be forcibly stopped by this runner. Use a bounded HTTP provider or a
separate supervised process for untrusted or blocking implementations. Local
cancellation does not prove that a remote provider stopped processing or billing.
An interrupted call remains recorded and is never silently retried.

Manual agents can still use `respond` while a call is pending. Timeout or
cancellation cannot overwrite an accepted response or application. Later policy
responses are retained as late or conflicting evidence, not applied changes.
The contract caps decision count and serialized context/output size. Token caps
are for providers that support them; reported usage is telemetry, not billing
authority or proof of actual provider consumption.

## Optional HTTP LLM policy

`ChatCompletionsPolicy` implements the same policy protocol using the existing
HTTP client dependency. Configure the full endpoint and model explicitly. It
does not select a provider, discover credentials, fall back to another model or
retry failed requests.

```python
import os
from research_swarm.reactive.chat_completions import (
    ChatCompletionsConfig,
    ChatCompletionsPolicy,
)

config = ChatCompletionsConfig(
    endpoint="https://api.openai.com/v1/chat/completions",
    model="MODEL_ID",
)
policy = ChatCompletionsPolicy(config, api_key=os.environ["OPENAI_API_KEY"])
# Declare config.digest as the reactive contract's policy_digest before admission.
```

The fingerprint covers endpoint, model, instructions, response format, token
parameter and limits. The API key stays in memory and authorization headers;
never put credentials in the endpoint or prompt. Plain HTTP requires the
explicit `allow_insecure_http` option, for example for an operator-selected local
server. Redirects are rejected and environment proxy discovery is disabled.

The default uses strict `json_schema` output. An explicitly configured
`json_object` mode supports compatible servers; both receive the same strict
client validation. The prompt includes admitted control bounds and diagnostic
definitions. The entire serialized request body, including schema and framing,
must fit `max_prompt_bytes`; oversized input fails before network access.

The request sets one non-streamed completion, `store=false` and a finite token
cap. The default `max_completion_tokens` includes reasoning tokens. A server
that requires `max_tokens` must opt into that fingerprinted parameter; there is
no automatic fallback. See the [Chat Completions reference](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
and [JSON mode guidance](https://developers.openai.com/api/docs/guides/structured-outputs#json-mode).

Only one complete, non-refused text response without tool calls is accepted.
Wire bytes, content bytes and the resulting proposal are bounded. Duplicate JSON
keys, nonfinite values, undeclared controls, out-of-range values and application
claims are rejected. Request identity and revision come from the caller, never
from model output. Usage is labelled provider-reported. HTTP fixture tests prove
this wire contract; they do not qualify a live provider or demonstrate better
training results.

## Agent interfaces

`DecisionClient(session_path)` grants access to one existing local session. It
offers `inspect()`, `pending()`, `respond(proposal)` and `export(...)`. The Python
client, JSON CLI and dedicated stdio MCP server all use its restricted dispatch
and validation path. They cannot acquire training ownership, cancel the training
process, claim application or submit an application receipt.

```sh
research reactive --session session.sqlite inspect
research reactive --session session.sqlite pending
research reactive --session session.sqlite respond --file proposal.json
research reactive --session session.sqlite export --after -1 --limit 20 --max-bytes 65536
research reactive --session session.sqlite mcp
```

The MCP server exposes only `reactive_inspect`, `reactive_pending`,
`reactive_respond` and `reactive_export`. The selected path is fixed when the
operator starts it; tools cannot select another file. Proposals include the
original request ID, digest and control revision. Preserve those fields on
retries. A successful response acknowledges a proposal, not a control change.

Exports are bounded by both event count and serialized bytes. Continue from
`next_after` while `has_more` is true. If one event exceeds the requested byte
budget, increase `max_bytes` within its documented schema bound. These are local
tools for trusted workloads and agents; controller credentials, hosted adapter
parity and filesystem sandboxing are separate capabilities.
