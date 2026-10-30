# Autoresearch Swarm design contract

This document defines the target behavior and acceptance criteria. See [implementation.md](implementation.md) for current implementation and verification status.

## Contents

- [1. Decisions to carry into implementation](#1-decisions-to-carry-into-implementation)
- [2. Goals, boundaries, and product shape](#2-goals-boundaries-and-product-shape)
- [3. Relationship to original autoresearch](#3-relationship-to-original-autoresearch)
- [4. Design motivations](#4-design-motivations)
- [5. Architecture](#5-architecture)
- [6. Domain model and vocabulary](#6-domain-model-and-vocabulary)
- [7. Identity, hashing, and comparison](#7-identity-hashing-and-comparison)
- [8. Project contract](#8-project-contract)
- [9. Execution schemas](#9-execution-schemas)
- [10. Campaign, job, and attempt lifecycle](#10-campaign-job-and-attempt-lifecycle)
- [11. State, transactions, and event delivery](#11-state-transactions-and-event-delivery)
- [12. Artifact store and reproducibility](#12-artifact-store-and-reproducibility)
- [13. Workers and resource scheduling](#13-workers-and-resource-scheduling)
- [14. Execution engines and platform support](#14-execution-engines-and-platform-support)
- [15. API, SDK, CLI, and MCP](#15-api-sdk-cli-and-mcp)
- [16. Local, Temporal, and Cloudflare adapters](#16-local-temporal-and-cloudflare-adapters)
- [17. Search, swarm behavior, and research memory](#17-search-swarm-behavior-and-research-memory)
- [18. Evaluation, statistical rigor, and promotion](#18-evaluation-statistical-rigor-and-promotion)
- [19. Budgets and cost accounting](#19-budgets-and-cost-accounting)
- [20. Recursive optimization, RSI, and RL](#20-recursive-optimization-rsi-and-rl)
- [21. Isolation, permissions, and process safety](#21-isolation-permissions-and-process-safety)
- [22. Reporting, observability, and optional UI](#22-reporting-observability-and-optional-ui)
- [23. Existing-project adoption](#23-existing-project-adoption)
- [24. Python, Rust, and implementation technology](#24-python-rust-and-implementation-technology)
- [25. Extension SDK and repository organization](#25-extension-sdk-and-repository-organization)
- [26. Testing and qualification](#26-testing-and-qualification)
- [27. Performance and scale acceptance](#27-performance-and-scale-acceptance)
- [28. Operational behavior and deployment](#28-operational-behavior-and-deployment)
- [29. Delivery roadmap](#29-delivery-roadmap)
- [30. Implementation work packages](#30-implementation-work-packages)
- [31. Assessment of the supplied deep research](#31-assessment-of-the-supplied-deep-research)
- [32. Requirement traceability](#32-requirement-traceability)
- [33. Open decisions and deferred work](#33-open-decisions-and-deferred-work)
- [34. Source and evidence register](#34-source-and-evidence-register)
- [35. Definition of specification completion](#35-definition-of-specification-completion)

## 1. Decisions to carry into implementation

| ID | Decision | Status |
|---|---|---|
| D01 | Start with Python for the controller, SDK, search policies, and native worker. | Recommended implementation default |
| D02 | Make wire protocols, schemas, hashes, and persisted records language-neutral. | Required |
| D03 | Default to one local service, asyncio, SQLite, local artifacts, and an automatically registered local worker. | Required |
| D04 | Use uniform domain primitives across local, Temporal, and Cloudflare deployments. | Required |
| D05 | Implement orchestration, storage, artifact, and worker adapters behind those primitives. | Required |
| D06 | Ship complete agent control through an API and bundled MCP tools in the first useful release. | Required |
| D07 | Keep the UI optional. Closing a browser or chat must not complete a campaign. | Required |
| D08 | Treat an existing research project as an ordinary research repository and reference adoption case. | Required |
| D09 | Treat CUDA, ROCm, MPS, and MLX as parts of execution profiles, selected against actual worker capabilities. | Required |
| D10 | Support multiple proposal agents and multiple independent experiment workers. | Required |
| D11 | Preserve the original one-agent, one-worker, fixed-budget greedy mode. | Required |
| D12 | Borrow useful optimizer vocabulary and SDK patterns from prior optimizer SDK work. Add no Opik integration or dependency. | Required |
| D13 | Make decision models optional implementations of the decision/search interface. | Required |
| D14 | Support bounded recursive optimization through nested campaigns and policy artifacts. | Required design contract |
| D15 | Add actual RL through explicit rollout, training, checkpoint, and evaluation stages. Do not equate logs with RL. | Later implementation |
| D16 | Prefer research parallelism before distributed training. | Default policy |
| D17 | Preserve raw measurements, failures, lineage, and comparison boundaries. | Required |
| D18 | Adopt Temporal before Cloudflare as the first additional durable orchestration implementation. | Recommended sequence |
| D19 | Keep Ray optional. It is not the default runtime or a mandatory dependency. | Required correction to the original report |
| D20 | Consider Rust for native workers or measured bottlenecks after the Python system establishes a baseline. | Deferred |
| D21 | Keep a population/archive of useful candidates alongside a canonical incumbent. | Required for swarm search |
| D22 | Separate scientific completion, resource reconciliation, and artifact retention. | Required |

The defining promise is:

> Any authorized agent can propose research. Any compatible worker can execute it. Humans and agents inspect the same evidence. Promotion follows the declared policy.

“Compatible” does not mean every implementation runs on every accelerator. “Validated” means that a named validation contract passed. It does not imply universal scientific truth.

## 2. Goals, boundaries, and product shape

### 2.1 Goals

1. Start a useful local campaign without provisioning a cloud account or a distributed framework.
2. Allow an existing coding agent to control research through structured operations.
3. Register an available machine as a worker without moving the research controller to that machine.
4. Execute independent candidates concurrently without shared-checkout conflicts.
5. Keep experiments interpretable as code or configuration evaluated under a fixed contract.
6. Resume orchestration after process interruption without losing pending work, budget accounting, or evidence.
7. Make infrastructure replaceable without changing research terminology or project code.
8. Support training, data preparation, evaluation, prompt optimization, and other measurable research workloads.
9. Preserve enough structured history for reflective search and later policy training.
10. Measure confirmed progress per unit of time and cost, not only device utilization.
11. Keep a small core that researchers can understand and modify.
12. Make the result useful as a notable continuation of Karpathy's autoresearch idea.

### 2.2 Non-goals for the first release

- A universal tensor abstraction that makes PyTorch and MLX interchangeable.
- A new distributed training framework.
- A requirement to run Kubernetes, Ray, Temporal, or Cloudflare.
- A replacement for every dataset preparation system.
- A built-in application or plugin dedicated to one research project.
- Opik support, exporters, tracing integration, or optimizer inheritance.
- An autonomous model-training service for every hosted LLM provider.
- An assumption that an inference API exposes trainable model weights.
- A large workflow language before the basic campaign works.
- A browser dashboard as a prerequisite for control.
- Automatic conversion of historical exploratory scores into confirmed results.
- Automatic registration, consumption, or interruption of operator-managed machines.
- A claim of recursive self-improvement before independent policy evaluation demonstrates it.

### 2.3 Initial users and operating modes

| Mode | User intent | Required behavior |
|---|---|---|
| Single researcher | Run the original experiment loop on one device. | One command, one local worker, durable results. |
| Agent-operated research | Let a coding agent propose, inspect, and control experiments. | Complete API/MCP access with bounded context. |
| Compute swarm | Use several available machines. | Outbound worker registration, capability matching, shared scheduling. |
| Research swarm | Explore several hypotheses or branches. | Concurrent proposals, lineage, population management, explicit selection. |
| Hosted control | Keep research running while a laptop disconnects. | Hosted state and orchestration with native compute workers. |
| Meta-research | Improve the policy that chooses experiments. | Parent/child campaigns, policy versions, comparable policy evaluations. |
| Policy learning | Train a policy from research outcomes. | Algorithm-specific trajectories and an actual training stage. |

A compute swarm and a research swarm are separate features. One agent can use many workers. Many agents can share one worker.

## 3. Relationship to original autoresearch

The upstream repository defines the reference experiment contract. An external coding agent edits a narrow training surface and runs a fixed training budget. It evaluates a metric, then keeps or reverts the candidate. The inspected design uses five minutes of training and minimizes validation bits-per-byte. Its central files include `train.py`, `prepare.py`, and `program.md`. [Upstream repository](https://github.com/karpathy/autoresearch)

| Concern | Original method | Proposed system |
|---|---|---|
| Research payload | Primarily one editable training file. | An ordinary repository with a declared mutation surface. |
| Agent instructions | Markdown playbook. | Markdown remains supported, with a typed control API. |
| Execution | Agent launches a process. | A worker executes a sealed experiment specification. |
| Search | Sequential keep/revert. | Greedy baseline plus asynchronous multi-branch policies. |
| Concurrency | One experiment at a time. | Independent jobs on reserved workers. |
| State | Git, logs, small result ledger. | Durable records, events, artifact references, and lineage. |
| Evaluation | Workload-specific validation metric. | The same concept, with protected evaluation and explicit comparison rules. |
| Promotion | Agent follows the playbook. | An atomic policy decision references eligible evidence. |
| Hardware | A particular supported machine. | Capability-qualified execution profiles. |
| Interruption | Agent reconstructs its context. | Controller and worker reconcile persisted state. |
| Reporting | Terminal output, ledger, notebook. | JSON and human reports from the same records. |
| Meta-optimization | An agent may revise its playbook informally. | Versioned policies and bounded child campaigns. |
| Interface | Repository and shell. | API, SDK, CLI, MCP, optional UI. |

The system must retain an `upstream-compatible` policy preset. That preset preserves the workload's training budget and selection behavior. It must label a single-run winner as observed, not statistically confirmed.

A separate `confirmed-greedy` preset adds replication and promotion gates. Results from these presets have different decision semantics. The report must show which preset produced a decision.

The fork should retain upstream attribution and comply with the licenses of copied code. Before copying any fork implementation, review its license and pin the exact revision. Naming and repository ownership remain product decisions.

## 4. Design motivations

The contracts address failure modes that become costly when a repository-operated research loop grows.

| Failure class | Required behavior |
|---|---|
| Producer and consumer schema drift | Shared versioned schemas and executable fixtures. |
| Unsupported worker capabilities | Reject incompatible work before execution. |
| Chat or client disconnection | Persist campaign lifecycle independently of the client. |
| Repeated narrative coordination | Provide structured tasks and bounded evidence views. |
| Diagnostic caches confused with reusable inputs | Declare artifact scope, lineage, and compatibility. |
| Cold and warm execution differ | Record preparation, startup, compilation, training, and evaluation separately. |
| A lower measurement fails the practical gate | Keep metrics separate from promotion decisions. |
| Infrastructure work delays unrelated hypotheses | Schedule explicit dependencies and already qualified resources. |

These are design motivations, not public benchmark claims. Scientific and operational qualification require their own reproducible evidence.

Execution qualification, local research confirmation, protected evaluation, and official external results remain distinct evidence classes.

## 5. Architecture

### 5.1 Logical structure

~~~mermaid
flowchart TB
    Human[Human, CLI, or optional UI] --> API[Research API and command handlers]
    External[External coding agent via MCP or SDK] --> API
    Policy[Optional built-in search or decision policy] --> API
    API --> Kernel[Research kernel]
    Kernel --> State[Authoritative state and event store]
    Kernel --> Artifacts[Artifact store]
    Kernel --> Driver[Orchestration adapter]
    Driver --> Dispatch[Dispatch and reconciliation]
    Dispatch --> Local[Local worker]
    Dispatch --> Remote[Registered remote workers]
    Local --> Workload[Project commands and evaluator]
    Remote --> Workload
    Workload --> Results[Measurements and artifact references]
    Results --> Kernel
    Kernel --> Reports[Human and machine reports]
~~~

The API, CLI, SDK, and MCP tools call the same command handlers. A dashboard does not implement a second scheduler or a separate promotion path.

The worker executes commands. It does not decide which candidate becomes the incumbent.

The evaluator produces measurements and validity findings. The promotion policy decides how those measurements affect the campaign.

### 5.2 Small core

The core owns:

- Entity identities and schema versions.
- Campaign and job state transitions.
- Budget and resource reservations.
- Candidate lineage and comparison contracts.
- Idempotent command handling.
- Accepted observations and promotion transactions.
- Artifact references and provenance.
- Bounded context and report generation.

The core does not own every tensor operation, cloud SDK, agent provider, or project-specific metric.

### 5.3 Extension boundaries

| Interface | Responsibility | First implementation |
|---|---|---|
| `ProjectContract` | Declare executable stages and research rules. | Repository manifest and command runner. |
| `StateStore` | Implement atomic domain operations and queries. | SQLite. |
| `ArtifactStore` | Stage, verify, publish, fetch, and retain artifacts. | Filesystem. |
| `OrchestrationDriver` | Deliver durable dispatch, waits, timers, and wakeups. | Local asyncio with persisted intents. |
| `WorkerProtocol` | Registration, claim, heartbeat, completion, reconciliation. | Local and HTTP transports. |
| `ExecutionEngine` | Probe capabilities and prepare a compatible execution environment. | Native Python/subprocess profiles. |
| `SearchPolicy` | Propose work and consume accepted observations. | Greedy and asynchronous portfolio. |
| `DecisionPolicy` | Recommend a bounded scheduling or research decision. | Deterministic rules, optional models. |
| `PromotionPolicy` | Decide eligibility, replication, and promotion. | Workload-declared rules. |
| `LearningAdapter` | Consume trajectories and produce policy artifacts. | Deferred. |

A project is not an infrastructure plugin. It supplies commands and a contract. An adapter supplies a reusable implementation of an engine interface.

### 5.4 Deployment shapes

**Local:** one controller process, SQLite, filesystem artifacts, and a local worker. The API binds to loopback by default.

**Local controller with remote compute:** the controller remains local. Remote workers connect through an authenticated reachable endpoint. Disconnection affects control availability, so the worker's assigned offline policy matters.

**Hosted control:** an adapter places the control service, durable state, and artifacts on hosted infrastructure. Native workers still execute experiments.

**Managed multi-node training:** a later resource-group facility reserves several workers for one job. The project command owns DDP, MLX distributed execution, or another training mechanism.

Do not equate a hosted workflow runtime with a native GPU worker.

## 6. Domain model and vocabulary

### 6.1 Canonical entities

| Entity | Meaning |
|---|---|
| `Project` | A research repository and its contract. |
| `Campaign` | One objective, policy, budget, comparison scope, and lifecycle. |
| `Round` | Optional proposal or reporting group. It does not impose a global barrier. |
| `Hypothesis` | A proposed mechanism or question, with expected evidence. |
| `Candidate` | An immutable code/configuration/prompt artifact with parent candidates. |
| `ExperimentSpec` | The sealed executable and scientific contract for one planned trial. |
| `Trial` | A planned candidate evaluation at a declared seed, fidelity, and environment contract. |
| `Job` | A schedulable stage, such as preparation, training, evaluation, or policy training. |
| `Attempt` | One actual execution attempt for a job. |
| `Evaluation` | One artifact scored against a named evaluator and data panel. |
| `Observation` | An accepted outcome delivered to a search policy. |
| `PromotionDecision` | A policy decision that references exact evaluations and a comparator. |
| `Population` | A collection of useful candidate branches and their selection metadata. |
| `Worker` | An enrolled execution process with capabilities and a local journal. |
| `ResourceReservation` | Authority to use declared capacity for an attempt or resource group. |
| `Artifact` | Immutable content plus metadata, digest, access scope, and retention. |
| `PolicyArtifact` | A versioned prompt, program, configuration, or model checkpoint for research decisions. |
| `Trajectory` | A sequence of observations, actions, outcomes, and policy provenance. |
| `RewardSpec` | A versioned rule for assigning reward to a trajectory or decision. |

The canonical top-level term is `Campaign`. The Python SDK may expose an optimization convenience object, but it must not create a second top-level identity.

### 6.2 Trial, attempt, and evaluation rules

A deliberate new seed creates a new Trial. A changed fidelity or comparison contract also creates a new Trial.

A retry creates a new Attempt under the relevant Job. The record states whether execution resumed a checkpoint or restarted initialization.

A fresh stochastic execution cannot be hidden as an invisible retry. Its seed, consumed compute, outputs, and selection treatment remain visible.

Reevaluating an existing checkpoint creates a new Evaluation. It does not create a new model candidate.

A patch that changes scientific payload creates a new Candidate. Operational annotations do not change candidate identity.

A crash has a failure status and a missing primary metric. It never receives a synthetic zero that can win a lower-is-better ranking.

### 6.3 Concepts borrowed from Opik Optimizer

Borrow:

- An optimization context that keeps objective, budget, baseline, and counters aligned.
- Separation between algorithm logic and lifecycle management.
- Candidate-first APIs.
- Typed results and histories.
- Optional rounds.
- Explicit hooks for proposal, evaluation, and reporting.
- A convenient result object that links back to underlying evidence.

Do not borrow:

- A mandatory Opik runtime or tracking backend.
- Prompt-specific assumptions for every workload.
- Threaded dataset evaluation as the worker scheduler.
- Higher-is-better assumptions for raw metrics.
- A process-local context as durable campaign state.

The audited Opik vocabulary used `OptimizationTrial` for one scoring event. This specification uses `Evaluation` for that event and `Trial` for planned experimental execution. Document this distinction in developer guidance. There is no binary or API compatibility promise.

### 6.4 State and claim dimensions

Keep these dimensions separate:

1. Execution state: did the command run and terminate?
2. Artifact state: are required outputs verified and available?
3. Evaluation validity: did the declared evaluation contract pass?
4. Comparison eligibility: may these results be compared?
5. Promotion state: did the selection policy accept the candidate?
6. Evidence class: qualification, exploratory, confirmed, protected, or official.
7. Resource disposition: released, retained, or uncertain.

A single boolean `passed` is insufficient.

## 7. Identity, hashing, and comparison

### 7.1 Required identifiers

Every persisted object has a globally unique opaque ID. IDs are stable across adapters and retries of the same command.

Use UUIDs represented as strings for v1. The wire contract does not depend on a database's integer sequence. Events also carry a monotonically increasing sequence within their authoritative scope.

Each mutating request has an idempotency key and a canonical request digest. Reusing a key with different content returns a conflict.

Every mutable aggregate has a revision number. Commands that depend on its current state supply an expected revision.

### 7.2 Canonical serialization

The schema package defines:

- UTF-8 encoding.
- Explicit schema and hash-format versions.
- Canonical JSON serialization for hashed structured content.
- Rejection of duplicate JSON keys.
- Explicit nullability.
- No NaN or infinity in persisted numeric measurements.
- Integer byte counts and explicitly named time units.
- UTC timestamps with fixed documented precision.
- Decimal-string amounts or integer minor units for financial budgets.
- Ordered collections preserve order. Unordered string collections sort by canonical JSON element bytes.
- Stable treatment of optional fields and defaults.

Version 1 uses RFC 8785 canonical JSON with golden fixtures. Reject duplicate keys, non-finite numbers, invalid Unicode, and integers outside the agreed safe numeric range. Numeric integers must fall within negative and positive 9,007,199,254,740,991. Larger counters use schema-declared decimal strings. Financial precision must not silently round through JavaScript numbers. [Canonical JSON specification](https://www.rfc-editor.org/rfc/rfc8785)

Preserve Unicode string contents without implicit normalization. RFC 8785 defines numeric serialization, including negative zero. A project that needs distinct textual numeric representations must use an explicitly typed string field.

Content hashes use a named algorithm, initially SHA-256, over exact bytes or the specified canonical representation. Store the algorithm and format version alongside the digest.

Structured digests use a domain-separated preimage with `domain`, `hash_format_version`, and `payload`. Canonicalize that preimage, encode it as UTF-8, and hash those bytes.

The payload excludes only the record's own digest field and its signature envelope. For ExperimentSpec, exclude `spec_digest`. For RunResult, exclude `result_digest` but retain its referenced `spec_digest`. A schema must name these exclusions explicitly. It cannot strip every field containing the word “digest.”

Omitted optional fields remain omitted. Explicit nulls remain null. Apply declared defaults before sealing the payload and never insert new defaults while verifying an old record. Signatures cover the resulting named digest and their declared authority context.

### 7.3 Three identities

**Protocol identity** describes the scientific question and scoring rules:

- Dataset and split manifests.
- Target semantics and units.
- Tokenizer or feature-transform identity.
- Evaluator identity.
- Metric definition and aggregation.
- Budget semantics and fidelity.
- Exposure and promotion policies.

**Execution fingerprint** describes how a particular attempt ran:

- Candidate source and environment.
- Framework and runtime.
- Device, memory, OS, driver, and relevant firmware.
- Precision, compiler settings, and kernel versions.
- Seeds, initial weights, cache mode, and data order.
- Worker qualification receipt.

**Comparison class** states which differences the project allows for a particular comparison.

This separation avoids both silent cross-hardware comparisons and unnecessary fragmentation into unrelated leaderboards.

### 7.4 Comparison rules

Default fixed-time comparisons require the same protocol and qualified hardware/resource class.

A project may declare a broader comparison policy, such as evaluator parity across two backends. That policy must exist before selecting a winner.

Changing a driver version does not automatically invalidate every historical observation. It changes the execution fingerprint and may require qualification under the comparison policy.

Cross-backend exploration produces a candidate for reference confirmation. It does not automatically establish a win on another backend.

`best_observed`, `best_confirmed`, and `incumbent` always include their comparison scope. An unqualified global “best” is not a valid API response.

## 8. Project contract

### 8.1 Ordinary repository onboarding

A repository provides:

1. A manifest.
2. One or more executable stages.
3. A structured result format.
4. References to data, environments, and evaluators.
5. A declared candidate mutation surface.

The engine does not import the project's model classes. A small wrapper may adapt existing commands to the result format. That wrapper belongs to the project.

The project retains its own source-control, data, scientific, and contribution rules.

### 8.2 Proposed manifest

The following TOML illustrates the contract. The script names are proposed project-local wrappers, not existing upstream commands.

~~~toml
schema_version = "1.0"

[project]
id = "example-language-model"
name = "Fixed-budget language-model research"

[source]
mode = "git"
base_revision = "PINNED_COMMIT"
include_dirty_changes = false

[mutation]
allow = ["train.py"]
deny = ["prepare.py", "evaluation/**", "data-manifest.json"]

[commands]
prepare = ["python", "research_entrypoints/prepare.py"]
smoke = ["python", "research_entrypoints/smoke.py"]
train = ["python", "research_entrypoints/train.py"]
evaluate = ["python", "research_entrypoints/evaluate.py"]

[environment]
profile = "pytorch-cuda"
lock_artifact = "artifact:environment-lock"
network_during_execution = false

[resources]
accelerators = 1
exclusive_device = true
min_memory_bytes = 8589934592
cpu_slots = 4

[data]
manifest = "artifact:dataset-manifest"
preparation_contract = "artifact:preparation-contract"

[evaluation]
contract = "artifact:evaluator-contract"
primary_metric = "val_bpb"
direction = "minimize"
unit = "bits_per_byte"
aggregation = "evaluator_defined"

[timing]
profile = "active-training-v1"
training_budget_ms = 300000
startup_limit_ms = 300000
evaluation_limit_ms = 300000
wall_limit_ms = 900000

[search]
policy = "greedy"
promotion_policy = "upstream-compatible"

[artifacts]
required = ["run-result.json", "stdout.log", "stderr.log"]
optional = ["checkpoint", "training-curve.jsonl"]
~~~

The manifest is a template. A Campaign resolves every alias, revision, profile, and artifact reference into an immutable ExperimentSpec.

A run must not depend on an alias such as `latest` that can resolve differently after restart.

### 8.3 Command execution

Commands are argument arrays. The worker does not interpolate arbitrary shell strings by default.

The worker supplies context through a versioned JSON file. Environment variables identify its path and the result-output path:

~~~text
RESEARCH_CONTEXT_FILE
RESEARCH_RESULT_FILE
RESEARCH_ARTIFACT_DIR
~~~

These paths are worker-local. They are not portable artifact identities.

The context supplies IDs, seed, input mounts, budget, output contract, and permitted capabilities. It supplies secret references only through the enrolled secret mechanism.

A command writes a structured result and exits. The worker captures process status independently. A result file cannot override a nonzero exit or missing required evidence without an explicit project policy.

Streaming metrics use a separate bounded channel or JSONL file. Human stdout remains available without becoming the authoritative source of every measurement.

### 8.4 Timing contract

Record these phases independently:

- Environment preparation.
- Data preparation and transfer.
- Process startup.
- Compilation and warmup.
- Active training.
- Evaluation.
- Artifact publication.
- End-to-end elapsed time.

A fixed training budget needs a trusted timing mechanism. An editable candidate must not gain unlimited work by relabeling training as warmup.

Where a trusted harness can control the timed region, it uses a monotonic clock and bounds startup and warmup separately.

Where onboarding preserves an existing candidate-controlled timer, declare that trust limitation. An upstream-compatible wrapper is not automatically a tamper-resistant evaluator.

The worker enforces a hard process deadline in addition to the scientific timing contract. It records overshoot and termination behavior.

### 8.5 Data and preparation

Preparation is a schedulable dependency, not an invisible precondition.

A prepared artifact records:

- Input source manifests and checksums.
- Transformation source and environment.
- Schema, shapes, dtypes, units, and label masks.
- Split membership and participant/recording boundaries where applicable.
- Compression, chunking, indexing, and expected size.
- Compatibility with consumers.
- Access restrictions and retention.
- Whether preparation accessed protected data.

A cache hit requires matching content and preparation identity. Matching filenames or approximate shape is insufficient.

The scheduler considers data locality. Moving a large dataset for every short trial can erase parallel speed gains.

## 9. Execution schemas

### 9.1 ExperimentSpec fields

| Group | Required fields |
|---|---|
| Identity | Schema version, project ID, campaign ID, trial ID, spec digest. |
| Scientific payload | Candidate ID, parent IDs, hypothesis reference. |
| Source | Base revision, patch or source artifact, mutation policy digest. |
| Protocol | Protocol ID, comparison class, evaluator and data references. |
| Execution | Stage definitions, command arrays, dependency edges. |
| Environment | Engine profile, lock/image reference, required features. |
| Resources | CPU, memory, device requirements, storage, locality constraints. |
| Randomness | Seed plan, initialization policy, random-state handling. |
| Timing | Phase budgets, deadline semantics, clock profile. |
| Accounting | Declared budget requirements, maximum execution envelope, and charge policy. |
| Outputs | Required artifacts, result schema, metrics schema. |
| Recovery | Retry policy, checkpoint policy, cancellation policy. |
| Authority constraints | Workspace identity, required permission scope, and frozen policy versions. |

Do not copy credentials into ExperimentSpec.

### 9.2 RunResult fields

~~~json
{
  "schema_version": "1.0",
  "campaign_id": "campaign_example",
  "trial_id": "trial_example",
  "job_id": "job_example",
  "attempt_id": "attempt_example",
  "spec_digest": "sha256:EXAMPLE",
  "worker_id": "worker_example",
  "worker_session_id": "session_example",
  "control_epoch": 3,
  "fencing_generation": 12,
  "execution": {
    "status": "succeeded",
    "exit_code": 0,
    "started_at": "2026-10-30T00:00:00.000Z",
    "finished_at": "2026-10-30T00:05:40.000Z"
  },
  "timing_ms": {
    "startup": 20000,
    "training": 300000,
    "evaluation": 20000
  },
  "artifact_refs": [
    {"id": "artifact_checkpoint", "sha256": "EXAMPLE"},
    {"id": "artifact_stdout", "sha256": "EXAMPLE"}
  ],
  "measurements": [
    {
      "name": "val_bpb",
      "value": 1.3312,
      "unit": "bits_per_byte",
      "direction": "minimize",
      "source": "evaluation_example"
    }
  ],
  "execution_fingerprint": "fingerprint_example",
  "result_digest": "sha256:EXAMPLE"
}
~~~

Opaque IDs and digests in examples are illustrative. Production schema fixtures use valid generated IDs and actual digest syntax.

A failed result sets an explicit status and may omit measurements. The receiving API verifies the attempt, current acceptance authority, outputs, and schema. Training-worker measurements remain descriptive until a qualified EvaluationReceipt supports scientific acceptance.

The result does not contain an authoritative `promote: true` field.

### 9.3 Evaluation and decision records

An Evaluation records the input artifact, evaluator contract, data panel, exposure class, seed or replicate grouping, raw metrics, validity checks, and missingness.

A PromotionDecision records:

- Candidate and comparator IDs.
- Expected incumbent revision.
- Referenced evaluations.
- Comparison policy and version.
- Effect size and direction.
- Uncertainty or replication evidence.
- Practical threshold.
- Eligibility checks.
- Decision: promote, reject, replicate, or inconclusive.
- Reason codes and optional human-readable explanation.
- Decision actor and authorization.

An LLM explanation may accompany a decision. It cannot substitute for required measurements.

Scientific evaluation also requires an EvaluationRequest and an authenticated EvaluationReceipt. Section 9.6 defines that acceptance boundary.

### 9.4 Compatibility rules

Unknown required capabilities cause preflight rejection.

Optional metadata extensions use namespaced fields. They cannot modify scientific or authorization semantics without a schema revision.

Readers may preserve unknown optional metadata. They must not silently interpret unknown state transitions or required enum values.

Additive changes use a minor schema version where compatible. Breaking changes require a major version and migration strategy.

### 9.5 DispatchEnvelope

An immutable ExperimentSpec does not contain mutable launch authority.

A per-attempt DispatchEnvelope contains the attempt and job IDs, spec digest, worker session, current controller epoch, and fencing generation. It also contains actual reservation IDs, launch permit, expiration, and allowed result-publication scope.

The controller issues a new envelope when policy permits a new attempt. Ownership transfer does not rewrite the ExperimentSpec or its scientific digest.

Workers validate both documents. The specification says what may execute. The envelope grants this particular execution its bounded authority.

### 9.6 EvaluationRequest and EvaluationReceipt

An EvaluationRequest binds the exact candidate-output digest, evaluator contract, data panel, metric schema, comparison scope, and exposure authorization.

An EvaluationReceipt references that request and includes the observed input digests, measurements, validity checks, execution identity, and evaluator principal.

The service accepts scientific measurements only from an evaluator principal authorized for that request. The candidate process receives no evaluator credential or receipt-signing authority.

In a protected profile, a qualified evaluator process executes frozen code and dependencies with controlled data access. It publishes the receipt through its own authenticated channel. A training result that merely names an evaluation ID does not satisfy this contract.

Trusted-local and upstream-compatible profiles may explicitly accept self-reported measurements. Their records carry `measurement_trust = self_reported`. They cannot claim protected evaluation or automatically satisfy a policy requiring an independent evaluator.

An authenticated receipt establishes provenance under its declared trust profile. It does not prove the host itself is trustworthy or the scientific design is valid.

## 10. Campaign, job, and attempt lifecycle

### 10.1 Campaign lifecycle

~~~text
draft -> ready -> running
running -> pausing -> paused -> running
running -> awaiting_input -> running
running -> stopping -> cancelled
running -> completed
running -> budget_exhausted
running -> failed
~~~

Completion requires a recorded stopping condition and a final report. A chat reply, successful smoke test, or completed milestone does not complete the Campaign.

`awaiting_input` includes a precise missing decision or dependency. It does not repeatedly spawn agents to ask the same unresolved question.

Pause has explicit modes:

- `drain`: stop admitting new work and allow owned attempts to finish.
- `checkpoint`: request supported checkpoints, then stop the affected attempts.
- `terminate`: stop the exact owned processes under the declared policy.

A backend that cannot support a requested pause mode returns a capability error.

### 10.2 Job and attempt state

A Job moves through dependency-blocked, ready, queued, active, and a terminal outcome. Its current Attempt has a more detailed state:

~~~text
reserved -> starting -> running -> publishing -> succeeded
                     -> failed
                     -> cancel_requested -> cancelled
                     -> execution_unknown -> reconciled outcome
~~~

A startup failure can occur before any training begins. It still has an Attempt record and measured preparation cost.

`execution_unknown` means the controller cannot establish the physical process state. It does not mean the process stopped.

A lease expiry fences acceptance and new authority. It does not prove termination, refund consumed cost, or make the device safe to reuse.

### 10.3 Retry semantics

Separate:

1. Delivery retry: repeat the same dispatch intent.
2. Publication retry: submit the same result or artifact again.
3. Execution retry: start a new Attempt.
4. Scientific replicate: create a new Trial.

The first two are idempotent transport operations. The third requires a retry policy and capacity proof. The fourth is a research decision.

A driver must not convert its timeout into an unrecorded fresh training run.

Every retry preserves the failed attempt. Reports must not select the best retry while hiding worse realizations.

### 10.4 Cancellation and completion

Cancellation is initially a requested state. The worker acknowledges the request, stops its exact process group or platform equivalent, and reports termination evidence.

Only then may the controller mark the attempt cancelled and release execution capacity.

An offline worker retains uncertain resource and budget accounting. Reconciliation determines the final disposition.

Scientific completion, operational closure, and retention have separate records. A completed campaign can have a clearly reported unresolved resource reconciliation. That condition must not silently restart research or reopen a settled objective.

Terminal campaigns may accept evidence and settle accounting for previously authorized attempts. They cannot admit new research work without a separately authorized continuation.


## 11. State, transactions, and event delivery

### 11.1 Authoritative state

Each workspace has one authority for shared resource reservations. Each Campaign has one active orchestration owner and a monotonically increasing control epoch.

The domain store is authoritative for:

- Campaign lifecycle.
- Pending proposals and jobs.
- Budget availability and charges.
- Resource ownership.
- Accepted results and observations.
- Incumbent revisions.
- Evaluation exposure.
- Outbox notifications.

Vendor workflow history is not the scientific ledger. A reporting database is not a second resource authority.

The initial SQLite implementation uses short transactions and a serialized write path where useful. Long subprocess calls, model calls, uploads, and report generation occur outside transactions.

SQLite permits one writer at a time. The controller must measure contention rather than assume unlimited write concurrency. [SQLite guidance](https://www.sqlite.org/whentouse.html)

SQLite is not placed on a network filesystem for remote workers to open. A hosted deployment uses a suitable service or database adapter. The requested specification folder is only a document destination, not the proposed runtime database location.

### 11.2 Required atomic operations

A StateStore must implement domain operations, not expose a generic key-value facade that leaves concurrency to callers.

| Operation | Atomic effects |
|---|---|
| Submit trial | Check idempotency, validate campaign revision, record spec and pending work, append event. |
| Claim attempt | Verify worker and owner epoch, reserve capacity and budget, create attempt and lease, append dispatch event/outbox. |
| Renew lease | Check current fence and worker session, update heartbeat and desired control state. |
| Accept result | Verify authority, result digest, artifacts, and required evaluator receipts, then settle accounting and append event/outbox. |
| Reconcile attempt | Verify original journal/result and closure evidence under current authority, then settle the original attempt and append event/outbox. |
| Commit policy transition | Check expected policy revision, publish proposals, consume observation IDs, update policy state, and append event/outbox. |
| Reserve child budget | Reduce parent availability and create child allocation without double counting. |
| Record evaluation exposure | Reserve or charge the declared evaluation access before protected scoring begins. |
| Promote candidate | Validate evidence and expected incumbent revision, replace incumbent, append decision/event. |
| Change orchestration owner | Verify quiescence or reconciliation, advance epoch, record ownership transfer. |

If a backend cannot provide an operation's atomic guarantees, it must declare the capability unsupported. It cannot claim conformance because its method names match.

Objects in external storage cannot participate in the same database transaction. Artifact publication uses staged content and a verified reference commit.

### 11.3 Event envelope

~~~json
{
  "schema_version": "1.0",
  "event_id": "event_example",
  "scope_id": "workspace_example",
  "sequence": 124,
  "aggregate_type": "attempt",
  "aggregate_id": "attempt_example",
  "aggregate_revision": 4,
  "type": "attempt.result_accepted",
  "occurred_at": "2026-10-30T00:05:40.000Z",
  "recorded_at": "2026-10-30T00:05:40.250Z",
  "actor_id": "worker_example",
  "causation_id": "command_example",
  "correlation_id": "campaign_example",
  "payload": {
    "result_ref": "artifact_result",
    "control_epoch": 3
  }
}
~~~

Events preserve ordering within their authoritative scope. Cross-scope ordering is not guaranteed by wall-clock timestamps.

Use an outbox to publish notifications after the state transaction commits. Consumers deduplicate by event or observation ID.

Delivery is at least once. The acceptance path prevents duplicate logical effects. The system does not promise exactly-once physical execution across network partitions.

### 11.4 Initial database tables

The first schema should represent the following concepts, even if several share a compact implementation:

~~~text
workspaces              projects
campaigns               campaign_budget_accounts
budget_entries          child_allocations
hypotheses              candidates
candidate_parents       populations
trials                  jobs
job_dependencies        attempts
workers                 worker_sessions
resource_reservations   leases
artifact_metadata       artifact_references
evaluations             evaluation_exposures
promotion_decisions     incumbents
policy_artifacts        policy_states
proposal_requests       observations_consumed
events                  outbox
idempotency_records     schema_migrations
~~~

Trajectory and learning tables may arrive later. Early events must preserve IDs needed to derive them.

Avoid storing every training step as a synchronous database transaction. Put high-volume curves in artifact streams and retain indexed summaries.

### 11.5 Restart and reconciliation

On restart, the controller:

1. Verifies schema and authority.
2. Acquires the controller role for its scope.
3. Reconstructs pending work from state.
4. Replays unsent outbox entries.
5. Reconciles active attempts with worker journals.
6. Classifies missing workers as uncertain.
7. Resumes eligible dispatch without duplicating active work.
8. Emits a recovery summary.

A restart does not erase failed attempts, reset budget counters, or assume all expired leases are safe to rerun.

Store recovery checkpoints for policy state. They reduce startup cost, but events and accepted observations remain the source of provenance.

### 11.6 Late results and reconciliation

An authorized reconciliation command can recover a legitimate result from an expired lease.

It authenticates the original attempt and worker journal. It verifies the immutable result, required evaluator receipts, process closure, resource disposition, and accounting. It then settles the original attempt under the current controller epoch.

The reconciliation receipt references the original result without changing its digest or historical epoch. It grants no new execution authority.

An exact replay of an already accepted result returns the original acknowledgment after actor authorization and digest matching. This remains true after an epoch change. It creates no new state transition.

If reconciliation cannot prove the required facts, the result remains quarantined and the uncertainty remains visible.

### 11.7 Evidence correction and invalidation

An authorized correction command appends an invalidation or replacement record. It never overwrites the original evidence.

The transaction marks dependent comparisons and promotions as affected. It suspends incumbent eligibility when required and emits a new correction observation.

Preserve the original observation and all actions taken from it. A correction is a new observation with its own identity.

Each SearchPolicy declares whether it rebuilds state from eligible evidence, applies a compensating transition, or requires explicit review. A rebuild creates a new state generation and preserves the old one.

Selecting a replacement incumbent requires eligible evidence and a new promotion decision. Do not silently roll back to an ancestor that may also be invalid.

## 12. Artifact store and reproducibility

### 12.1 Artifact protocol

The artifact interface supports:

~~~text
begin_upload(metadata, expected_digest, expected_size)
upload_parts(upload_id, ...)
finalize_upload(upload_id)
verify(artifact_id)
get_reference(artifact_id)
fetch(artifact_id, destination)
retain(artifact_id, policy)
release_reference(artifact_id, owner)
~~~

Small local artifacts may publish through a single atomic operation. Remote multipart uploads use the same final verification semantics.

An artifact becomes referenceable for scientific acceptance only after content and required metadata pass verification.

A database failure after upload leaves an unreferenced staged object. Reconciliation may recover or remove it under retention policy. It must not fabricate an accepted result.

### 12.2 Artifact classes

| Class | Examples | Default handling |
|---|---|---|
| Source | Patch, source bundle, environment lock. | Retain for replayable candidates. |
| Scientific inputs | Dataset manifest, split definition, tokenizer. | Reference immutable source and access policy. |
| Preparation | Feature cache, compressed tensors, index. | Reusable by content and compatibility identity. |
| Execution evidence | stdout, stderr, phase timings, exit receipt. | Retain according to campaign policy. |
| Model outputs | Checkpoints, predictions, embeddings. | Optional size-aware retention. |
| Evaluation | Metric records, per-group results, uncertainty output. | Retain when used by a decision. |
| Policy | Prompt, search program, model checkpoint. | Version and retain when adopted or compared. |
| Reporting | Markdown, HTML, JSON summaries. | Derived and regenerable where possible. |

A content digest does not grant access. Authorization is checked separately.

### 12.3 Environment reproducibility

Persist backend-specific environment profiles. Do not force CUDA, ROCm, MLX, MPS, and DirectML into one universal dependency lock.

A profile records:

- Supported OS and architecture.
- Python version constraints.
- Dependency lock or container digest.
- Framework and accelerator-runtime requirements.
- Driver compatibility checks.
- Precision and compiler configuration.
- Qualification tests and their last valid result.
- Network and package-cache policy.

A worker may reuse an environment only when its frozen inputs match. An agent cannot silently install into a shared environment used by unrelated campaigns.

Reproducibility means the system can reconstruct the declared conditions and account for remaining nondeterminism. It does not guarantee bitwise-identical GPU training on every platform.

### 12.4 Retention and deletion

The campaign defines retention classes and storage budgets. Required evidence cannot be deleted while an active decision or replay contract depends on it.

Retain metadata for deleted optional artifacts, including their digest and deletion reason. A report must show when replay is no longer possible.

Cleanup operates only on owned, unreferenced, eligible content. It must not infer that an active external checkout or cache is disposable because it is old.

Completion does not require indefinite retention of every temporary output. Scientific records and disposable execution scratch have different policies.

## 13. Workers and resource scheduling

### 13.1 Worker enrollment

A local worker registers automatically with the local service using a local identity.

Remote enrollment is an explicit operator action. The proposed command is:

~~~sh
research worker connect --controller https://research.example --name gpu-worker
~~~

Enrollment credentials come from an interactive secure flow, an enrolled secret store, or a protected environment reference. Do not pass raw secrets in command-line arguments.

The worker initiates outbound communication. It does not require inbound shell access from proposal agents.

Enrollment binds:

- Worker identity.
- Workspace and allowed projects.
- Allowed devices and capacity.
- Storage roots.
- Available environments.
- Secret references.
- Network policy.
- Maximum concurrency and budget exposure.
- Offline and cancellation behavior.

Registration is not permission to use every resource on a machine.

### 13.2 Capabilities

~~~json
{
  "schema_version": "1.0",
  "worker_id": "worker_example",
  "session_id": "session_example",
  "platform": {"os": "linux", "arch": "x86_64"},
  "engines": [
    {
      "framework": "pytorch",
      "device_backend": "cuda",
      "profile_id": "profile_example",
      "features": ["fp32", "bf16", "checkpoint_resume"]
    }
  ],
  "devices": [
    {
      "device_id": "accelerator_0",
      "memory_bytes": 25769803776,
      "exclusive_slots": 1
    }
  ],
  "cpu_slots": 8,
  "memory_bytes": 68719476736,
  "scratch_available_bytes": 107374182400,
  "cached_artifact_ids": ["artifact_dataset"],
  "protocol_versions": ["1.0"],
  "qualification_refs": ["qualification_example"]
}
~~~

Concrete model names, runtime versions, and device architecture appear in real fingerprints. This illustrative payload does not claim that any private machine is enrolled.

Capabilities combine probed facts and operator allowances. The scheduler must not treat an operator limit as detected physical capacity.

Capabilities are time-sensitive. A changed runtime, driver, environment, or device can invalidate a qualification.

The native supervisor also arbitrates physical resource ownership. It uses a stable host/device identity and a machine-local exclusive reservation.

That reservation binds controller, workspace, attempt, worker session, and fence. A second worker process or controller cannot claim the same exclusive device independently.

The first implementation forbids concurrent multi-authority enrollment of a device and enforces that restriction locally. Supporting it later requires an explicit shared admission design.

Machine-local admission still respects pre-existing native owners. A platform lease cannot make unrelated external GPU use disappear.

### 13.3 Placement

The scheduler evaluates:

1. Required engine features.
2. Comparison and hardware class.
3. Device memory and exclusivity.
4. CPU and host-memory requirements.
5. Scratch and artifact capacity.
6. Data residency and locality.
7. Existing reservations.
8. Worker availability and health.
9. Campaign priority and fairness.
10. Estimated transfer and startup overhead.

Default to one independent experiment per reserved accelerator. Shared-device execution is an explicit qualified profile, not an opportunistic assumption.

Host CPU, data loading, storage bandwidth, and unified memory can constrain throughput before the accelerator count does.

### 13.4 Worker journal

A worker persists enough state to reconcile a controller restart or temporary disconnection:

- Received dispatch ID and spec digest.
- Attempt ID and current fence.
- Local process identity and start evidence.
- Artifact staging paths and upload progress.
- Measured resource consumption.
- Last acknowledged controller sequence.
- Desired and observed cancellation state.
- Completion receipt and publication status.

The journal does not grant indefinite authority. Every attempt has a bounded offline policy and hard deadline.

A worker that reconnects reports its actual state before claiming more capacity.

### 13.5 Leases and fencing

The controller assigns an attempt credential and fencing generation. Every heartbeat and result carries them.

A stale generation cannot change accepted state. Preserve a stale result as quarantined evidence when useful.

Do not automatically release a physical device on lease expiry. The process may still be running.

Safe reuse requires one of:

- Verified process exit from the owning worker.
- A qualified native supervisor proving termination.
- Operator reconciliation under a recorded authority.
- Isolation that proves the previous execution resource no longer exists.

Unknown resource state reduces available capacity. It must not cause broad process-name cleanup.

### 13.6 Distributed jobs

A later `ResourceGroup` represents one job requiring several devices or nodes.

It contains:

- Member requirements and rank count.
- Atomic reservation policy.
- Rendezvous identity and expiration.
- Per-rank attempt identity.
- Shared start deadline.
- Group cancellation and failure policy.
- Checkpoint/restart policy.
- Communication requirements.

A partial group reservation cannot start a job that requires all members. Group timeout must reconcile every member before capacity reuse.

The worker launches the project's distributed command. The platform does not implement tensor parallelism or collectives.

## 14. Execution engines and platform support

### 14.1 Engine model

Separate these properties:

~~~text
framework: pytorch | mlx | custom
device_backend: cpu | cuda | rocm | mps | metal | directml | custom
execution_profile: qualified combination of runtime, environment, and features
~~~

CUDA is an accelerator/runtime ecosystem. MLX is a framework. They can be presented as user-facing engine profiles while preserving the distinction internally.

Current MLX documentation includes CUDA-related distributed support. The architecture must not hardcode MLX as Apple-only. Apple Silicon remains the first intended MLX qualification target. [MLX distributed documentation](https://ml-explore.github.io/mlx/build/html/usage/distributed.html)

### 14.2 Initial profile priorities

| Profile | Intended role | Release expectation |
|---|---|---|
| Python CPU command | Protocol tests, smoke runs, small workloads. | Mandatory first release. |
| PyTorch CUDA | Original autoresearch and supported GPU workloads. | First accelerator profile. |
| PyTorch MPS | Compatibility with existing Mac research code. | Early qualification. |
| MLX Metal | Native Apple research implementation. | Early qualification with a real workload. |
| PyTorch ROCm | AMD compute. | Add on a supported, tested hardware/runtime pair. |
| MLX CUDA | Possible qualified extension. | Investigate against a specific workload and environment. |
| DirectML | Optional Windows portability. | Later, only with explicit operator and numerical tests. |
| Custom command | Non-training and specialized research. | Supported by the generic worker contract. |

Windows, Linux, and macOS need separate process-supervision qualification. A command worker is not cross-platform merely because Python imports successfully.

### 14.3 Qualification levels

1. Environment probe.
2. Import and capability checks.
3. Tiny numerical/shape test.
4. Workload smoke.
5. Evaluator semantics check.
6. Reproducibility/variance characterization.
7. Performance qualification.
8. Scientific comparison eligibility.

A pass at one level does not imply later levels passed.

### 14.4 MLX-specific requirements

For compiled training, record random-state handling and compilation behavior. Distinguish cold compilation, warm execution, and the timed training region.

Custom Metal kernels require reference comparisons and resource limits. Kernel-generation research is a later mutation surface.

Multi-Mac execution is optional. First measure independent one-Mac trials. Add inner-loop distributed execution only when its communication and synchronization costs justify it.

Document topology and runtime requirements for a selected distributed backend. The existence of JACCL, ring, MPI, or NCCL does not establish that a particular fleet or workload is qualified.

### 14.5 PyTorch-specific requirements

CUDA and ROCm profiles have separate environment and capability checks. Device detection alone is insufficient.

MPS compatibility does not imply the same compiler behavior or numerical path as CUDA.

Unsupported operators fail clearly. A fallback to CPU must be declared and reflected in the execution fingerprint. A silent fallback cannot produce an eligible performance comparison.

Use selective custom kernels only after identifying a bottleneck and validating against a reference.

### 14.6 Technologies outside the core

- ONNX Runtime can support export, inference, or validation workloads.
- Triton can support qualified custom-kernel experiments on supported targets.
- JAX can be a project-selected engine when its semantics are useful.
- Direct Metal or MPSGraph can support specialized projects.
- Distributed training libraries remain inside project commands.
- Dependency lifecycle changes, including PyTorch/XLA changes, are qualification inputs rather than reasons to expand the core.

The first release does not need adapters for all of these.

## 15. API, SDK, CLI, and MCP

### 15.1 One command model

The public contract is a set of domain commands and queries.

The recommended v1 network transport is HTTP with JSON, an OpenAPI description, and a resumable event stream. A local SDK can call the same handlers in process.

MCP exposes those commands to coding agents. It does not maintain separate campaign state.

A future JSON-RPC or other RPC transport may map the same command model. Do not maintain divergent business logic for REST, MCP, and the CLI.

### 15.2 API operations

| Domain operation | Proposed HTTP route | Agent/SDK operation |
|---|---|---|
| Inspect capabilities | GET /v1/capabilities | research.capabilities |
| Register project | POST /v1/projects | research.register_project |
| Inspect project | GET /v1/projects/{id} | research.get_project |
| Create campaign | POST /v1/campaigns | research.create_campaign |
| Validate campaign contract | POST /v1/campaigns/{id}/validations | research.validate_campaign |
| Seal campaign contract | POST /v1/campaigns/{id}/seal | research.seal_campaign |
| Start campaign | POST /v1/campaigns/{id}/start | research.start_campaign |
| Get bounded context | GET /v1/campaigns/{id}/context | research.get_context |
| Query trials | GET /v1/trials | research.query_trials |
| Propose candidate | POST /v1/candidates | research.propose_candidate |
| Validate candidate | POST /v1/candidates/{id}/validations | research.validate_candidate |
| Submit trial | POST /v1/trials | research.submit_trial |
| Inspect trial | GET /v1/trials/{id} | research.get_trial |
| Compare candidates | POST /v1/comparisons | research.compare_candidates |
| Request replication | POST /v1/replication-requests | research.request_replication |
| Propose promotion | POST /v1/promotion-decisions | research.propose_promotion |
| Commit eligible promotion | POST /v1/promotions | research.commit_promotion |
| Pause campaign | POST /v1/campaigns/{id}/pause | research.pause_campaign |
| Resume campaign | POST /v1/campaigns/{id}/resume | research.resume_campaign |
| Reprioritize pending work | POST /v1/campaigns/{id}/priority-amendments | research.reprioritize |
| Amend budget | POST /v1/campaigns/{id}/budget-amendments | research.amend_budget |
| Change future policy | POST /v1/campaigns/{id}/policy-amendments | research.change_future_policy |
| Cancel owned work | POST /v1/cancellations | research.cancel |
| Inspect workers | GET /v1/workers | research.list_workers |
| Inspect budget | GET /v1/campaigns/{id}/budget | research.get_budget |
| Read events | GET /v1/events | research.watch |
| Get artifact metadata | GET /v1/artifacts/{id} | research.get_artifact |
| Get report | GET /v1/campaigns/{id}/report | research.get_report |
| Create child campaign | POST /v1/campaigns/{id}/children | research.create_child_campaign |
| Inspect policy | GET /v1/policies/{id} | research.get_policy |
| Submit policy candidate | POST /v1/policies | research.submit_policy |
| Reconcile original attempt | POST /v1/attempts/{id}/reconciliations | research.reconcile_attempt |
| Correct or invalidate evidence | POST /v1/evidence-corrections | research.correct_evidence |

Worker enrollment, claims, heartbeats, and result publication use a separate authenticated worker API. Proposal agents do not impersonate workers.

### 15.3 Command requirements

Every mutation includes:

- Authenticated actor and workspace.
- Idempotency key.
- Request schema version.
- Relevant expected revision.
- Target campaign or resource scope.
- Structured parameters.

The server returns:

- Command ID.
- Accepted, rejected, or pending disposition.
- Result IDs and current revisions.
- Machine-readable reason codes.
- Evidence references.
- Next permitted operations where useful.

A pending command is not a completed experiment.

### 15.4 Error model

~~~json
{
  "error": {
    "code": "INCUMBENT_REVISION_CONFLICT",
    "message": "The incumbent changed after this comparison.",
    "retryable": false,
    "details": {
      "expected_revision": 7,
      "actual_revision": 8,
      "comparison_id": "comparison_example"
    },
    "suggested_actions": ["refresh_context", "request_comparison"]
  }
}
~~~

Core error codes include:

~~~text
SCHEMA_UNSUPPORTED
CAPABILITY_UNAVAILABLE
PROJECT_CONTRACT_INVALID
IDEMPOTENCY_CONFLICT
REVISION_CONFLICT
BUDGET_EXHAUSTED
RESOURCE_UNAVAILABLE
EXECUTION_UNKNOWN
STALE_FENCE
ARTIFACT_UNVERIFIED
COMPARISON_INELIGIBLE
EVALUATION_INCOMPLETE
EXPOSURE_LIMIT_REACHED
INCUMBENT_REVISION_CONFLICT
AUTHORIZATION_DENIED
ADAPTER_CAPABILITY_UNSUPPORTED
~~~

`retryable` describes whether repeating the same operation is safe. It does not authorize a new scientific execution.

### 15.5 Agent context

`research.get_context` returns a bounded, versioned view:

- Objective and stopping condition.
- Current incumbent and useful alternatives.
- Comparison scope.
- Pending candidates and reservations.
- Recent comparable results.
- Distinct scientific and infrastructure failures.
- Unresolved hypotheses.
- Remaining and uncertain budgets.
- Available qualified workers.
- Relevant constraints.
- Evidence links and retrieval cursors.

It must not dump the entire transcript or all raw logs by default.

The agent can retrieve a cited artifact or a narrower history slice. Every summary retains references to the records supporting it.

### 15.6 SDK example

~~~python
from research_swarm import ResearchClient

client = ResearchClient.local()

campaign = client.campaigns.create(
    project=".",
    objective="Improve the declared validation metric",
    policy="asynchronous-portfolio",
    budget={"accelerator_seconds": 7200},
)

sealed = client.campaigns.seal(campaign.id, expected_revision=campaign.revision)
campaign = client.campaigns.start(campaign.id, expected_revision=sealed.revision)

context = client.campaigns.context(campaign.id)

candidate = client.candidates.propose(
    campaign_id=campaign.id,
    parent_ids=[context.incumbent.candidate_id],
    patch_artifact="artifact_patch",
    hypothesis="Reduce optimizer overhead within the fixed training budget",
)

validation = client.candidates.validate(candidate.id)
trial = client.trials.submit(
    candidate_id=candidate.id,
    seed=42,
    validation_id=validation.id,
    idempotency_key="proposal-17-seed-42",
)

result = client.trials.wait(trial.id)
report = client.campaigns.report(campaign.id, format="json")
~~~

This is proposed SDK syntax. Validation checks execute under the same budget and resource rules. The SDK does not launch hidden unaccounted work.

Provide synchronous convenience methods and an asynchronous client over the same schemas. Keep worker dependencies smaller than the full agent SDK where practical.

### 15.7 CLI behavior

Proposed commands:

~~~sh
research run .
research serve
research worker connect --controller https://research.example
research campaign status CAMPAIGN_ID --json
research trial list --campaign CAMPAIGN_ID --json
research campaign pause CAMPAIGN_ID --mode drain
research campaign resume CAMPAIGN_ID
research campaign report CAMPAIGN_ID --format markdown
research mcp
~~~

`research run .` starts a local foreground service if needed and attaches to the campaign. Its exit behavior must be explicit.

A managed background service can continue after the terminal closes. If no service remains, persisted work is resumable but is not executing by magic.

An external agent can attach to an existing campaign without becoming the lifecycle owner of the process.

### 15.8 Events and pagination

Queries use stable cursor pagination. Large artifacts are fetched separately.

The event stream includes a resume cursor. If history no longer covers that cursor, return an authorized, consistent state snapshot with its exact event-scope watermark.

Do not silently skip missed events.

The client resumes strictly after that watermark and deduplicates event IDs. A report revision is not a substitute for the event watermark. Test concurrent commits while a client recovers from a gap.

MCP tools declare bounded output limits and reference large payloads. The API must support useful agent control without screenshots or repeated full-log parsing.

### 15.9 Authorization

Scopes distinguish:

- Read evidence.
- Propose candidates.
- Submit experiments.
- Operate workers.
- Request protected evaluation.
- Commit promotion.
- Change budgets.
- Change policy.
- Manage adapters and enrollment.

A campaign may authorize automatic promotion under its declared policy. Human approval is not mandatory for every improvement.

Changing an evaluator, increasing a budget, or enrolling a new resource requires the corresponding authority. An agent's explanation cannot grant that authority.

### 15.10 Activation and prospective amendments

Create returns a draft Campaign. Validation checks its contract without changing lifecycle state. Seal resolves and freezes its configuration, then moves draft to ready. Start moves ready to running after resource and authority preflight.

Dynamic validation that consumes compute runs as an accounted Job. Static contract validation does not grant a hidden execution budget.

Reprioritization changes pending-work ordering. It does not modify an active attempt's immutable specification.

Budget amendments require expected revision and budget-management authority. They append a limit-change event without rewriting consumption. A reduced limit below current commitments stops further admission and applies the declared drain or cancellation policy.

Future-policy amendments record an effective sequence and a new policy artifact. They apply to future proposals and unsealed work. Existing sealed trials and historical decisions retain their original policy versions.

Every amendment records actor, reason, old value, new value, and effective scope. Terminal campaigns remain terminal. Further research starts a new campaign or an explicitly defined continuation.

### 15.11 Worker and evaluator commands

The worker API exposes enroll, connect, claim, heartbeat, publish-result, acknowledge-cancellation, and reconcile-journal operations.

The evaluator API exposes accept-request, record-exposure, and publish-receipt operations. It uses a separate role and authorization scope.

These commands share the same idempotency and error model as the research API. Proposal-agent credentials cannot call worker or evaluator publication endpoints.

## 16. Local, Temporal, and Cloudflare adapters

### 16.1 Shared guarantees

Every conforming deployment provides:

- Stable object identities.
- Atomic resource and budget claims.
- Durable dispatch intent.
- Idempotent result acceptance.
- Explicit attempt lineage.
- Observable cancellation.
- Recovery from duplicate delivery.
- Preservation of execution uncertainty.
- Atomic promotion.
- Equivalent report semantics.

The adapter contract specifies observable behavior. It does not require identical internal tables or provider histories.

### 16.2 Adapter matrix

| Primitive | Local | Temporal-backed | Cloudflare-backed |
|---|---|---|---|
| API | Local Python service. | Same research API. | Workers-hosted facade or equivalent. |
| Domain authority | SQLite. | SQLite for small deployments or a transactional server store. | SQLite Durable Object authority, initially per workspace/lab. |
| Artifacts | Filesystem. | Filesystem or object storage. | R2 or another supported object store. |
| Dispatch/waits | Persisted intents plus asyncio. | Workflows/Activities and external completion or signals. | Workflows and external events. |
| Native compute | Local or remote worker. | Same worker protocol. | Same worker protocol. |
| Query projection | Domain queries. | Domain queries or reporting projection. | Object-local queries, optional D1 projection. |
| Additional queue | Not required. | Temporal task routing. | Optional Queues, not required initially. |

A hosted provider can be adopted without adopting its agent framework.

### 16.3 Local driver

The local driver uses persisted ready jobs, timers, and notification intents. Asyncio handles concurrent I/O and subprocess supervision.

Timers are recoverable database facts, not only in-memory sleep calls.

SQLite write contention is measured. Move to another state implementation when transaction latency, availability needs, or deployment topology justify it. Do not invent an arbitrary maximum worker count.

A filesystem artifact implementation supports atomic local publication and digest checks.

### 16.4 Temporal driver

Temporal is the preferred first additional durable driver.

Keep LLM calls, filesystem operations, network I/O, and subprocess work outside deterministic Workflow code. Put them in Activities or native workers.

Two implementation patterns are valid:

1. A dispatch Activity persists work, then the Workflow waits for a result signal.
2. An asynchronously completed Activity waits for an external worker through the research service.

Select one pattern for the initial adapter and test it consistently. Keep provider task tokens inside the adapter. Workers use research attempt credentials.

Temporal supports asynchronous Activity completion in Python. Current Rust SDK interfaces also expose asynchronous completion. Python is therefore a product choice, not a Temporal requirement. [Python completion](https://docs.temporal.io/develop/python/activities/asynchronous-activity), [Rust ActivityError](https://docs.rs/temporalio-sdk/latest/temporalio_sdk/activities/enum.ActivityError.html)

Temporal retries must repeat dispatch or reconciliation safely. They must not create an undisclosed new training realization.

Use bounded histories and deliberate workflow versioning. Replay representative saved histories before an adapter upgrade. [Temporal replay guidance](https://docs.temporal.io/develop/python/best-practices/testing-suite)

### 16.5 Cloudflare driver

The recommended lean hosted shape is:

- Workers for API and MCP access.
- A SQLite-backed Durable Object for authoritative workspace transitions.
- R2 for artifacts.
- Workflows for durable orchestration.
- Optional D1 for cross-campaign reporting.
- Optional Queues for additional dispatch buffering.

Keep all campaigns that share a resource under one compatible reservation authority. Do not split a shared device's lease and budget across unrelated objects without a new coordination design.

A Durable Object can provide transactional, strongly consistent storage. A D1 projection must not become a competing lease authority. [Durable Object storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)

Python Workflows is available. TypeScript remains a reasonable small-adapter implementation choice. The Python Workers runtime does not make Workers a native CUDA or MLX subprocess host. [Python Workflows](https://developers.cloudflare.com/workflows/python/), [Python Workers runtime](https://developers.cloudflare.com/workers/languages/python/how-python-workers-work/)

The hosted adapter must explicitly qualify its command implementation against the portable domain contract. Do not assume the native Python service or its dependencies run unchanged in Workers.

Within each deployment, API, MCP, CLI, and UI use one command implementation. A second-language hosted implementation must pass shared transition fixtures and concurrency tests. This maintenance cost is one reason to defer it until the local contract stabilizes.

Persist a native result before sending the Workflow wakeup event. An outbox retries that notification. Existing Workflow instances can buffer an event before the matching wait. [Workflow events](https://developers.cloudflare.com/workflows/build/events-and-parameters/)

Workflow step limits, payload limits, wait limits, and completed-history retention are finite. Keep the permanent ledger and large artifacts outside Workflow history. Refresh actual provider limits during implementation rather than baking transient quotas into the domain protocol. [Workflow limits](https://developers.cloudflare.com/workflows/reference/limits/)

Queues can support external pull consumers, but duplicate delivery still requires application-level attempt identity and fencing. Do not add Queues unless it solves a measured need. [Pull consumers](https://developers.cloudflare.com/queues/configuration/pull-consumers/)

### 16.6 Adapter switching

Selecting a different adapter for a new Campaign is straightforward.

Migrating an existing Campaign requires:

1. Stop new admission.
2. Drain or reconcile active attempts.
3. Capture a domain-state checkpoint.
4. Verify artifact reachability.
5. Transfer ownership and advance the control epoch.
6. Start the new driver's durable intents.
7. Reject old-driver writes.
8. Verify pending work and accounting.

Live provider coroutine histories are not portable. The portable checkpoint is the research domain state.

Do not run two drivers against the same campaign because both appear healthy.

### 16.7 Alternatives

| Option | Role in this design |
|---|---|
| Ray | Optional execution adapter when accelerator-aware cluster scheduling offers enough value. |
| Dask | Optional data/DAG integration for workloads that already use it. |
| Celery | Possible durable dispatch adapter, with resource semantics supplied by the research kernel. |
| Kubernetes | Deployment substrate when operations justify it. |
| KubeRay | Optional scale path for a Ray deployment. |
| MPI | Inside a distributed experiment, not the default campaign controller. |
| Plain asyncio | Default local execution and control implementation. |

The platform should not accumulate adapters merely to display a long compatibility list.

## 17. Search, swarm behavior, and research memory

### 17.1 Durable search protocol

~~~python
class SearchPolicy:
    async def ask(self, context, pending, budget, request_id): ...

    async def observe(self, observation, expected_state_revision): ...
~~~

The interface is conceptual. Persisted schemas define its actual wire contract.

An ask request records:

- Request ID and policy version.
- Context snapshot reference.
- Pending work at proposal time.
- Requested proposal count.
- Allocated proposal budget.
- Model/provider metadata where relevant.
- Result publication state.

A model response that arrives before a controller crash must not become invisible. Store the response or an execution receipt before candidate publication where possible.

If a model call's outcome is unknown, retain that uncertainty and its possible cost. A retry is a new provider invocation linked to the same proposal request.

### 17.2 Observation types

A policy can receive:

- Valid scientific measurements.
- Promotion rejection.
- Replication needed.
- Numerical or contract failure.
- Infrastructure failure.
- Cancellation.
- Censored or incomplete execution.
- Resource unavailability.

These types remain distinct. A data-transfer error must not teach the optimizer that the model architecture is scientifically poor.

Each observation updates a policy state at most once. The observation ledger survives restart.

### 17.3 Policies

| Policy | Intended use | Priority |
|---|---|---|
| Greedy | Original behavior and a measurable baseline. | First release. |
| Confirmed greedy | Greedy with explicit replication. | First scientific qualification. |
| Multi-start | Independent starting points with low coordination cost. | Early. |
| Asynchronous portfolio | Allocate work across research themes. | First swarm policy. |
| Beam/evolutionary | Retain diverse branches and combine useful changes. | After basic swarm correctness. |
| Bandit allocation | Allocate budget among themes or proposal policies. | Optional. |
| Bayesian numeric search | Optimize declared numeric spaces. | Optional. |
| Reflective policy | Use trajectories and failure explanations. | Early extension. |
| Learned policy | Use a trained proposer or scheduler. | Later. |

Rounds are optional reporting groups. The default scheduler does not wait at a generation barrier.

### 17.4 Agent roles

Possible roles include director, proposer, implementer, critic, literature/memory assistant, and meta-researcher.

Roles are policy configuration. The engine does not require all roles for every candidate.

Cheap validation precedes expensive accelerator use:

1. Mutation-surface check.
2. Syntax and import checks where appropriate.
3. Shape or tiny-device smoke.
4. Duplicate detection.
5. Optional semantic criticism.
6. Resource admission.

A critic's negative opinion is not a scientific result. Record its rationale and the policy that used it.

### 17.5 Candidate diversity and duplication

Use exact duplicate checks on source/configuration identity.

Use optional semantic grouping for related hypotheses. Semantic similarity is advisory unless a declared policy makes it exclusionary.

Track pending hypotheses so parallel agents do not unknowingly repeat the same work.

Retain candidates that offer useful tradeoffs in quality, memory, startup, throughput, or transferability. Preserve raw Pareto dimensions rather than compressing everything into one opaque score.

A recombined candidate records all parents and the exact resulting patch.

### 17.6 Research memory

Start with structured queries over experiments, decisions, artifacts, and failure classes.

Provide bounded evidence-linked summaries. Summaries are derived artifacts with source references and a generator version.

An RLM-style retrieval layer can recursively inspect subsets of a large history. It is a context-management and reasoning mechanism, not a replacement for the search policy.

Retrieved text, logs, papers, and candidate comments remain untrusted data. They cannot change the controller's authority rules.

### 17.7 Decision models

A decision model receives a bounded observation and returns a typed recommendation:

~~~text
propose
allocate
replicate
defer
stop
request_evidence
~~~

Its output includes referenced evidence and optional uncertainty. The kernel validates feasibility and authority.

No provider-specific decision model is required. Keep the interface generic and qualify any selected provider separately.

An external coding agent already qualifies as a controller through the API. A built-in decision model is not required.

### 17.8 Policy transition commit

Compute proposal or observation responses outside the state transaction. Persist the request ID, expected policy revision, input observation IDs, context snapshot, outputs, and incurred cost.

Commit through one operation:

~~~text
commit_policy_transition(
    request_id,
    expected_revision,
    ordered_input_observation_ids,
    proposal_refs,
    next_state_ref
)
~~~

The transaction publishes proposals, updates pending work, consumes observations, stores the new policy state, and advances its revision atomically.

On conflict, retain the computed output and its cost. Reject it or explicitly rebase it against fresh context. Never use last-writer-wins for policy state.

A deterministic policy can compute transitions locally. An LLM-backed policy follows the same commit protocol after its external computation.

Record the actual observation order consumed. The system does not pretend that out-of-order asynchronous search is identical to a synchronous generation.

## 18. Evaluation, statistical rigor, and promotion

### 18.1 Protected evaluation boundary

The evaluator, dataset manifest, split definitions, and scoring policy are outside the candidate's writable surface.

Protection must include transitive dependencies. Freezing an evaluator file is insufficient if the candidate can replace an imported helper or modify the evaluated data.

Where possible, evaluation runs in a separate process and environment over immutable candidate outputs. Restrict access to protected labels.

A Git worktree isolates source changes. It is not an operating-system sandbox.

### 18.2 Metric definition

Every metric declares:

- Name and version.
- Raw value and unit.
- Direction.
- Aggregation and weighting.
- Population or sample definition.
- Missing-data policy.
- Validity domain.
- Associated uncertainty where applicable.

Store raw metrics separately from any utility transformation. A search utility may combine quality, cost, or latency. A promotion decision still references the original scientific metrics.

For a minimized metric, define improvement as:

~~~text
improvement = comparator_metric - candidate_metric
~~~

A positive value is beneficial under that convention. The definition and comparator must accompany the value.

### 18.3 Eligibility before ranking

A result is rankable only when:

1. Execution and artifact requirements passed.
2. The evaluator completed its required panel.
3. Metrics are finite and correctly typed.
4. Units and target semantics match.
5. The comparison policy permits the execution differences.
6. Data leakage and exposure checks meet the project policy.
7. Required resource and timing evidence exists.
8. The result has not been invalidated.

Do not silently rank on whichever participant rows survived a failure. Incomplete-panel behavior must be declared before execution.

### 18.4 Promotion pipeline

~~~text
exploratory trial
    -> validity and comparability checks
    -> clear failure or poor result: reject
    -> promising result: request confirmation
    -> paired or otherwise declared replication
    -> practical and statistical gate
    -> atomic promotion or inconclusive result
~~~

The project selects its gate. The kernel supplies consistent execution and accounting.

A paired design should match candidate and comparator seeds, data order, fidelity, and relevant initialization where meaningful. Record when exact pairing is impossible.

Seed equality across different frameworks does not establish identical random draws. Cross-backend parity may require explicit initial-weight artifacts and data-order manifests.

### 18.5 Statistical policy

A policy declares:

- Minimum practically useful effect.
- Number or range of independent realizations.
- Pairing method.
- Estimator and uncertainty procedure.
- Early rejection rule.
- Any sequential-testing or alpha-spending procedure.
- Maximum confirmation budget.
- Treatment of failed or censored realizations.
- Multiple-comparison and adaptive-search handling.
- Protected evaluation access rules.

Do not repeatedly inspect an ordinary confidence interval and stop at the first favorable result while claiming its original coverage.

Bootstrap methods need a declared resampling unit. Steps from one training run are not independent training seeds. Participant and recording structure may also matter.

Three or five seeds are operational choices. They are not universal proof of significance or generalization.

The first implementation should offer a simple fixed replication policy before implementing sophisticated sequential tests.

### 18.6 Correcting the MLX fork lesson

The supplied research report described a multi-seed gate in the MLX fork. The source check performed for this specification found an important limitation.

The inspected `rigor.py` reruns the training command, while the inspected `train.py` fixes `mx.random.seed(42)`. Repeated launches therefore do not establish distinct seeded realizations by themselves. [Gate source](https://raw.githubusercontent.com/trevin-creator/autoresearch-mlx/main/rigor.py), [Training source](https://raw.githubusercontent.com/trevin-creator/autoresearch-mlx/main/train.py)

Borrow the idea of confirmation and an append-only decision ledger. Do not copy the claimed statistical guarantee without checking implementation.

The new system must:

- Inject a seed explicitly.
- Record the requested and observed seed.
- Record initialization identity when relevant.
- Bind cached comparator evidence to its full benchmark identity.
- Distinguish repeated execution under one seed from multi-seed replication.
- Test that a seed change reaches the training implementation.

The mutable upstream URLs identify the inspected source surface. Pin exact revisions before reusing code.

### 18.7 Protected and official evaluation

Exploratory, confirmation, protected, and official evaluations have separate identities and access policies.

A local confirmation gate cannot erase prior development-set exposure.

A protected evaluation consumes its own access budget. An interrupted request may leave exposure uncertain. Do not automatically refund or repeat it.

Official competition evaluation is an external project-specific stage. The platform records the authenticated receipt and conditions. It does not infer an official result from a local metric.

### 18.8 Atomic promotion

Promotion checks:

- Expected incumbent revision.
- Candidate and comparator identity.
- Eligible evaluation references.
- Promotion-policy version.
- Required authority.
- Unchanged protocol and exposure constraints.

If another candidate became incumbent, return a conflict and preserve the original decision. The policy can request a new comparison.

Promotion does not delete other candidates. The population retains useful branches and failure evidence.

## 19. Budgets and cost accounting

### 19.1 Budget dimensions

A Campaign may constrain:

- Accelerator time.
- CPU time.
- Wall-clock duration.
- LLM tokens or monetary cost.
- Native execution cost.
- Storage and transfer.
- Trial count.
- Evaluation count.
- Protected evaluation exposures.
- Child campaigns and recursion depth.

Some limits are hard admission controls. Others are estimates that become accurate after billing or measurement. The API reports the distinction.

A monetary estimate must include its price source and effective date. Do not pretend an estimated provider bill is exact.

### 19.2 Account states

For cumulative dimensions, record:

~~~text
limit
outstanding_reserved
consumed
outstanding_uncertain
released_total
available
~~~

The normative equation for cumulative dimensions is:

~~~text
available = limit - consumed - outstanding_reserved - outstanding_uncertain
~~~

These balances are disjoint. A reservation occupies availability until settlement or release. Moving work into uncertainty removes the corresponding outstanding reservation first.

The ledger records signed transitions with unique settlement IDs. Settlement transfers a measured amount into consumed and releases any proven unused remainder. `released_total` is a cumulative diagnostic, not another deducted balance.

Admission requires sufficient available budget. Measured overshoot can make availability negative. Record that deficit and stop further admission rather than clipping the evidence.

Unknown execution can retain an uncertain allocation. Releasing it requires evidence or an explicit accounting decision.

All attempts count. Preparation, compilation, failed jobs, evaluations, and policy-training jobs can consume budget.

Reports show both scientific budget and end-to-end cost. Five minutes of active training can require more than five minutes of occupied worker time.

### 19.3 Hierarchical accounting

A child Campaign receives an allocation from its parent.

That allocation reduces the parent's available budget. Child consumption rolls up once. Do not charge the parent both the whole reservation and the same measured consumption as separate spend.

Parallel child creation reserves atomically. A child cannot expand its authority by creating more descendants.

Parent and child accounts share one transactional authority in v1. The parent's outstanding reservation holds the child's unspent allocation. Child consumption settles that reservation into parent consumption exactly once.

Unused allocation returns to the parent only through a recorded release. Moving child accounting across authorities requires a separate transfer protocol and is outside v1.

Live ancestor accounting must also preserve known commitments after overshoot. For a child grant `A`, cumulative consumption `C`, uncertainty `U`, and current reservations `R`, the direct parent's contribution is `consumed=C`, `uncertain=U`, and `reserved=max(R, A-C-U, 0)`. Its total encumbrance is `max(A, C+U+R)`. Apply exact differences from the previous projection; a parent already includes its descendants and must not sum them again.

A zero ancestor availability is valid for work inside an already funded allocation. A negative availability blocks new reservations in the affected dimension, including reservations by funded siblings. Measured usage still propagates; admission rules must never reject the recording of overshoot. Child releases do not directly refund ancestors: the grant floor remains held. Release only a proven decrease in projected encumbrance or the remaining held grant at terminal settlement.

Inherited constraints include:

- Maximum remaining depth.
- Allowed projects and workers.
- Mutation permissions.
- Evaluation access.
- Per-dimension budget limits.
- Stopping conditions.

### 19.4 Admission and overshoot

Before admitting an attempt, reserve its allowed resource and cost envelope.

Measured usage may exceed estimates because of shutdown latency or provider accounting. Preserve the overshoot. Do not rewrite it to fit the original reservation.

A hard native deadline limits continued work. It cannot retroactively remove consumed time.

The campaign stops admitting work when required availability is exhausted. Existing attempts follow their declared completion or cancellation policy.

### 19.5 Progress metrics

Primary platform outcomes:

- Confirmed improvement per accelerator-hour.
- Confirmed improvement per total cost.
- Replicated win rate.
- Unique eligible hypotheses completed per hour.
- Time from proposal to trustworthy decision.
- Fraction of budget lost to infrastructure failure.
- Coordination and proposal cost per accepted finding.
- Transfer success under a second declared protocol.

Also report throughput, memory, and device utilization. They explain outcomes but do not replace them.

### 19.6 Different accounting models

Do not apply cumulative-spend arithmetic to every resource.

| Dimension | Model | Release or completion rule |
|---|---|---|
| Accelerator-seconds, tokens, money | Cumulative consumption. | Consumed amounts never become available through ordinary release. |
| Campaign wall time | Deadline or declared active-interval policy. | Default elapsed time continues during disconnection. Pause behavior is frozen in the campaign contract. |
| Device slots and host memory | Concurrent capacity. | Release requires process closure or qualified reconciliation. |
| Retained storage | Current occupancy plus reservations and uncertainty. | Reclaim capacity after verified deletion or authorized ownership transfer. |
| Trial/evaluation counts | Named admission or exposure counters. | Define whether the unit is admission, execution, or exposure. Never substitute completed count. |

Storage admission uses capacity minus measured occupancy, outstanding reservations, and outstanding uncertainty. Retained outputs remain occupied after their producer exits.

Report peak occupancy separately from current occupancy. If storage cost is time-based, record it as a separate cumulative charge.

Lease and deadline calculations use the qualified authority's time model. Native elapsed durations use monotonic clocks. Preserve clock metadata and test restart and skew behavior.

## 20. Recursive optimization, RSI, and RL

### 20.1 Three separate capabilities

| Capability | What changes | Required evidence |
|---|---|---|
| Ordinary research | Model, data recipe, optimizer, prompt, or other project payload. | Project-defined experiment results. |
| Meta-optimization | Research prompts, search policy, role allocation, or experiment-selection rules. | Comparable campaigns on a policy benchmark. |
| Policy learning | Trainable policy parameters or model weights. | Actual training output and held-out policy evaluation. |

The platform can host all three. Implementing nested campaigns alone does not demonstrate recursive self-improvement.

### 20.2 Parent and child campaigns

A parent meta-campaign submits a policy candidate. Child campaigns execute that policy against declared research tasks.

Each child has:

- A fixed task and starting state.
- Its own experiment ledger.
- A parent-funded budget.
- An immutable policy version.
- A comparable resource allowance.
- A terminal summary.

The parent compares research outcomes, not only the policy's explanation.

A policy that uses more agents or tokens must account for those costs. It cannot appear better merely because it received a larger hidden budget.

### 20.3 Policy benchmark

A policy benchmark should include:

- Multiple research tasks or starting states.
- Frozen initial candidate sets.
- Fixed compute and proposal budgets.
- Repeated policy seeds or runs.
- A declared quality-versus-cost objective.
- Held-out tasks or starting states.
- Failure and timeout treatment.
- A reproducible baseline policy.

An existing research project can contribute a benchmark task if its data and evaluation access permit it. One repeatedly optimized development set is not a general policy benchmark.

### 20.4 Trajectory schema

A trajectory links:

- Campaign, episode, step, and parent IDs.
- Observation/context reference.
- Policy artifact and provider version.
- Proposed action.
- Actual authorized action.
- Tool request and response references.
- Candidate and trial identities.
- Evaluation outcomes.
- Reward and reward-spec version.
- Cost, duration, termination, and censoring.
- Behavior-policy information required by the algorithm.
- Data-access and consent scope.

Do not require unavailable private model reasoning. Record observable actions and outputs.

Prompts and tool outputs can contain sensitive material. Export permissions and redaction policy apply.

### 20.5 RewardSpec

A reward specification declares:

- Which outcome earns reward.
- Baseline and comparison scope.
- Cost penalties.
- Infrastructure-failure treatment.
- Delayed credit rules.
- Invalid or missing result treatment.
- Novelty or diversity terms, if any.
- Scaling and clipping.
- Protected evaluation restrictions.

Preserve raw outcomes alongside reward. A scalar reward must not replace the scientific ledger.

A policy cannot modify its own active reward specification. A proposed reward change creates a separate benchmark version.

### 20.6 Actual RL execution

A learning adapter consumes qualified trajectories and produces a PolicyArtifact through a TrainingJob.

An RL adapter must declare its reinforcement-learning algorithm and reward objective. It must execute a reward-driven optimization update using compatible rollout data. Supervised fine-tuning and behavior cloning are policy training, but do not by themselves establish RL support.

The adapter declares algorithm-specific requirements. PPO-style or GRPO-style training may need policy probabilities, grouped rollouts, reward normalization, and strict behavior-policy versions.

If the required data was not recorded, reject the training dataset as incompatible. Do not silently treat generic agent logs as equivalent.

An inference-only hosted API can operate the search loop. It does not automatically provide the model-weight access needed for policy training.

A learned policy is evaluated before adoption. Model-candidate promotion and research-policy promotion are separate operations.

### 20.7 Recursive code improvement

Research agents may propose changes to search or orchestration code only within a declared meta-project.

Evaluate those changes in an isolated candidate service or shadow run. The live ledger, evaluator, authorization rules, and parent accounting stay protected.

A candidate controller must not promote itself by rewriting the test that judges it.

Use a versioned service artifact and explicit migration plan before adopting a controller change.

### 20.8 Claim levels

Use these claims precisely:

- “Supports nested optimization”: child campaigns and accounting work.
- “Supports policy training”: a compatible learning adapter completes a real policy update.
- “Supports RL training”: a declared RL algorithm performs a reward-driven update with its required rollout data.
- “Improves a research policy”: held-out policy evaluation shows a declared gain.
- “Demonstrates recursive improvement”: repeated independently evaluated policy improvements occur across successive versions.

The first claim does not establish the last.

## 21. Isolation, permissions, and process safety

### 21.1 Source isolation

Each candidate executes from an immutable source bundle or an isolated worktree prepared from the declared base and patch.

Agents do not share a mutable canonical checkout.

Validate:

- Allowed mutation paths.
- Path traversal and symlink behavior.
- Protected evaluator and data paths.
- Source digest.
- Environment identity.
- Output locations.

Candidate source changes do not automatically update the user's working tree or merge into the project.

### 21.2 Trust profiles

| Profile | Intended workload | Required boundary |
|---|---|---|
| Trusted local | User-owned research code. | Separate workspace/process and bounded resources. |
| Restricted native | Agent-generated code on a trusted machine. | Qualified OS controls, limited mounts, network policy, owned process supervision. |
| Containerized | Compatible Linux workload. | Image digest, mounts, resource limits, network controls. |
| Strong isolation | Untrusted code or kernel experiments. | A separately qualified sandbox/VM profile. |

Do not label a Git worktree as a sandbox. Do not promise equivalent isolation across macOS, Windows, and Linux without native tests.

If the requested security profile is unavailable, fail admission. Do not silently downgrade it.

### 21.3 Process ownership

Track the exact process group or platform-native job object for each attempt.

Cancellation affects only that owned execution. It must not use broad process-name kills.

On Windows, qualify process-tree termination and child containment. On Unix-like systems, qualify process groups and surviving descendants.

A controller timeout or workflow cancellation does not prove native termination.

### 21.4 Secrets and networking

Workers receive only secrets required by the enrolled project and stage.

Store references in metadata. Inject values through the configured runtime mechanism. Do not put secrets in source artifacts, command arguments, logs, or reports.

Default candidate execution has no network access where the selected isolation profile can enforce it. A data-download stage may have a separate network policy.

The API rejects unauthorized artifact reads even when the caller knows a digest.

### 21.5 Remote operation

Worker enrollment and capacity configuration are explicit operator actions.

Operators authorize deployment, worker enrollment, and account use separately from installing the software.

A future implementation must respect native ownership and resource admission on each machine. The platform's lease cannot override another service's ownership.

## 22. Reporting, observability, and optional UI

### 22.1 Common report model

Generate JSON and Markdown/HTML from the same report projection.

A report includes:

- Objective, scope, and stopping condition.
- Baseline and incumbent.
- Best observed and best confirmed candidates.
- Comparable trial table.
- Candidate diffs and lineage.
- Evaluation and promotion decisions.
- Failed and incomplete attempts.
- Budget consumption and uncertainty.
- Execution and hardware fingerprints.
- Evidence links.
- Unresolved scientific questions.
- Next permitted actions.
- Resource disposition.

Generated reports must show their schema and source-state revision.

### 22.2 Claim vocabulary

| Label | Meaning |
|---|---|
| Observed | A declared measurement occurred. |
| Qualified | A named interface, numerical, or execution check passed. |
| Confirmed | A named replication/promotion contract passed. |
| Hypothesized | An explanation or predicted mechanism remains unverified. |
| Protected | Evaluation used the declared restricted panel and access policy. |
| Official | A project-specific official evaluator issued a recorded result. |
| Inconclusive | Available evidence does not justify the requested decision. |

An agent-written explanation cannot upgrade a result's evidence class.

### 22.3 Metrics and traces

Record structured events and spans for:

- Proposal generation.
- Validation.
- Queue wait.
- Environment and data preparation.
- Transfer.
- Startup and compilation.
- Training.
- Evaluation.
- Artifact publication.
- Decision.
- Reconciliation.

Include correlation IDs across these phases.

Track agent calls, token/cost estimates, decision latency, failed tool operations, and duplicate work. Do not turn all raw trace text into a synchronous database write.

An optional telemetry exporter can expose metrics to existing observability tools. No specific tracing vendor is required.

### 22.4 Optional interface

The first useful UI should provide four views:

1. Overview: active, queued, blocked, agent-thinking, and budget state.
2. Comparison: hypothesis, patch, curves, metrics, hardware, and eligibility.
3. Review: replication requests, decisions, unresolved evidence, and promotion.
4. Lineage: candidate branches and useful ancestors.

The comparison table should be the default working view. A large node graph is secondary.

The UI uses the public API. Closing it has no effect on a running service.

### 22.5 Human intervention

Authorized users can pause, cancel, reprioritize, allocate budget, request replication, or change future policy.

Changes are recorded as commands with actor and reason. They do not rewrite historical evidence.

A request to alter an active experiment's immutable specification creates a new experiment or cancels/replaces the original under policy.

## 23. Existing-project adoption

### 23.1 Ordinary repository boundary

A research project remains an ordinary repository. It supplies a manifest, commands, evaluator, and structured results.

The platform does not import its model classes or encode its scientific business rules. A project-local wrapper can adapt an existing command interface.

### 23.2 Mapping existing concepts

| Existing concept | Platform concept |
|---|---|
| Research objective and portfolio | Campaign and SearchPolicy. |
| Baseline or candidate recipe | Candidate and Hypothesis. |
| Source revision and patch | Source artifact and lineage. |
| Seed-specific training | Trial and training Job. |
| Native execution receipt | Attempt result and resource disposition. |
| Source data and target definition | Dataset and preparation artifacts. |
| Reusable input package | Qualified preparation artifact. |
| Local evaluation | Evaluation within a declared comparison class. |
| Practical acceptance threshold | PromotionPolicy. |
| External protected evaluation | Separately authorized evaluation stage. |
| Narrative handover | Bounded campaign context and report. |

### 23.3 Staged adoption

1. Import selected historical metadata without rerunning experiments. Preserve its original evidence class and provenance completeness.
2. Execute a disposable small fixture. Compare the new result record with the existing receipt. This proves integration, not full-workload performance.
3. Start a new bounded campaign under the engine. Existing active runs retain their current owner until explicitly migrated.
4. Run independent hypotheses on already qualified resources. Preserve matched controls and data comparability.
5. Reuse qualified preparation artifacts through explicit dependencies.
6. Compare policies under equal budgets and declared scientific conditions.

Each stage has an explicit stop condition. Infrastructure expansion does not silently replace the research objective.

### 23.4 Evidence boundaries

A smoke test does not establish full-cohort throughput. A local metric does not establish an official result. Repeated seeds do not resolve data overlap.

New enrollment does not authorize interference with another owner. Imported results do not gain stronger scientific status merely because the platform records them.

A protected private-project replay may retain evidence outside the public repository. Public fixtures use synthetic data and demonstrate only their declared scope.

## 24. Python, Rust, and implementation technology

### 24.1 Recommendation

Build the first complete system in Python, with language-neutral protocols from the start.

Python keeps the research SDK, project wrappers, search policies, and training ecosystem close together. Asyncio can supervise concurrent subprocesses and I/O. [Python subprocess API](https://docs.python.org/3/library/asyncio-subprocess.html)

The controller is not the tensor execution engine. PyTorch and MLX already perform substantial computation in native implementations. Rewriting orchestration in Rust does not inherently accelerate the same GPU kernels. [PyTorch native frontend](https://docs.pytorch.org/cppdocs/frontend), [MLX documentation](https://ml-explore.github.io/mlx/build/html/index.html)

### 24.2 Suggested initial stack

| Concern | Initial choice |
|---|---|
| Language | Python, with a tested minimum version and backend-specific compatibility matrix. |
| Environment management | uv and explicit profiles. |
| Schemas | JSON Schema/OpenAPI plus generated or validated Python models. |
| API | A small asynchronous HTTP service. FastAPI is a reasonable implementation candidate. |
| Local scheduling | asyncio plus persisted state. |
| Local state | SQLite with explicit transactions and migrations. |
| Local artifacts | Content-addressed filesystem storage. |
| Worker execution | Subprocess supervisor with native platform qualification. |
| Agent access | Bundled MCP facade and Python SDK. |
| Tests | Unit/contract tests, subprocess integration tests, qualified hardware tests. |
| Formatting/types | Repository-selected Python formatter, linter, and type checker. |

FastAPI and a particular model library are implementation defaults to evaluate, not part of the public protocol.

Choose the Python minimum after testing the required PyTorch and MLX releases. Do not copy an old version range from the supplied report without checking current packages.

### 24.3 Rust opportunities

Rust is a credible implementation for:

- A small distributed worker binary.
- Process and resource supervision.
- Streaming artifact transfer.
- CPU-intensive parsing or indexing.
- A measured high-throughput control component.
- A future embedded or constrained worker.

Tokio provides an asynchronous Rust runtime. Temporal also has current Rust SDK facilities. Rust is not blocked by the orchestration choice. [Tokio](https://tokio.rs/tokio/tutorial), [Temporal Rust documentation](https://docs.temporal.io/develop/rust)

### 24.4 Rust adoption gate

Introduce Rust when evidence identifies one of these needs:

1. Python worker deployment is materially difficult across the target fleet.
2. Resident overhead matters for the target hardware.
3. Native process containment needs a focused implementation.
4. Profiling identifies CPU work that dominates control latency.
5. A stable component has enough reuse to justify a second toolchain.

Measure development and maintenance cost alongside runtime benefits.

Prefer a protocol boundary for a separate Rust worker. Use a Python extension only for a narrow in-process hotspot that benefits from it. PyO3 provides such an integration path. [PyO3](https://pyo3.rs/)

### 24.5 Cross-language conformance

A Rust implementation must pass the same:

- Schema fixtures.
- Canonical hash fixtures.
- Idempotency tests.
- State transition tests.
- Budget arithmetic tests.
- Cancellation and reconciliation tests.
- Result acceptance tests.
- Artifact verification tests.

No shared Python object graph is required. Vendor workflow replay histories remain adapter-specific.

The migration unit is a component, not the entire research repository.

## 25. Extension SDK and repository organization

### 25.1 SDK design rules

- Prefer composition and plain records over a deep inheritance tree.
- Keep public interfaces small.
- Make synchronous convenience APIs wrap the same asynchronous contract.
- Return IDs and references rather than live process handles.
- Expose raw evidence alongside convenience summaries.
- Keep metric direction explicit.
- Make lifecycle hooks observational unless they possess explicit command authority.
- Treat optional dependencies as extras.
- Make engine and adapter capability discovery inspectable.
- Provide executable conformance fixtures.

A custom SearchPolicy should not need to implement worker leasing. A custom ArtifactStore should not need to understand model architectures.

### 25.2 Proposed repository layout

~~~text
autoresearch-swarm/
  src/research_swarm/
    domain/
      models.py
      transitions.py
      commands.py
      comparison.py
      budgets.py
    api/
      http.py
      schemas/
      events.py
    sdk/
      client.py
      async_client.py
    mcp/
      server.py
      tools.py
    state/
      protocol.py
      sqlite.py
      migrations/
    artifacts/
      protocol.py
      filesystem.py
    orchestration/
      protocol.py
      local.py
    worker/
      protocol.py
      journal.py
      supervisor.py
      resources.py
      platforms/
    engines/
      protocol.py
      python_command.py
      pytorch.py
      mlx.py
    search/
      protocol.py
      greedy.py
      portfolio.py
      memory.py
    evaluation/
      eligibility.py
      promotion.py
      statistics.py
    reporting/
      model.py
      json_report.py
      markdown_report.py
    learning/
      contracts.py
  adapters/
    temporal/
    cloudflare/
    object_storage/
  examples/
    original_autoresearch/
    small_cpu_project/
    existing_project_onboarding/
  tests/
    contracts/
    state/
    workers/
    adapters/
    hardware/
    fixtures/
  docs/
  pyproject.toml
~~~

This layout describes responsibilities. Do not create empty packages for every future feature before they are needed.

The existing-project example contains onboarding documentation and sanitized contract examples. It does not vendor private data, credentials, or the live research application.

### 25.3 Plugin loading

Python packages may expose registered entry points for trusted in-process adapters.

A subprocess/RPC implementation can implement the same protocol in another language.

Plugin metadata declares:

- Interface and schema versions.
- Supported capabilities.
- Required dependencies.
- Configuration schema.
- Trust and execution model.
- Conformance result.

Do not automatically install or execute arbitrary plugins based on an agent suggestion.

### 25.4 Versioning and migration

Version the API, stored schema, project contract, and plugin interface separately where their compatibility differs.

Database migrations are explicit and tested against representative snapshots. Backups use supported database mechanisms.

A downgrade that cannot interpret new states fails clearly. It does not discard fields or rewrite them into older meanings.

## 26. Testing and qualification

### 26.1 Required contract tests

| Test ID | Scenario | Expected result |
|---|---|---|
| C01 | Submit the same command twice. | One logical effect and the original result IDs. |
| C02 | Reuse an idempotency key with changed payload. | Conflict. |
| C03 | Two workers claim one exclusive device. | Exactly one successful reservation. |
| C04 | Two campaigns reserve the final available budget. | No oversubscription. |
| C05 | Controller crashes after state commit but before notification. | Outbox resumes delivery. |
| C06 | Result arrives twice. | One accepted outcome and one policy observation. |
| C07 | Old worker session publishes after a new fence. | Stale acceptance rejected, evidence preserved as applicable. |
| C08 | Lease expires while the child still runs. | Execution remains unknown and capacity is not reused automatically. |
| C09 | Workflow times out after dispatch. | Reconcile the original attempt before any execution retry. |
| C10 | Cancellation is requested for an offline worker. | Requested/unknown state, not false termination proof. |
| C11 | Worker reconnects after controller restart. | Journal and controller reconcile without duplicate execution. |
| C12 | Artifact upload succeeds but result commit fails. | Staged artifact is recoverable and not falsely accepted. |
| C13 | Required artifact is corrupt or absent. | Result is ineligible. |
| C14 | Candidate changes a protected evaluator dependency. | Preflight rejects the mutation. |
| C15 | Crashed run has no metric. | It cannot rank as the best candidate. |
| C16 | Candidate and comparator use different forbidden protocols. | Comparison is ineligible. |
| C17 | Two promotions reference the same incumbent revision. | One commits and the other conflicts. |
| C18 | Child campaigns reserve concurrently. | Parent accounting remains consistent. |
| C19 | Child finishes and releases unused budget. | Consumption rolls up once and remainder becomes available. |
| C20 | Policy receives a duplicate observation. | State updates once. |
| C21 | Controller crashes after a proposal-model response. | Proposal publication resumes or uncertainty is explicit. |
| C22 | Seed changes between declared replicates. | Training receives and records the distinct seeds. |
| C23 | Evaluation returns an incomplete panel. | Missingness policy applies before ranking. |
| C24 | Protected evaluation times out after access. | Exposure remains consumed or uncertain, not silently refunded. |
| C25 | Adapter ownership changes. | Old epoch cannot mutate the campaign. |
| C26 | Event cursor falls outside retained stream history. | Explicit gap and snapshot recovery path. |
| C27 | Worker lacks a required isolation feature. | Admission fails without downgrade. |
| C28 | Process spawns a child and cancellation occurs. | Qualified supervision accounts for the owned process tree. |
| C29 | Native clocks differ. | Lease authority and monotonic durations remain valid. |
| C30 | API/MCP/CLI perform the same operation. | Equivalent domain effects and error codes. |
| C31 | Controller restarts with budget exhausted. | No new experiment is admitted. |
| C32 | Campaign completes with an unresolved resource. | Scientific state remains terminal with separate reconciliation. |
| C33 | A late valid result returns after lease expiry. | Current authority can reconcile the original attempt without new execution. |
| C34 | An already accepted result replays after epoch change. | Original acknowledgment returns without a new effect. |
| C35 | A training worker fabricates an evaluation ID. | Scientific acceptance fails without an authorized EvaluationReceipt. |
| C36 | Two controllers enroll the same exclusive device. | Machine-local admission rejects the conflict. |
| C37 | Concurrent policy computations use one state revision. | One commit succeeds and conflicting outputs remain recorded. |
| C38 | Previously promoted evidence is invalidated. | Dependent eligibility is suspended and a correction observation is emitted. |
| C39 | Events arrive while a client recovers from a cursor gap. | Snapshot watermark and resumed stream lose no committed event. |
| C40 | Retained artifacts outlive a completed job. | Storage remains occupied until verified reclaim. |
| C41 | A child budget settles after repeated notifications. | Parent consumption and release occur once. |
| C42 | Python and another implementation hash edge-case fixtures. | Canonical payload digests match without self-reference. |

Every adapter must pass the applicable contract suite. Exceptions must be explicit capability exclusions.

### 26.2 Scientific tests

- Immutable evaluator tests with known outputs.
- Raw metric direction tests.
- No-op candidate distribution tests.
- Seed injection and provenance tests.
- Pairing and aggregation tests.
- Missing/censored result tests.
- False-promotion characterization under the chosen policy.
- Cross-backend evaluator parity tests where claimed.
- Data-split and leakage checks supplied by the project.
- Protected-evaluation exposure accounting.

A test that merely repeats the implementation is insufficient. Use independent expected outcomes and adversarial event ordering.

### 26.3 Hardware tests

Hardware-changing work runs on the relevant qualified platform.

Record exact source revision, environment, workload, dataset fixture, and device fingerprint.

Separate:

- Functional smoke.
- Numerical parity.
- Short performance regression.
- Full-budget research comparison.
- Multi-seed confirmation.
- Fleet failure injection.

A tiny fixture cannot satisfy a full-workload performance acceptance condition.

### 26.4 CI levels

| Trigger | Checks |
|---|---|
| Ordinary change | Formatting, typing, schema compatibility, targeted unit/contract tests. |
| Worker or state change | Subprocess lifecycle, crash/restart, and relevant concurrency tests. |
| Engine change | Relevant hardware smoke and numerical qualification. |
| Adapter change | Shared contract suite plus provider-specific replay/recovery tests. |
| Scheduled qualification | Full workload baselines, variance checks, and drift reports. |
| Release | Supported platform matrix, migration tests, artifact replay, and packaging. |

Do not run expensive full-budget GPU trials for every documentation change.

## 27. Performance and scale acceptance

### 27.1 Measure layers separately

**Scientific:** confirmed metric change, replication rate, held-out transfer, improvement per fixed budget.

**Execution:** throughput, optimizer steps, startup, compilation, input preparation, evaluation, memory, energy where measurable.

**Control:** queue delay, transaction latency, scheduling latency, API latency, event lag, recovery time, and controller resource use.

**Agent:** proposal latency, unique hypotheses, duplicate proposals, tokens, cost, critic overhead, and decision latency.

### 27.2 Independent-trial scaling

Define:

~~~text
parallel_efficiency(N) =
    completed_comparable_trials_per_hour(N)
    / (N * completed_comparable_trials_per_hour(1))
~~~

This equation applies to equivalent workers in a homogeneous hardware class.

For a heterogeneous fleet, define:

~~~text
heterogeneous_efficiency =
    observed_combined_completed_trial_rate
    / sum(isolated_completed_trial_rate_for_each_actual_worker)
~~~

Use the same eligible workload and cache assumptions for each isolated rate. If workloads are scientifically incomparable, report separate classes instead of one aggregate efficiency.

Measure on available qualified worker counts such as 1, 2, 4, and 8. Larger counts are useful only when the fleet and budget permit them.

Keep workload, evaluation, and failure treatment comparable. Report preparation and cache assumptions.

Near-linear scaling is a target for independent jobs with adequate inputs. It is not an implementation claim before measurement.

### 27.3 Controller benchmark

Use simulated workers to test:

- Claim contention.
- Heartbeat volume.
- Out-of-order completions.
- Event backlog.
- Budget contention.
- Slow artifact publication.
- Worker disappearance.
- Duplicate results.

Simulated scale establishes controller behavior. It does not establish real accelerator throughput.

Publish the benchmark envelope rather than an unsupported universal worker limit.

### 27.4 Cold and warm measurements

For each engine, distinguish:

~~~text
cold_end_to_end
warm_end_to_end
active_training_only
~~~

Use the same workload dimensions, precision semantics, effective batch, and evaluation contract when comparing engines.

Record thermal and power conditions where material. A faster step is not a scientific win if it changes numerical behavior enough to degrade the objective.

### 27.5 Swarm policy benchmark

Compare the swarm policy with sequential greedy and a simple parallel baseline.

Use equal total budgets, repeated runs, and the same initial knowledge. Include agent and orchestration costs.

Report failed and incomplete experiments. Do not compare only each method's best surviving run.

The success criterion is more trustworthy progress per budget. More simultaneous agents alone is not a success criterion.

## 28. Operational behavior and deployment

### 28.1 Local startup

A new local installation should:

1. Locate or create its runtime state directory.
2. Initialize the database and schema.
3. Bind a loopback API endpoint.
4. Register the local worker.
5. Probe supported execution profiles.
6. Load the project manifest.
7. Validate required artifacts and permissions.
8. Start the requested campaign.

It must not install every GPU framework, download a full dataset, or enroll remote machines without the corresponding project setup action.

Keep runtime state separate from source repositories and document archives. Use the platform's application-data conventions.

### 28.2 Process lifetime

Distinguish:

- Foreground controller lifetime.
- Managed background service lifetime.
- Campaign lifecycle.
- Attempt process lifetime.

A foreground command can attach to an existing service. Detaching the client does not cancel the Campaign.

If the controller actually exits, pending state remains durable. Workers follow their bounded offline policy. Report that control is unavailable until a service resumes.

Service installation and automatic startup are explicit operator choices. A library import must not install a daemon.

### 28.3 Health and readiness

Expose separate health signals:

- API process liveness.
- State-store readiness.
- Artifact-store readiness.
- Orchestration ownership.
- Worker connection and qualification.
- Queue progress.
- Unresolved resource reconciliation.

A healthy HTTP endpoint does not prove that experiments can run.

A device is schedulable only when its advertised capability, current reservation state, and required qualification allow it.

### 28.4 Backup and restore

Back up the authoritative store through its supported snapshot mechanism. For SQLite, use a consistent backup procedure rather than copying an arbitrary live database/WAL combination.

Record the artifact references required by the snapshot. Verify that restoration can resolve them.

Restore into a new inactive control epoch first. Reconcile live workers before allowing dispatch. A restored database can otherwise issue stale authority for processes that continued running.

Test restoration as part of release qualification. A successful backup command alone does not establish recoverability.

### 28.5 Availability progression

The first release provides durable local state, not a multi-controller high-availability cluster.

A later deployment can introduce:

- A transactional server database.
- An elected controller or partitioned authority.
- Durable object storage.
- Hosted orchestration.
- Reporting replicas.

Each step must preserve the same domain invariants. Multiple controller processes are not safe merely because they connect to the same database.

### 28.6 Provider configuration

Keep provider-specific options namespaced:

~~~toml
[orchestration]
driver = "local"

[state]
driver = "sqlite"

[artifacts]
driver = "filesystem"

[adapters.temporal]
namespace = "research"
task_queue = "research-control"

[adapters.cloudflare]
workspace_binding = "research-control"
artifact_binding = "research-artifacts"
~~~

Unused adapter blocks do not cause connections or provisioning.

Credential values remain outside this configuration. The runtime resolves authorized references.

### 28.7 Dependency graph semantics

Use a small acyclic job graph before designing a workflow language.

A Job declares predecessor IDs and required artifact outputs. It becomes ready when its required predecessors succeed and their artifacts verify.

Define dependency outcomes explicitly:

- Required predecessor failed: dependent job is blocked or skipped with a reason.
- Optional diagnostic predecessor failed: execution follows the declared policy.
- Predecessor cancelled: dependent work does not start by accident.
- Cached preparation reused: record the artifact and its original preparation identity.
- Partial output exists: it is ineligible unless the stage contract permits partial input.

Versioned templates can expand into this graph. Avoid arbitrary embedded expressions that create a second programming language.

## 29. Delivery roadmap

### 29.1 M0: freeze the research contract

Deliver:

- Canonical vocabulary.
- Experiment, worker, result, event, and error schemas.
- State transition rules.
- Budget and comparison semantics.
- Project manifest.
- Golden fixtures.
- A small CPU project.
- An upstream-compatible project example.

Exit criteria:

- The examples can be expressed without project-specific core code.
- Candidate, Trial, Attempt, and Evaluation identities are unambiguous.
- Raw metric direction is explicit.
- The same schema fixtures can be implemented by another language.
- The accepted scope excludes optional cloud services.

### 29.2 M1: complete local vertical slice

Deliver:

- Python command service.
- SQLite domain store and migrations.
- Filesystem artifacts.
- Local asyncio driver.
- Local worker and process journal.
- HTTP API, CLI, SDK, and bundled MCP.
- Greedy policy.
- JSON and Markdown reports.
- CPU fixture and one qualified accelerator path.

Exit criteria:

- An external agent creates and controls a campaign through MCP.
- The original sequential method can run through the project contract.
- A controller restart preserves pending work and accounting.
- A failed attempt cannot become the metric winner.
- Cancellation proves termination for the qualified platform.
- Closing a client does not incorrectly complete the campaign.

Agent control is part of M1. It is not deferred behind a dashboard.

### 29.3 M2: remote workers and actual swarm execution

Deliver:

- Authenticated outbound worker protocol.
- Resource and data-locality scheduling.
- Multiple isolated candidate workspaces.
- Asynchronous portfolio policy.
- Candidate population and atomic promotion.
- Early MPS/MLX qualification where available.
- Import and disposable replay for an existing research project.

Exit criteria:

- At least two independent agents submit concurrently.
- Multiple qualified workers finish out of order.
- Duplicate delivery changes no scientific state.
- A lost lease does not cause device reuse while execution remains unknown.
- A bounded existing-project campaign uses the generic interfaces.
- Existing active research remains under its original owner unless explicitly migrated.

The actual physical worker count depends on available, authorized hardware. Simulation supplements but does not replace native evidence.

### 29.4 M3: scientific confirmation and meta-optimization

Deliver:

- Fixed replication policies.
- Paired comparison support.
- Evaluation exposure accounting.
- Search-policy snapshots and bounded research memory.
- Child campaigns and hierarchical budgets.
- A policy benchmark with held-out tasks or starting states.

Exit criteria:

- Explicit seed injection reaches the workload.
- A no-op candidate suite characterizes false promotion.
- Incomplete panels are handled as declared.
- A child campaign cannot exceed parent authority by nesting.
- A policy comparison accounts for agent and execution costs.
- Reports separate observed, confirmed, protected, and official claims.

Do not claim policy improvement unless the benchmark produces evidence for it.

### 29.5 M4: Temporal adapter

Deliver:

- Durable dispatch/wait integration.
- Native-worker completion through the research API.
- Workflow history versioning and replay tests.
- Migration from a reconciled local checkpoint.

Exit criteria:

- The shared contract suite passes.
- Provider retries do not duplicate scientific execution.
- Provider cancellation does not falsely prove native termination.
- The same project and API operate without Temporal imports in project code.

### 29.6 M5: Cloudflare deployment profile

Deliver:

- API/MCP facade.
- Authoritative Durable Object implementation.
- R2 artifacts.
- Workflow integration.
- Optional reporting projection only if needed.
- Deployment and restore documentation.

Exit criteria:

- The same transaction and race tests pass.
- Native GPU workers remain external.
- Domain state outlives workflow-history retention.
- Artifacts and event payloads respect qualified limits.
- A quiescent ownership transfer fences the old driver.

Implementation does not imply authorization to deploy to an operator's Cloudflare account. Deployment requires its own concrete scope and authority.

### 29.7 M6: advanced search, learning, and specialized execution

Independent workstreams:

- Evolutionary and reflective policies.
- Actual policy-training adapter, with separate qualification for reward-driven RL.
- Optional decision-model integration.
- ROCm and additional supported hardware.
- ResourceGroup and distributed training.
- Kernel-level research.
- Optional UI.
- Rust worker prototype.

Each workstream needs its own measurable exit criterion. None should delay a working local engine.

### 29.8 Release positioning

The first public release should promise:

- Local research that works through an API.
- Multiple agents and registered workers.
- Reproducible experiment records.
- Explicit comparison and promotion.
- A demonstrated original-autoresearch example.
- A second ordinary-project adoption example.

Do not market every planned adapter, backend, or RL capability as shipped.

## 30. Implementation work packages

These packages are suitable for separate implementation tasks. They are not instructions to start implementation as part of this specification-writing task.

| Package | Deliverable | Depends on | Completion evidence |
|---|---|---|---|
| WP01 | Domain schemas and canonical serialization. | M0 decisions. | Golden fixture validation. |
| WP02 | SQLite transactions, events, and outbox. | WP01. | Concurrency and crash tests. |
| WP03 | Local artifact lifecycle. | WP01. | Corruption, staging, and publication tests. |
| WP04 | Local worker journal and supervision. | WP01, WP03. | Native start/cancel/reconcile proof. |
| WP05 | Project contract and stage graph. | WP01, WP04. | CPU and original-workload examples. |
| WP06 | Public API and authorization. | WP02, WP03. | Command equivalence and access tests. |
| WP07 | SDK, CLI, and MCP. | WP06. | External-agent vertical slice. |
| WP08 | Greedy search and report projection. | WP02, WP05. | Replayable campaign and report. |
| WP09 | Remote worker transport and scheduling. | WP04, WP06. | Multi-worker out-of-order execution. |
| WP10 | Portfolio, population, and promotion. | WP08, WP09. | Conflicting promotion and diversity tests. |
| WP11 | Replication and exposure policy. | WP10. | Scientific contract suite. |
| WP12 | Existing-project import and replay. | WP05 and WP07. WP11 precedes confirmed comparisons. | Receipt equivalence with scoped fixture. |
| WP13 | Hierarchical campaigns and policy benchmark. | WP11. | Bounded meta-run and accounting proof. |
| WP14 | Temporal adapter. | Stable WP02/WP09 contracts. | Shared conformance plus replay. |
| WP15 | Cloudflare adapter and storage profile. | Stable domain transactions. | Shared conformance plus hosted recovery. |
| WP16 | Learning adapter. | WP13 and qualified trajectories. | Declared training method, actual policy update, and held-out evaluation. RL claims also require reward-driven optimization. |
| WP17 | Rust worker feasibility. | Stable WP09 protocol and measured need. | Cross-language fixtures and cost comparison. |
| WP18 | Optional UI. | WP06 and reporting model. | API-only operational workflow. |

Parallel implementation should have one owner per package. Shared schema changes require coordination before dependent work proceeds.

Do not dispatch a large agent organization merely to implement this organization. Small bounded tasks with explicit outputs remain the default.

## 31. Assessment of the supplied deep research

### 31.1 Adopted ideas

- Preserve the small mutable experiment contract.
- Separate research payload from orchestration.
- Prefer independent experiments over distributed training.
- Use immutable evaluation and explicit provenance.
- Distinguish raw metrics from promotion.
- Preserve failures and retry lineage.
- Make hardware and runtime part of comparison evidence.
- Support MLX and PyTorch profiles.
- Use a headless engine with optional UI.
- Give agents bounded structured research context.
- Add reflective search before expensive policy training.
- Preserve population diversity and useful tradeoffs.
- Measure validated progress per unit of compute and wall time.

### 31.2 Revised recommendations

| Earlier suggestion | Final direction |
|---|---|
| Ray as the default orchestrator. | Local asyncio first, Temporal and Cloudflare adapters later. Ray remains optional. |
| A workload-plugin architecture for an existing research project. | Ordinary repository manifest and command interface. |
| Opik as an optional integration or optimizer runtime. | No Opik support. Borrow ideas only. |
| MLX treated as Apple-only. | Framework and device backend are separate. Apple is the first intended MLX target. |
| Multi-seed fork behavior accepted from descriptions. | Verify seed injection and evaluator semantics directly. |
| One benchmark hash containing every environmental detail. | Separate protocol, execution fingerprint, and declared comparison class. |
| More agents as an inherent improvement. | Compare research outcome and total cost against simple baselines. |
| Workflow history as durable research memory. | Domain ledger and artifacts remain authoritative. |
| A single “best” score. | Scoped best-observed, best-confirmed, incumbent, and population. |
| An eventual large framework tree. | Implement small complete vertical slices before optional abstractions. |

### 31.3 Fork and ecosystem reference inventory

| Reference | Useful idea | Adoption rule |
|---|---|---|
| Karpathy autoresearch | Minimal mutable payload and fixed-budget feedback. | Preserve an upstream-compatible mode and attribution. |
| miolini/autoresearch-macos | MPS adaptation and fallback patterns. | Qualify the exact implementation and workload. |
| trevin-creator/autoresearch-mlx | Native MLX implementation and confirmation-ledger ideas. | Audit seeds, optimizer, evaluator, and metric semantics. |
| jsegov/autoresearch-win-rtx | Consumer-GPU and Windows setup patterns. | Test native Windows process and numerical behavior. |
| andyluo7/autoresearch | Possible AMD-related work. | Do not assume an upstream label proves working ROCm support. |
| pi-autoresearch | Small agent tool surface and shared recorded state. | UX/API reference, not a required dependency. |
| pi-autoresearch-studio | Plan-to-experiment-to-review workflow. | UX reference, not proof of distributed correctness. |
| auto-log-research | Reporting that feeds later decisions. | Borrow the feedback-loop idea. |
| AlphaEvolve/FunSearch | Executable candidate generation and objective evolutionary selection. | Inspiration, not a claim of equivalent capability. |
| GEPA | Reflection over trajectories and policy/prompt evolution. | Optional search-policy pattern. |
| Recursive Language Models | Externalized, recursive context inspection. | Memory/retrieval pattern. |
| InstructGPT/DeepSeekMath | Policy-learning precedents and algorithms. | Add only through a qualified learning contract. |

Repository counts, stars, dependency versions, issue status, and provider quotas are volatile. They are not normative requirements here.

### 31.4 Platform facts to refresh during implementation

Refresh the exact supported versions of:

- Python, PyTorch, MLX, CUDA, ROCm, and drivers.
- Temporal SDKs.
- Cloudflare runtime and service limits.
- DirectML and ONNX Runtime execution providers.
- JAX platform support.
- PyTorch/XLA or successor lifecycle.
- MLX distributed topology requirements.
- CI runner capabilities.

The supplied report contains date-specific claims. Do not use an unverified announcement date or version number as a permanent architectural premise.

### 31.5 Naming

Use “Autoresearch Swarm” as this specification's working title.

Alternatives remain:

- Autoresearch Next while the project closely follows upstream.
- ResearchForge if it becomes a broader independent platform.
- Autoresearch Swarm if parallel research remains the defining identity.

Keep visible upstream attribution regardless of branding. Perform repository/package/name checks before publication.

## 32. Requirement traceability

| Requirement | Sections | Acceptance evidence |
|---|---|---|
| Local operation without cloud. | 5, 11, 16, 28, 29.2 | M1 vertical slice. |
| Uniform infrastructure primitives. | 5.3, 11.2, 16 | Shared adapter conformance. |
| Agent control out of the box. | 15, 29.2 | API/MCP campaign control. |
| Optional UI. | 22.4 | Campaign runs without UI process. |
| Ordinary-project onboarding. | 8, 23, 25 | Two repositories without core project imports. |
| Registered compute workers. | 13, 14 | Native multi-worker execution. |
| Parallel research swarm. | 17, 27.5, 29.3 | Concurrent proposals and out-of-order results. |
| Original autoresearch mode. | 3, 8, 29.1 | Upstream-compatible workload qualification. |
| No Opik integration. | 1, 6.3, 31.2 | Dependency and public API inspection. |
| Optional decision models. | 17.7 | Deterministic and external-agent paths work without them. |
| MLX and CUDA profiles. | 14 | Qualified real workload per claimed profile. |
| Durable resumption. | 10, 11.5, 13.4 | Crash/restart contract tests. |
| Correct unknown execution handling. | 10, 13.5 | C08-C11. |
| Statistical promotion. | 18 | Scientific qualification and C15-C17, C22-C24. |
| Hierarchical recursive loops. | 19, 20 | C18-C19 and bounded meta-run. |
| Actual RL path. | 20.4-20.6 | Declared RL algorithm, compatible rollouts, reward-driven update, and held-out evaluation. |
| Python-first, Rust-compatible. | 7.2, 24 | Golden cross-language fixtures. |
| Existing-project adoption. | 4, 23 | Historical import, fixture replay, then new campaign. |
| Research efficiency. | 19.5, 27 | Equal-budget policy comparison. |
| Failure preservation. | 6, 10, 18, 22 | Reports include all attempts and missingness. |

## 33. Open decisions and deferred work

These items do not block writing the core schemas:

1. Final name, package namespace, and repository location.
2. The exact first accelerator hardware used for qualification.
3. The first API framework and schema-model implementation.
4. Default practical-effect and replication settings for each project.
5. Whether the first Temporal adapter uses signals or asynchronous Activity completion.
6. The exact hosted authority partition once one workspace no longer fits one authority.
7. Which trainable policy and learning framework justify an RL adapter.
8. Whether measured deployment friction justifies a Rust worker.
9. Optional UI technology.
10. The optional decision-model provider and its qualified interface.

Do not turn these choices into prerequisites for the local CPU and agent-API vertical slice.

Before freezing a public v1 API, resolve:

- Canonical JSON edge-case fixtures and enforcement of the selected numeric constraints.
- Final field names and enum values.
- Authority scope for shared resources.
- Retry and unknown-execution accounting.
- Protocol compatibility and migration policy.
- Permission and artifact-access model.

## 34. Source and evidence register

### 34.1 Product requirements

The design requires local operation, uniform adapters, registered workers, complete agent APIs, optional UI, parallel research, durable evidence, and explicit promotion.

Research projects remain ordinary repositories. Decision models are optional. There is no Opik integration.

Python is the first implementation language. Language-neutral contracts permit independently qualified implementations and adapters.

### 34.2 Evidence publication

Publish only synthetic fixtures or explicitly public datasets and receipts. Keep private research source, data, runtime identity, and provenance outside public artifacts.

A public claim must link to evidence that proves its stated scope. Implementation, local verification, native/provider qualification, and scientific demonstration are separate states.

### 34.3 Original project and implementation references

- [Karpathy autoresearch](https://github.com/karpathy/autoresearch)
- [Original playbook](https://raw.githubusercontent.com/karpathy/autoresearch/master/program.md)
- [macOS fork](https://github.com/miolini/autoresearch-macos)
- [MLX fork](https://github.com/trevin-creator/autoresearch-mlx)
- [MLX gate source](https://raw.githubusercontent.com/trevin-creator/autoresearch-mlx/main/rigor.py)
- [MLX training source](https://raw.githubusercontent.com/trevin-creator/autoresearch-mlx/main/train.py)
- [Windows RTX fork](https://github.com/jsegov/autoresearch-win-rtx)
- [AMD-listed fork reference](https://github.com/andyluo7/autoresearch)

### 34.4 Infrastructure and language references

- [Python asyncio subprocesses](https://docs.python.org/3/library/asyncio-subprocess.html)
- [SQLite deployment guidance](https://www.sqlite.org/whentouse.html)
- [JSON Canonicalization Scheme, RFC 8785](https://www.rfc-editor.org/rfc/rfc8785)
- [Temporal Python asynchronous completion](https://docs.temporal.io/develop/python/activities/asynchronous-activity)
- [Temporal Python replay/testing](https://docs.temporal.io/develop/python/best-practices/testing-suite)
- [Temporal Rust development](https://docs.temporal.io/develop/rust)
- [Temporal Rust asynchronous Activity outcome](https://docs.rs/temporalio-sdk/latest/temporalio_sdk/activities/enum.ActivityError.html)
- [Temporal Rust asynchronous completion handle](https://docs.rs/temporalio-client/latest/temporalio_client/struct.AsyncActivityHandle.html)
- [Cloudflare Python Workflows](https://developers.cloudflare.com/workflows/python/)
- [Cloudflare Python runtime](https://developers.cloudflare.com/workers/languages/python/how-python-workers-work/)
- [Cloudflare Workflow events](https://developers.cloudflare.com/workflows/build/events-and-parameters/)
- [Cloudflare Workflow limits](https://developers.cloudflare.com/workflows/reference/limits/)
- [Cloudflare SQLite Durable Object storage](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/)
- [Cloudflare Queues pull consumers](https://developers.cloudflare.com/queues/configuration/pull-consumers/)
- [Tokio](https://tokio.rs/tokio/tutorial)
- [PyO3](https://pyo3.rs/)

### 34.5 Compute references

- [PyTorch](https://pytorch.org/)
- [PyTorch native frontend](https://docs.pytorch.org/cppdocs/frontend)
- [MLX documentation](https://ml-explore.github.io/mlx/build/html/index.html)
- [MLX distributed communication](https://ml-explore.github.io/mlx/build/html/usage/distributed.html)
- [MLX custom Metal kernels](https://ml-explore.github.io/mlx/build/html/dev/custom_metal_kernels.html)
- [Apple PyTorch acceleration](https://developer.apple.com/metal/pytorch/)
- [CUDA compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/)
- [ROCm documentation](https://rocm.docs.amd.com/)
- [DirectML PyTorch guidance](https://learn.microsoft.com/en-us/windows/ai/directml/pytorch-windows)
- [ONNX Runtime providers](https://onnxruntime.ai/docs/execution-providers/)
- [Triton](https://triton-lang.org/)
- [JAX installation](https://docs.jax.dev/en/latest/installation.html)

### 34.6 Research and interface inspiration

- [AlphaEvolve](https://deepmind.google/blog/alphaevolve-a-gemini-powered-coding-agent-for-designing-advanced-algorithms/)
- [FunSearch](https://deepmind.google/blog/funsearch-making-new-discoveries-in-mathematical-sciences-using-large-language-models/)
- [GEPA paper](https://arxiv.org/abs/2507.19457)
- [Recursive Language Models paper](https://arxiv.org/abs/2512.24601)
- [InstructGPT paper](https://arxiv.org/abs/2203.02155)
- [DeepSeekMath paper](https://arxiv.org/abs/2402.03300)
- [pi-autoresearch](https://github.com/davebcn87/pi-autoresearch)
- [pi-autoresearch-studio](https://github.com/jhochenbaum/pi-autoresearch-studio)
- [auto-log-research](https://github.com/ottogin/auto-log-research)
- [Opik source snapshot used for vocabulary comparison](https://github.com/comet-ml/opik/tree/412414d7c313d3a1b5cfa461ae8b5ebb62ab7201/sdks/opik_optimizer)

The final reference supplies design context only. It creates no Opik integration requirement.

## 35. Definition of specification completion

This document is complete when it:

- Captures the product requirements.
- Explains differences from original autoresearch.
- Defines local, worker, adapter, API, and search contracts.
- Covers scientific evidence and operational failure states.
- Describes an existing research project adoption without special platform coupling.
- Separates nested optimization from actual RL training.
- Records the Python-first recommendation and a credible Rust path.
- Supplies implementation stages and acceptance tests.
- Labels proposed interfaces and unproven capabilities.
- Preserves source and audit caveats.

This design is not proof of implementation or qualification. See the implementation status and reproducible test evidence.
