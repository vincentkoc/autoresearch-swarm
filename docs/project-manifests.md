# Project manifests

A research repository declares its commands and evaluation contract in
`research.toml`. It does not inherit a platform class or install a project-specific
plugin. The parser and immutable-bundle resolver are implemented. Campaign execution
and the CLI remain in progress.

The following template declares a training command and a separate evaluator.
The repository supplies the named scripts, dataset manifest, and dependency lock.

~~~toml
schema_version = "1.0"

[project]
slug = "example"
name = "Example research"

[source]
mode = "git"
base_revision = "HEAD"
paths = ["*.py", "*.json", "*.lock", "research.toml"]

[mutation]
allow = ["train.py"]
deny = ["evaluate.py"]

[environment]
profile = "python-cpu"
lock_path = "requirements.lock"
network_policy = "inherit"

[data]
manifest_path = "data-manifest.json"

[evaluation]
protected_paths = ["evaluate.py"]
comparison_class = "example-v1"
measurement_trust = "authenticated"

[[evaluation.metrics]]
name = "loss"
unit = "squared_error"
direction = "minimize"

[timing]
budget_ms = 300000
scope = "active_training"
timer_authority = "project_reported"

[[stages]]
id = "train"
kind = "train"
argv = ["python", "train.py"]
timeout_ms = 600000
output_names = ["model"]

[[stages]]
id = "evaluate"
kind = "evaluate"
argv = ["python", "evaluate.py"]
dependencies = ["train"]
required_artifacts = ["model"]
timeout_ms = 60000
~~~

## Resolution and comparison

`load_manifest` parses the local template. `resolve_project` reads the captured
manifest and protected files from an immutable source bundle. Working-tree edits
after capture cannot change the resolved contract.

Resolution produces separate source, manifest, dataset, evaluator, environment,
and protocol identities. Changing an allowed training file changes source identity.
It does not change the protocol identity. Changing the evaluator or its protected
dependencies changes the protocol identity.

Evaluator command arguments, declared inputs and outputs, dependencies, and timeout
also contribute to evaluator identity. Project names, descriptions, comments, and
source capture revisions do not change scientific identity. Environment profiles
remain part of the separate execution contract.

The project manifest, dataset manifest, and declared dependency lock are always
protected. Every protected pattern must match captured source. The caller supplies
the full evaluator dependency closure, including imported helpers when needed.
Resolution verifies these files against their source hashes.

The campaign must preserve its original resolved contract when evaluating a new
candidate. Validate the candidate against that contract's mutation policy and
protected paths. Do not accept an edited candidate manifest as a new authority.

## Requirements and claims

`HEAD` is a capture-time input. Git capture resolves it to a complete commit ID.
Execution uses the resulting source bundle and immutable identities.

Timing distinguishes `active_training` from `execution`. The former requires a
training stage. `timer_authority` declares the required timing contract. A manifest
claiming a trusted harness does not create or qualify that harness.

Similarly, `measurement_trust = "protected"` requests a protected evaluator.
It does not prove protected execution. Worker and evaluator admission must reject
unavailable requirements. `network_policy = "denied"` also requires enforcement.
The local trusted profile can explicitly inherit the operator's network policy.

Stage commands are argument arrays. Stage dependencies form an acyclic graph.
The shared stage validator rejects duplicate IDs, unknown predecessors, and cycles.
Commands, resource profiles, and declared outputs still require execution validation.

Stage definitions inherit `environment.profile` unless their
`resources.engine_profile` explicitly overrides it. This includes Python model
objects whose profile was omitted. Serializing and parsing a resolved manifest
preserves each stage's selected profile.
