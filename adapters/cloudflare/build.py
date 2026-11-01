"""Build separate production and local-qualification bundles from shared source."""

import argparse
import ast
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SOURCE = ROOT.parents[1] / "src/research_swarm"
SOURCES = {
    "delivery/delivery_registry.py": "delivery_registry.py",
    "delivery/delivery_router.py": "delivery_router.py",
    "delivery/orchestration.py": "orchestration.py",
    "delivery/startup.py": "startup.py",
    "delivery/transfers.py": "transfers.py",
    "delivery/wait_delivery.py": "wait_delivery.py",
    "runtime/atomic.py": "atomic.py",
    "runtime/deployment.py": "deployment.py",
    "runtime/durable_sql.py": "durable_sql.py",
    "runtime/ed25519.py": "ed25519.py",
    "runtime/exclusive.py": "exclusive.py",
    "runtime/http_binding.py": "http_binding.py",
    "runtime/http_io.mjs": "http_io.mjs",
    "runtime/provider_management.py": "provider_management.py",
    "runtime/provisioning.py": "provisioning.py",
    "runtime/r2_binding.py": "r2_binding.py",
    "runtime/r2_io.mjs": "r2_io.mjs",
    "runtime/runtime.py": "runtime.py",
    "runtime/witness_binding.py": "witness_binding.py",
    "runtime/witness_io.mjs": "witness_io.mjs",
    "qualification/fixture_transport.mjs": "fixture_transport.mjs",
    "qualification/native_router_qualification.py": "native_router_qualification.py",
    "qualification/r2_qualification.py": "r2_qualification.py",
    "qualification/router_qualification.py": "router_qualification.py",
    "qualification/signing_qualification.py": "signing_qualification.py",
    "qualification/storage_qualification.py": "storage_qualification.py",
    "qualification/witness_qualification.py": "witness_qualification.py",
    "recovery/activation_cut.py": "activation_cut.py",
    "recovery/activation_history.py": "activation_history.py",
    "recovery/checkpoint_activation.py": "checkpoint_activation.py",
    "recovery/checkpoint_admission.py": "checkpoint_admission.py",
    "recovery/checkpoint_anchor.py": "checkpoint_anchor.py",
    "recovery/checkpoint_artifacts.py": "checkpoint_artifacts.py",
    "recovery/checkpoint_capture.py": "checkpoint_capture.py",
    "recovery/checkpoint_closure.py": "checkpoint_closure.py",
    "recovery/checkpoint_election.py": "checkpoint_election.py",
    "recovery/checkpoint_history.py": "checkpoint_history.py",
    "recovery/checkpoint_import.py": "checkpoint_import.py",
    "recovery/checkpoint_requalification.py": "checkpoint_requalification.py",
    "recovery/checkpoint_routes.py": "checkpoint_routes.py",
    "recovery/checkpoint_runtime.py": "checkpoint_runtime.py",
    "recovery/checkpoint_schema.py": "checkpoint_schema.py",
    "recovery/checkpoint_source.py": "checkpoint_source.py",
    "recovery/checkpoint_target.py": "checkpoint_target.py",
    "recovery/checkpoint_turns.py": "checkpoint_turns.py",
    "recovery/checkpoint_wire.py": "checkpoint_wire.py",
    "recovery/recovery_router.py": "recovery_router.py",
    "recovery/runtime_history.py": "runtime_history.py",
    "research/artifact_router.py": "artifact_router.py",
    "research/evaluations.py": "evaluations.py",
    "research/hosted_http.py": "hosted_http.py",
    "research/lifecycle.py": "lifecycle.py",
    "research/reconciliation.py": "reconciliation.py",
    "research/research_admission.py": "research_admission.py",
    "research/research_router.py": "research_router.py",
    "research/result_publication.py": "result_publication.py",
    "research/scientific.py": "scientific.py",
    "research/worker_inputs.py": "worker_inputs.py",
    "research/worker_outputs.py": "worker_outputs.py",
}
COMMON = (
    "atomic.py",
    "exclusive.py",
    "witness_io.mjs",
    "witness_binding.py",
    "durable_sql.py",
    "research_router.py",
    "research_admission.py",
    "worker_inputs.py",
    "worker_outputs.py",
    "result_publication.py",
    "reconciliation.py",
    "evaluations.py",
    "lifecycle.py",
    "scientific.py",
    "orchestration.py",
    "delivery_router.py",
    "wait_delivery.py",
    "transfers.py",
    "startup.py",
    "artifact_router.py",
    "hosted_http.py",
    "http_binding.py",
    "http_io.mjs",
    "r2_binding.py",
    "r2_io.mjs",
    "ed25519.py",
)
QUALIFICATION = (
    "fixture_transport.mjs",
    "storage_qualification.py",
    "signing_qualification.py",
    "witness_qualification.py",
    "router_qualification.py",
    "native_router_qualification.py",
    "r2_qualification.py",
)
PRODUCTION = (
    "deployment.py",
    "provisioning.py",
    "runtime.py",
    "delivery_registry.py",
    "checkpoint_schema.py",
    "checkpoint_anchor.py",
    "checkpoint_election.py",
    "checkpoint_capture.py",
    "checkpoint_import.py",
    "checkpoint_history.py",
    "checkpoint_wire.py",
    "checkpoint_source.py",
    "checkpoint_target.py",
    "checkpoint_admission.py",
    "checkpoint_turns.py",
    "checkpoint_artifacts.py",
    "checkpoint_routes.py",
    "checkpoint_closure.py",
    "checkpoint_runtime.py",
    "checkpoint_requalification.py",
    "provider_management.py",
    "checkpoint_activation.py",
    "activation_cut.py",
    "activation_history.py",
    "runtime_history.py",
    "recovery_router.py",
)
TARGETS = {
    "qualification": (".generated", "local-qualification-v1\n", "entry.py", QUALIFICATION),
    "production": (".generated-production", "production-v1\n", "production.py", PRODUCTION),
}


def command_names():
    catalog = ast.parse((SOURCE / "control/dispatch.py").read_text())
    definitions = [
        node.value
        for node in catalog.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "COMMANDS" for target in node.targets)
    ]
    if len(definitions) != 1 or not isinstance(definitions[0], ast.Dict):
        raise RuntimeError("Command catalog must remain one explicit dictionary")
    names = [ast.literal_eval(key) for key in definitions[0].keys if key is not None]
    if len(names) != len(definitions[0].keys) or any(not isinstance(name, str) for name in names):
        raise RuntimeError("Command catalog must contain explicit command names")
    return sorted(names)


def build(target="qualification", *, output=None):
    if target not in TARGETS:
        raise ValueError("Unknown Cloudflare build target")
    directory, ownership, entry, additions = TARGETS[target]
    sources = {name: ROOT / path for path, name in SOURCES.items()}
    if len(sources) != len(SOURCES):
        raise RuntimeError("Duplicate generated Cloudflare module name")
    output = ROOT / directory if output is None else Path(output)
    marker = output / ".research-cloudflare-build"
    names = command_names()
    if output.is_symlink():
        raise RuntimeError("Refusing to replace a linked build directory")
    if output.exists():
        if marker.is_symlink() or not marker.is_file() or marker.read_text() != ownership:
            raise RuntimeError("Refusing to replace an unowned or different-target build directory")
        shutil.rmtree(output)
    output.mkdir()
    marker.write_text(ownership)
    shutil.copyfile(ROOT / entry, output / "entry.py")
    for name in (*COMMON, *additions):
        shutil.copyfile(sources[name], output / name)
    shutil.copytree(SOURCE, output / "research_swarm", ignore=shutil.ignore_patterns("__pycache__"))
    (output / "commands.json").write_text(json.dumps(names) + "\n")
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=tuple(TARGETS), default="qualification")
    build(parser.parse_args().target)
