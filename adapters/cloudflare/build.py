"""Build an isolated local-provider bundle from the actual shared package."""

import ast
import json
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
output = ROOT / ".generated"
marker = output / ".research-cloudflare-build"
if output.exists():
    if not marker.is_file() or marker.read_text() != "local-qualification-v1\n":
        raise RuntimeError("Refusing to replace an unowned build directory")
    shutil.rmtree(output)
output.mkdir()
marker.write_text("local-qualification-v1\n")
shutil.copyfile(ROOT / "entry.py", output / "entry.py")
shutil.copyfile(ROOT / "atomic.py", output / "atomic.py")
shutil.copyfile(ROOT / "exclusive.py", output / "exclusive.py")
shutil.copyfile(ROOT / "fixture_transport.mjs", output / "fixture_transport.mjs")
shutil.copyfile(ROOT / "witness_io.mjs", output / "witness_io.mjs")
shutil.copyfile(ROOT / "witness_binding.py", output / "witness_binding.py")
shutil.copyfile(ROOT / "durable_sql.py", output / "durable_sql.py")
shutil.copyfile(ROOT / "storage_qualification.py", output / "storage_qualification.py")
shutil.copyfile(ROOT / "signing_qualification.py", output / "signing_qualification.py")
shutil.copyfile(ROOT / "witness_qualification.py", output / "witness_qualification.py")
shutil.copyfile(ROOT / "research_router.py", output / "research_router.py")
shutil.copyfile(ROOT / "research_admission.py", output / "research_admission.py")
shutil.copyfile(ROOT / "worker_inputs.py", output / "worker_inputs.py")
shutil.copyfile(ROOT / "worker_outputs.py", output / "worker_outputs.py")
shutil.copyfile(ROOT / "result_publication.py", output / "result_publication.py")
shutil.copyfile(ROOT / "evaluations.py", output / "evaluations.py")
shutil.copyfile(ROOT / "lifecycle.py", output / "lifecycle.py")
shutil.copyfile(ROOT / "scientific.py", output / "scientific.py")
shutil.copyfile(ROOT / "orchestration.py", output / "orchestration.py")
shutil.copyfile(ROOT / "delivery_router.py", output / "delivery_router.py")
shutil.copyfile(ROOT / "transfers.py", output / "transfers.py")
shutil.copyfile(ROOT / "artifact_router.py", output / "artifact_router.py")
shutil.copyfile(ROOT / "hosted_http.py", output / "hosted_http.py")
shutil.copyfile(ROOT / "http_binding.py", output / "http_binding.py")
shutil.copyfile(ROOT / "http_io.mjs", output / "http_io.mjs")
shutil.copyfile(ROOT / "router_qualification.py", output / "router_qualification.py")
shutil.copyfile(ROOT / "native_router_qualification.py", output / "native_router_qualification.py")
shutil.copyfile(ROOT / "r2_binding.py", output / "r2_binding.py")
shutil.copyfile(ROOT / "r2_io.mjs", output / "r2_io.mjs")
shutil.copyfile(ROOT / "r2_qualification.py", output / "r2_qualification.py")
shutil.copyfile(ROOT / "ed25519.py", output / "ed25519.py")
package = output / "research_swarm"
source = ROOT.parents[1] / "src/research_swarm"
shutil.copytree(source, package, ignore=shutil.ignore_patterns("__pycache__"))

catalog = ast.parse((source / "control/dispatch.py").read_text())
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
(output / "commands.json").write_text(json.dumps(sorted(names)) + "\n")
