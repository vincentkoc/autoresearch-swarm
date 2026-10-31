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
shutil.copyfile(ROOT / "durable_sql.py", output / "durable_sql.py")
shutil.copyfile(ROOT / "storage_qualification.py", output / "storage_qualification.py")
shutil.copyfile(ROOT / "signing_qualification.py", output / "signing_qualification.py")
shutil.copyfile(ROOT / "witness_qualification.py", output / "witness_qualification.py")
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
