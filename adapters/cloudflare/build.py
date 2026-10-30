"""Build an isolated local-provider bundle from the actual shared package."""

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
shutil.copyfile(ROOT / "ed25519.py", output / "ed25519.py")
package = output / "research_swarm"
source = ROOT.parents[1] / "src/research_swarm"
shutil.copytree(source, package, ignore=shutil.ignore_patterns("__pycache__"))
