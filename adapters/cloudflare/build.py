"""Build an isolated local-provider bundle from the actual shared domain package."""

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
package = output / "research_swarm"
package.mkdir()
source = ROOT.parents[1] / "src/research_swarm"
shutil.copyfile(source / "__init__.py", package / "__init__.py")
shutil.copytree(source / "domain", package / "domain", ignore=shutil.ignore_patterns("__pycache__"))
