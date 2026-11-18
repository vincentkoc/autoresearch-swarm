"""Exercise the real Linux mount contract in a disposable mount namespace."""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.skipif(sys.platform != "linux", reason="Linux read-only bind mount")
def test_linux_worker_mount_and_gc(tmp_path: Path) -> None:
    if not shutil.which("unshare"):
        pytest.skip("mount namespace tools unavailable")
    namespace = ["unshare", "--user", "--map-root-user", "--mount", "--propagation", "private"]
    if subprocess.run([*namespace, "true"], capture_output=True).returncode:
        pytest.skip("unprivileged mount namespaces unavailable")
    program = """
import errno, subprocess, sys
from pathlib import Path
sys.path.insert(0, sys.argv[2])
from research_swarm.swarm.campaign_gc import _stages
from research_swarm.swarm.data_stage import hydrate, mount_readonly, prepare

root = Path(sys.argv[1])
source = root / 'source'
source.mkdir()
(source / 'row').write_bytes(b'42')
prepare(source, root / 'volume', 'demo', 'v1')
data = hydrate(root / 'volume', root / 'nvme', 'demo', 'v1')
target = root / 'mount'
mount_readonly(data, target)
mount_readonly(data, target)
assert (target / 'row').read_bytes() == b'42'
try:
    (target / 'new').touch()
except OSError as exc:
    assert exc.errno == errno.EROFS, exc
else:
    raise AssertionError('worker wrote to the data mount')
assert _stages('demo', [root / 'nvme'], set(), True)[0].action == 'mounted'
subprocess.run(['umount', '--', str(target)], check=True)
assert _stages('demo', [root / 'nvme'], set(), True)[0].action == 'removed'
"""
    import research_swarm

    package_root = Path(research_swarm.__file__).resolve().parents[1]
    subprocess.run(
        [
            *namespace,
            sys.executable,
            "-c",
            program,
            str(tmp_path),
            str(package_root),
        ],
        check=True,
        timeout=30,
    )
