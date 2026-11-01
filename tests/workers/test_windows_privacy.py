from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from research_swarm.domain.errors import ResearchError
from research_swarm.runtime import LocalRuntime
from research_swarm.state.private_files import read_private, write_private
from research_swarm.state.windows_privacy import (
    create_private_descriptor,
    current_user_sid,
    private_directory,
    verified_path,
    verify_descriptor,
)
from research_swarm.workers.local import LocalWorker
from tests.integration.test_local_pipeline import pipeline as pipeline

pytestmark = pytest.mark.skipif(sys.platform != "win32", reason="Native Windows ACL qualification")


def test_private_root_and_credentials_have_native_protection(tmp_path: Path) -> None:
    root = tmp_path / "private-runtime"
    with private_directory(root):
        path = root / "credential"
        descriptor = create_private_descriptor(path)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(b"synthetic credential")
            stream.flush()
            verify_descriptor(stream.fileno())
        with path.open("rb") as stream:
            verify_descriptor(stream.fileno())
            assert stream.read() == b"synthetic credential"
    with private_directory(root), verified_path(path, directory=False, protected=True):
        pass


@pytest.mark.parametrize("target", ["root", "credential"])
def test_broad_existing_acl_is_rejected_without_repair(tmp_path: Path, target: str) -> None:
    root = tmp_path / "reject-broad"
    with private_directory(root):
        path = root / "credential"
        with os.fdopen(create_private_descriptor(path), "wb") as stream:
            stream.write(b"fixture")
    selected = root if target == "root" else path
    subprocess.run(
        ["icacls", str(selected), "/grant", "*S-1-1-0:(R)"], check=True, capture_output=True
    )
    with (
        pytest.raises(ResearchError, match="another account"),
        verified_path(selected, directory=target == "root", protected=True),
    ):
        pass
    # Verification does not replace the descriptor or alter user-chosen access.
    result = subprocess.run(["icacls", str(selected)], check=True, capture_output=True)
    assert b"(R)" in result.stdout


@pytest.mark.parametrize("defect", ["inheritance", "system"])
def test_private_directory_requires_propagating_owner_and_system(
    tmp_path: Path, defect: str
) -> None:
    root = tmp_path / "incomplete-acl"
    with private_directory(root):
        pass
    if defect == "inheritance":
        subprocess.run(
            ["icacls", str(root), "/remove:g", "*" + current_user_sid()],
            check=True,
            capture_output=True,
        )
    arguments = (
        ["/grant", "*" + current_user_sid() + ":(F)"]
        if defect == "inheritance"
        else ["/remove:g", "*S-1-5-18"]
    )
    subprocess.run(["icacls", str(root), *arguments], check=True, capture_output=True)
    try:
        with private_directory(root):
            pass
    except ResearchError as error:
        assert "full control" in str(error)
    else:
        observed = subprocess.run(["icacls", str(root)], check=True, capture_output=True)
        pytest.fail("Incomplete ACL was accepted: " + observed.stdout.decode(errors="replace"))


def test_runtime_rejects_broad_token_before_database_mutation(tmp_path: Path) -> None:
    root = tmp_path / "broad-operator"
    LocalRuntime.open(root)
    before = (root / "research.sqlite").read_bytes()
    subprocess.run(
        ["icacls", str(root / "operator.token"), "/grant", "*S-1-1-0:(R)"],
        check=True,
        capture_output=True,
    )
    with pytest.raises(ResearchError, match="another account"):
        LocalRuntime.open(root)
    assert (root / "research.sqlite").read_bytes() == before


def test_atomic_private_replacement_retains_owner_and_system_acl(tmp_path: Path) -> None:
    root = tmp_path / "replace-private"
    with private_directory(root):
        path = root / "custody.json"
        write_private(path, b"original custody")
        write_private(path, b"elected target")
        assert read_private(root, path.name, limit=256) == b"elected target"
        with verified_path(path, directory=False, protected=True):
            pass
        assert {entry.name for entry in root.iterdir()} == {path.name}


def test_worker_lock_directory_is_private_and_broad_existing_acl_is_not_repaired(pipeline):
    worker = pipeline.worker
    locks = worker.root / "locks"
    with verified_path(locks, directory=True, protected=True):
        pass
    LocalWorker(
        worker.client, worker.worker, root=worker.root, machine_registry=worker.resources.path
    )
    subprocess.run(
        ["icacls", str(locks), "/grant", "*S-1-1-0:(R)"], check=True, capture_output=True
    )
    with pytest.raises(ResearchError, match="another account"):
        LocalWorker(
            worker.client, worker.worker, root=worker.root, machine_registry=worker.resources.path
        )
    observed = subprocess.run(["icacls", str(locks)], check=True, capture_output=True)
    assert b"(R)" in observed.stdout
