"""Persistent native storage selection without a hosted R2 account."""

import json
import shutil
from concurrent.futures import ThreadPoolExecutor

import pytest

import research_swarm.runtime as runtime_module
from research_swarm import ResearchClient
from research_swarm.cli import main
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.runtime import LocalRuntime, write_private
from research_swarm.storage.config import FilesystemConfig
from research_swarm.storage.r2 import R2BlobStore
from research_swarm.storage.r2_config import R2Config
from tests.test_r2_storage import ObjectSimulator


@pytest.fixture
def provider(monkeypatch):
    config = R2Config(
        endpoint="https://fixture.r2.cloudflarestorage.com",
        bucket="fixture-bucket",
        max_object_bytes=128 * 1024,
    )
    stores = {config.digest: ObjectSimulator(config)}

    def transport(selected):
        return stores.setdefault(selected.digest, ObjectSimulator(selected))

    monkeypatch.setattr(runtime_module, "S3R2Transport", transport)
    return config, stores


def snapshot(root):
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file() and path.name != "initialize.lock"
    }


def test_r2_init_cli_and_sdk_reopen_the_pinned_backend(provider, tmp_path, capsys):
    config, stores = provider
    root, selection = tmp_path / "runtime", tmp_path / "storage.json"
    selection.write_text(config.model_dump_json())
    args = ["--state-dir", str(root), "init", "--storage-config", str(selection)]
    assert main(args) == 0
    identity = json.loads(capsys.readouterr().out)
    assert identity["storage"] == config.model_dump(mode="json")
    assert identity["staging_id"]
    assert main(args) == 0
    assert json.loads(capsys.readouterr().out) == identity
    source = tmp_path / "source.bin"
    source.write_bytes(b"persistent storage")
    with ResearchClient.local(root) as client:
        reference = client.upload_file(source, idempotency_key="publish")
    with ResearchClient.local(root) as client:
        destination = tmp_path / "download.bin"
        assert client.download(reference.id, destination) == reference
        assert destination.read_bytes() == source.read_bytes()
        assert isinstance(client.transport.runtime.artifacts.store, R2BlobStore)
        assert client.transport.runtime.artifacts.max_object_bytes == config.max_object_bytes
    assert len(stores[config.digest].objects) == 1
    assert not (root / "artifacts").exists()
    before = snapshot(root)
    for changed in (FilesystemConfig(), config.model_copy(update={"prefix": "changed"})):
        with pytest.raises(ResearchError, match="immutable"):
            LocalRuntime.open(root, storage_config=changed)
        assert snapshot(root) == before


def test_r2_same_and_conflicting_initialization_under_contention(provider, tmp_path):
    config, _ = provider
    root = tmp_path / "same"
    with ThreadPoolExecutor(max_workers=4) as pool:
        runtimes = list(
            pool.map(lambda _: LocalRuntime.open(root, storage_config=config), range(4))
        )
    assert len({runtime.identity for runtime in runtimes}) == 1
    assert len({runtime.artifacts.store.binding for runtime in runtimes}) == 1
    root = tmp_path / "different"

    def initialize(selected):
        try:
            return LocalRuntime.open(root, storage_config=selected).identity
        except ResearchError as error:
            assert error.code == ErrorCode.IDEMPOTENCY_CONFLICT
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(initialize, (config, FilesystemConfig())))
    assert len([result for result in results if result]) == 1
    assert LocalRuntime.open(root).identity in results


@pytest.mark.parametrize("initialized", [False, True])
@pytest.mark.parametrize("missing", ["", "binding.json", "generations"])
def test_r2_missing_staging_never_recreates_even_during_initialization(
    provider, tmp_path, initialized, missing
):
    config, _ = provider
    root = tmp_path / "runtime"
    runtime = LocalRuntime.open(root, storage_config=config)
    if not initialized:
        identity = runtime.identity.model_copy(update={"initialized_at": None})
        write_private(root / "identity.json", identity.model_dump_json().encode())
    removed = root / "r2-staging" / missing
    if removed.is_dir():
        shutil.rmtree(removed)
    else:
        removed.unlink()
    before = snapshot(root)
    with pytest.raises((ResearchError, FileNotFoundError)):
        LocalRuntime.open(root)
    assert snapshot(root) == before
    assert not removed.exists()


@pytest.mark.parametrize("after_identity", [False, True])
def test_r2_initialization_crash_preserves_staging_custody(
    provider, tmp_path, monkeypatch, after_identity
):
    config, _ = provider
    root = tmp_path / "runtime"
    original = runtime_module.write_private

    def crash(path, content):
        if path.name == "identity.json":
            if after_identity:
                original(path, content)
            raise RuntimeError("initialization crash")
        original(path, content)

    monkeypatch.setattr(runtime_module, "write_private", crash)
    with pytest.raises(RuntimeError, match="initialization crash"):
        LocalRuntime.open(root, storage_config=config)
    assert (root / "r2-staging" / "binding.json").is_file()
    before = snapshot(root)
    monkeypatch.setattr(runtime_module, "write_private", original)
    if after_identity:
        recovered = LocalRuntime.open(root)
        assert recovered.identity.initialized_at is not None
        assert (root / "r2-staging" / "binding.json").read_bytes() == before[
            "r2-staging/binding.json"
        ]
    else:
        with pytest.raises(ResearchError, match="recovery is required"):
            LocalRuntime.open(root, storage_config=config)
        assert snapshot(root) == before


def test_r2_incomplete_creation_is_not_retried(provider, tmp_path, monkeypatch):
    config, _ = provider
    root = tmp_path / "runtime"

    def interrupted(cls, path, *args):
        path.mkdir(mode=0o700)
        raise RuntimeError("partial staging")

    monkeypatch.setattr(R2BlobStore, "initialize", classmethod(interrupted))
    with pytest.raises(RuntimeError, match="partial staging"):
        LocalRuntime.open(root, storage_config=config)
    with pytest.raises(ResearchError, match="recovery is required"):
        LocalRuntime.open(root, storage_config=config)
    assert not (root / "identity.json").exists()


def test_legacy_filesystem_identity_remains_compatible(provider, tmp_path):
    config, _ = provider
    root = tmp_path / "legacy"
    runtime = LocalRuntime.open(root)
    legacy = runtime.identity.model_dump(mode="json", exclude={"storage", "staging_id"})
    write_private(root / "identity.json", json.dumps(legacy).encode())
    assert LocalRuntime.open(root).identity.storage == FilesystemConfig()
    before = snapshot(root)
    with pytest.raises(ResearchError, match="immutable"):
        LocalRuntime.open(root, storage_config=config)
    assert snapshot(root) == before
    assert not (root / "r2-staging").exists()


def test_r2_dependency_and_credentials_preflight_before_state(tmp_path, monkeypatch):
    from research_swarm.storage import r2_transport

    config = R2Config(endpoint="https://fixture.r2.cloudflarestorage.com", bucket="fixture")
    root = tmp_path / "missing-sdk"

    def absent(name):
        raise ImportError("optional SDK absent")

    with monkeypatch.context() as patch:
        patch.setattr(r2_transport.importlib, "import_module", absent)
        with pytest.raises(ResearchError, match="optional s3"):
            LocalRuntime.open(root, storage_config=config)
    assert not root.exists()

    # Credential behavior is SDK-qualified separately; this constructor fails pre-state too.
    def missing_credentials(config):
        raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "credential sources unavailable")

    monkeypatch.setattr(runtime_module, "S3R2Transport", missing_credentials)
    with pytest.raises(ResearchError, match="credential sources"):
        LocalRuntime.open(root, storage_config=config)
    assert not root.exists()
