"""Native controller against a real local R2 binding, not a hosted S3 endpoint."""

import io
import json
from contextlib import contextmanager
from urllib.parse import quote

import httpx
import pytest

import research_swarm.runtime as runtime_module
from research_swarm import ResearchClient
from research_swarm.domain.artifacts import StoredBlob
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.storage.r2_config import R2Config
from research_swarm.storage.r2_transport import ObjectAbsent, RemoteObject, unavailable
from tests.adapters.cloudflare.test_cloudflare_native import build_current_sources, server
from tests.adapters.cloudflare.test_cloudflare_native import pytestmark as pytestmark
from tests.storage.test_artifacts import publish, stage
from tests.storage.test_r2_storage import occupied


class BindingTransport:
    """Test-only wire bridge. Conditional writes execute inside the real R2 binding."""

    def __init__(self, config, client):
        self.configuration_digest, self.client = config.digest, client
        self.lose_payload = self.lose_fence = self.defer_payload = False
        self.pending = []

    @contextmanager
    def get(self, key):
        response = self.client.get("/object/" + quote(key, safe=""))
        if response.status_code == 404:
            raise ObjectAbsent
        response.raise_for_status()
        with io.BytesIO(response.content) as body:
            yield RemoteObject(
                body,
                int(response.headers["content-length"]),
                json.loads(response.headers["x-object-metadata"]),
            )

    def put(self, key, body, size_bytes, metadata, *, if_absent):
        content = body.read()
        assert len(content) == size_bytes
        if if_absent and self.defer_payload:
            self.pending.append((key, content, dict(metadata)))
            raise unavailable()
        headers = {"x-object-metadata": json.dumps(dict(metadata))}
        if if_absent:
            headers["if-none-match"] = "*"
        response = self.client.put(
            "/object/" + quote(key, safe=""), content=content, headers=headers
        )
        if response.status_code == 412:
            return False
        response.raise_for_status()
        if (if_absent and self.lose_payload) or (not if_absent and self.lose_fence):
            raise unavailable()
        return True

    def complete_pending(self):
        self.defer_payload = False
        for key, content, metadata in self.pending:
            assert not self.put(key, io.BytesIO(content), len(content), metadata, if_absent=True)
        self.pending.clear()


def test_r2_binding_publication_restart_pins_and_delayed_write_fencing(tmp_path, monkeypatch):
    build_current_sources()
    config = R2Config(endpoint="https://fixture.r2.cloudflarestorage.com", bucket="fixture")
    emulator, root = tmp_path / "emulator", tmp_path / "runtime"
    emulator.mkdir()
    options = {
        "variables": {"QUALIFICATION": "local-r2-binding"},
        "config": "wrangler.r2-test.jsonc",
    }
    with (
        server(emulator, sequence=1, **options) as url,
        httpx.Client(base_url=url, trust_env=False, timeout=30) as client,
    ):
        transport = BindingTransport(config, client)
        monkeypatch.setattr(runtime_module, "S3R2Transport", lambda selected: transport)
        runtime = LocalRuntime.open(root, storage_config=config)
        actor = runtime.principal()

        def context(key=None, revision=None):
            return CommandContext(
                principal=actor, idempotency_key=key or new_id(), expected_revision=revision
            )

        upload = stage(runtime.artifacts, context, b"restart payload")
        transport.lose_payload = True
        with pytest.raises(ResearchError):
            runtime.artifacts.finalize_upload(context("lost-publish"), upload)
        assert occupied(runtime.artifacts, context) == 15
        identity = runtime.identity

    with (
        server(emulator, sequence=2, **options) as url,
        httpx.Client(base_url=url, trust_env=False, timeout=30) as client,
    ):
        transport = BindingTransport(config, client)
        runtime = LocalRuntime.open(root)
        assert runtime.identity == identity
        service, store = runtime.artifacts, runtime.artifacts.store
        result = service.finalize_upload(context("lost-publish"), upload)
        assert result["result"]["data"]["storage_generation"] == upload
        record = service.get(context(), result["result"]["id"])
        with service.reader(context(), record.reference) as source:
            assert source.read() == b"restart payload"
            with pytest.raises(ResearchError):
                service.delete(context(revision=1), record.reference.id, "pinned")

        # Native SDK follows the persisted selection without a storage-specific command.
        file = tmp_path / "sdk.bin"
        file.write_bytes(b"sdk bytes")
        with ResearchClient.local(root) as sdk:
            reference = sdk.upload_file(file, idempotency_key="sdk-upload")
            sdk.download(reference.id, tmp_path / "download.bin")
            assert (tmp_path / "download.bin").read_bytes() == file.read_bytes()

        pending = stage(service, context, b"delayed")
        transport.defer_payload = True
        with pytest.raises(ResearchError):
            service.finalize_upload(context(), pending)
        service.abort_upload(context(), pending)
        transport.complete_pending()  # Real conditional R2 PUT loses against the marker.

        first = publish(service, context, b"dedup")
        assert publish(service, context, b"dedup") == first
        transport.lose_fence = True
        with pytest.raises(ResearchError):
            service.delete(context(revision=1), first.reference.id, "replace")
        assert service.get(context(), first.reference.id).state == "deleting"
        charged = occupied(service, context)
        transport.lose_fence = False
        service.reconcile_deletion(context(), first.reference.id)
        assert occupied(service, context) == charged - 5
        replacement = publish(service, context, b"dedup")
        store.delete(
            first.workspace_id,
            StoredBlob(reference=first.reference, generation=first.storage_generation),
        )
        with service.reader(context(), replacement.reference) as source:
            assert source.read() == b"dedup"

        blob = StoredBlob(
            reference=replacement.reference, generation=replacement.storage_generation
        )
        transport.put(
            store._key(blob), io.BytesIO(b"wrong"), 5, store._metadata(blob), if_absent=False
        )
        with (
            pytest.raises(ResearchError) as corrupt,
            service.reader(context(), replacement.reference),
        ):
            pytest.fail("Corrupt remote bytes reached the reader")
        assert corrupt.value.code == ErrorCode.ARTIFACT_UNVERIFIED
        assert list((store.root / "readers").iterdir()) == []
