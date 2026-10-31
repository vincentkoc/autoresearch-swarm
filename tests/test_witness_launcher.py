"""Pinned native launch admission and real single-process epoch lifecycle."""

import json
import os
import socket
import ssl
import subprocess
import sys
import time
from dataclasses import replace

import httpx
import pytest
from cryptography.hazmat.primitives import serialization

from research_swarm import cli, witness_launcher
from research_swarm.api.time_witnesses import witness_consumer_digest
from research_swarm.control.native_time_witnesses import NativeWitnessSource
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.private_files import write_private
from research_swarm.witness_launcher import (
    MAX_CONFIGURATION_BYTES,
    WitnessServerConfiguration,
    configured_source,
    load_configuration,
    serve_witness,
)
from tests.test_http_delivery import serving, tls_certificate
from tests.test_native_time_witnesses import ControlledClock, pending
from tests.test_time_witness_http import TOKEN, headers
from tests.test_time_witnesses import SEED, identifier, scope


@pytest.fixture
def files(tmp_path):
    native = NativeWitnessSource(scope().authority, identifier(1), SEED)
    config = WitnessServerConfiguration(
        authority=native.authority,
        source=native.source,
        consumer_digest=witness_consumer_digest(TOKEN),
    )
    config_path, key_path = tmp_path / "witness.json", tmp_path / "witness.key"
    config_path.write_bytes(canonical_bytes(config.model_dump(mode="json")))
    write_private(key_path, SEED)
    return config_path, key_path, config, native


@pytest.mark.parametrize(
    "change", ["wrong-key", "short-key", "profile", "extra", "duplicate", "large", "listener"]
)
def test_invalid_inputs_refuse_before_server_creation(files, monkeypatch, change):
    config_path, key_path, config, _ = files
    raw = config.model_dump(mode="json")
    if change in {"wrong-key", "short-key"}:
        write_private(key_path, b"x" * (32 if change == "wrong-key" else 31))
    elif change == "profile":
        raw["source"]["native_profile"] = "unqualified-native-clock"
    elif change == "extra":
        raw["unexpected"] = "synthetic-sensitive-configuration"
    elif change == "listener":
        raw["host"] = "0.0.0.0"
    if change == "duplicate":
        config_path.write_text('{"host":"synthetic-sensitive-configuration","host":"other"}')
    elif change == "large":
        config_path.write_bytes(b"x" * (MAX_CONFIGURATION_BYTES + 1))
    else:
        config_path.write_bytes(canonical_bytes(raw))
    monkeypatch.setattr(
        witness_launcher.uvicorn, "run", lambda *a, **k: pytest.fail("Listener opened")
    )
    with pytest.raises(ResearchError) as refused:
        serve_witness(config_path, key_path)
    assert "synthetic-sensitive-configuration" not in str(refused.value)
    assert SEED.hex() not in str(refused.value)


@pytest.mark.skipif(os.name == "nt", reason="POSIX mode refusal; Windows uses native ACL admission")
def test_broad_secret_permissions_refuse_without_listener(files, monkeypatch):
    config_path, key_path, _, _ = files
    key_path.chmod(0o644)
    monkeypatch.setattr(
        witness_launcher.uvicorn, "run", lambda *a, **k: pytest.fail("Listener opened")
    )
    with pytest.raises(ResearchError) as refused:
        serve_witness(config_path, key_path)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_pinned_descriptor_and_tls_transport_are_not_rewritten(files, monkeypatch):
    config_path, key_path, config, native = files
    assert load_configuration(config_path) == config
    assert configured_source(config, key_path).source == native.source
    monkeypatch.setattr(
        witness_launcher.uvicorn, "run", lambda *a, **k: pytest.fail("Listener opened")
    )
    with pytest.raises(ResearchError):
        serve_witness(
            config_path, key_path, tls_certificate_file=config_path, tls_key_file=key_path
        )
    config_path.write_bytes(
        canonical_bytes(config.model_copy(update={"transport": "tls"}).model_dump(mode="json"))
    )
    with pytest.raises(ResearchError):
        serve_witness(config_path, key_path)


def test_tls_uses_admitted_bytes_after_source_path_replacement(files, tmp_path, monkeypatch):
    config_path, signing_key, config, _ = files
    config_path.write_bytes(
        canonical_bytes(config.model_copy(update={"transport": "tls"}).model_dump(mode="json"))
    )
    certificate, tls_key = tls_certificate(tmp_path)
    admitted = witness_launcher.read_private
    copies, calls = [], []
    write = witness_launcher.write_private

    def swapped(root, name, *, limit):
        content = admitted(root, name, limit=limit)
        if root / name == tls_key:
            write_private(tls_key, b"Replacement bytes must never reach the SSL loader")
        return content

    def copied(path, content):
        copies.append(path)
        write(path, content)

    monkeypatch.setattr(witness_launcher, "read_private", swapped)
    monkeypatch.setattr(witness_launcher, "write_private", copied)
    monkeypatch.setattr(
        witness_launcher.uvicorn, "run", lambda app, **kwargs: calls.append((app, kwargs))
    )
    serve_witness(config_path, signing_key, tls_certificate_file=certificate, tls_key_file=tls_key)
    assert len(calls) == 1 and len(copies) == 2
    assert all(not path.exists() for path in copies)
    app, options = calls[0]
    assert "ssl_certfile" not in options and "ssl_keyfile" not in options
    trust = ssl.create_default_context(cafile=str(certificate))
    certificate.unlink()
    tls_key.unlink()
    with (
        serving(app, ssl_context_factory=options["ssl_context_factory"]) as origin,
        httpx.Client(base_url=origin, verify=trust, trust_env=False) as client,
    ):
        assert client.post("/v1/time-witness", content=b"invalid").status_code == 401


def test_encrypted_tls_key_refuses_without_prompt_or_listener(files, tmp_path, monkeypatch):
    config_path, signing_key, config, _ = files
    config_path.write_bytes(
        canonical_bytes(config.model_copy(update={"transport": "tls"}).model_dump(mode="json"))
    )
    certificate, tls_key = tls_certificate(tmp_path)
    key = serialization.load_pem_private_key(tls_key.read_bytes(), password=None)
    write_private(
        tls_key,
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.BestAvailableEncryption(b"synthetic-fixture-password"),
        ),
    )
    monkeypatch.setattr(
        witness_launcher.uvicorn, "run", lambda *a, **k: pytest.fail("Listener opened")
    )
    monkeypatch.setattr(
        witness_launcher.ssl.SSLContext,
        "load_cert_chain",
        lambda *a, **k: pytest.fail("Encrypted key reached OpenSSL"),
    )
    with pytest.raises(ResearchError, match="Invalid native witness TLS input"):
        serve_witness(
            config_path, signing_key, tls_certificate_file=certificate, tls_key_file=tls_key
        )


async def test_launcher_keeps_one_source_and_fault_latch_without_controller_state(
    files, monkeypatch
):
    config_path, key_path, _, native = files
    clock, calls = ControlledClock(), []
    monkeypatch.setenv("WEB_CONCURRENCY", "7")
    monkeypatch.setattr("research_swarm.control.native_time_witnesses.native_clock", lambda: clock)
    monkeypatch.setattr(
        witness_launcher.uvicorn, "run", lambda app, **kwargs: calls.append((app, kwargs))
    )
    monkeypatch.setattr(cli.LocalRuntime, "open", lambda *a, **k: pytest.fail("Controller opened"))
    assert (
        cli.main(
            ["witness", "serve", "--config", str(config_path), "--signing-key-file", str(key_path)]
        )
        == 0
    )
    assert len(calls) == 1
    app, options = calls[0]
    assert options["workers"] == 1
    assert options["reload"] is options["proxy_headers"] is options["access_log"] is False
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://witness"
    ) as client:
        first = pending(native)
        response = await client.post(
            "/v1/time-witness",
            content=canonical_bytes(first.request.model_dump(mode="json")),
            headers=headers(),
        )
        assert response.status_code == 200
        first.complete(response.content)
        before = clock.reading
        clock.reading = replace(before, continuous_ns=before.continuous_ns - 1)
        request = pending(native)
        refused = await client.post(
            "/v1/time-witness",
            content=canonical_bytes(request.request.model_dump(mode="json")),
            headers=headers(),
        )
        assert refused.status_code == 409
        clock.reading = before
        new_generation = pending(native, provider_generation=identifier(99))
        latched = await client.post(
            "/v1/time-witness",
            content=canonical_bytes(new_generation.request.model_dump(mode="json")),
            headers=headers(),
        )
        assert latched.status_code == 409 and latched.json()["error"]["code"] == "STALE_FENCE"
    assert not (config_path.parent / "research.sqlite").exists()


def test_cli_rejects_controller_options_and_emits_opaque_configuration_failure(
    files, tmp_path, capsys
):
    config_path, key_path, _, _ = files
    arguments = [
        "witness",
        "serve",
        "--config",
        str(config_path),
        "--signing-key-file",
        str(key_path),
    ]
    assert cli.main(["--state-dir", str(tmp_path / "absent"), *arguments]) == 2
    assert not (tmp_path / "absent").exists()
    config_path.write_text('{"consumer_digest":"synthetic-sensitive-configuration"}')
    assert cli.main(arguments) == 2
    output = capsys.readouterr()
    assert "synthetic-sensitive-configuration" not in output.err
    assert "Traceback" not in output.err and output.out == ""
    assert all(
        json.loads(line)["error"]["code"] == "PROJECT_CONTRACT_INVALID"
        for line in output.err.splitlines()
    )


@pytest.mark.parametrize("transport", ["loopback-http", "tls"])
def test_real_launcher_subprocesses_sign_stable_then_distinct_epochs(files, tmp_path, transport):
    config_path, key_path, config, native = files
    epochs = []
    tls_arguments = []
    trust = ssl.create_default_context()
    scheme = "http"
    if transport == "tls":
        certificate, tls_key = tls_certificate(tmp_path)
        tls_arguments = ["--tls-certificate-file", str(certificate), "--tls-key-file", str(tls_key)]
        trust = ssl.create_default_context(cafile=str(certificate))
        scheme = "https"
    for index in range(2):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        config_path.write_bytes(
            canonical_bytes(
                config.model_copy(update={"port": port, "transport": transport}).model_dump(
                    mode="json"
                )
            )
        )
        with (tmp_path / f"witness-{index}.log").open("wb+") as output:
            process = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "research_swarm",
                    "witness",
                    "serve",
                    "--config",
                    str(config_path),
                    "--signing-key-file",
                    str(key_path),
                    *tls_arguments,
                ],
                stdin=subprocess.DEVNULL,
                stdout=output,
                stderr=subprocess.STDOUT,
                env={**os.environ, "WEB_CONCURRENCY": "7"},
            )
            try:
                with httpx.Client(
                    base_url=f"{scheme}://127.0.0.1:{port}",
                    trust_env=False,
                    timeout=2,
                    verify=trust,
                ) as client:
                    deadline = time.monotonic() + 15
                    while True:
                        try:
                            if client.get("/").status_code == 404:
                                break
                        except httpx.TransportError:
                            pass
                        assert process.poll() is None and time.monotonic() < deadline, (
                            "Owned witness failed to start"
                        )
                        time.sleep(0.02)
                    denied = client.post("/v1/time-witness", content=b"not json")
                    assert denied.status_code == 401
                    observed = []
                    for generation in (identifier(20), identifier(21)):
                        waiting = pending(native, provider_generation=generation)
                        result = client.post(
                            "/v1/time-witness",
                            content=canonical_bytes(waiting.request.model_dump(mode="json")),
                            headers=headers(),
                        )
                        assert result.status_code == 200
                        observed.append(
                            waiting.complete(result.content).witness.payload.source_epoch
                        )
                    assert observed[0] == observed[1]
                    epochs.append(observed[0])
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                output.seek(0)
                logged = output.read()
                assert TOKEN.encode() not in logged and SEED.hex().encode() not in logged
    assert epochs[0] != epochs[1]
