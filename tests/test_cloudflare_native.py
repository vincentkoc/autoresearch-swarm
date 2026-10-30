"""Opt-in local workerd qualification; no hosted account or deployment."""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from research_swarm.domain.assignments import AssignmentAuthority, SignedAssignment
from research_swarm.domain.canonical import canonical_bytes
from research_swarm.domain.clocks import ClockChallenge, SignedAssignmentTime, SignedClockReply
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.signatures import AssignmentVerifier
from research_swarm.workers.timed_launch import TimingEvidence, verify_timing
from tests.test_crypto_providers import signing_probe

pytestmark = pytest.mark.skipif(
    os.environ.get("RESEARCH_CLOUDFLARE_TESTS") != "1" or os.name != "posix",
    reason="Explicit opt-in and POSIX-owned local workerd process group required",
)
ROOT = Path(__file__).resolve().parents[1] / "adapters/cloudflare"


def build_current_sources():
    for command in (
        ["npm", "ci", "--ignore-scripts", "--no-audit", "--no-fund"],
        ["npm", "run", "build"],
    ):
        result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=120)
        assert result.returncode == 0, (result.stdout + result.stderr)[-4000:]


def close_owned_group(process):
    for action in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, action)
        except ProcessLookupError:
            process.wait(timeout=10)
            return
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            process.poll()  # Reap the leader while checking all remaining group members.
            try:
                os.killpg(process.pid, 0)
            except ProcessLookupError:
                process.wait(timeout=10)
                return
            time.sleep(0.02)
    raise AssertionError("Owned workerd process group did not close; refusing state reuse")


def port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


@contextmanager
def server(
    tmp_path: Path,
    *,
    sequence: int,
    variables: dict[str, str] | None = None,
    config: str = "wrangler.hybrid.jsonc",
):
    if variables is None:
        executable = ROOT / ".venv/bin/uv"
        assert executable.is_file(), "Prepare the isolated Cloudflare tool environment first"
        assert (ROOT / ".generated/crypto.mjs").is_file(), "Run npm run build first"
        command = [str(executable), "run", "--frozen", "pywrangler", "dev"]
    else:
        command = [
            str(ROOT / "node_modules/.bin/wrangler"),
            "dev",
            "--config",
            config,
        ]
        for name, value in variables.items():
            command.extend(["--var", f"{name}:{value}"])
    selected_port = port()
    url = f"http://127.0.0.1:{selected_port}"
    log = tmp_path / f"local-workerd-{sequence}.log"
    with log.open("wb") as output:
        process = subprocess.Popen(
            [
                *command,
                "--local",
                "--persist-to",
                str(tmp_path / "state"),
                "--port",
                str(selected_port),
                "--inspector-port",
                str(port()),
                "--log-level",
                "warn",
                "--show-interactive-dev-session",
                "false",
            ],
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            env={
                **os.environ,
                "WRANGLER_SEND_METRICS": "false",
                "CI": "true",
                "WRANGLER_LOG_PATH": str(tmp_path / f"wrangler-{sequence}.log"),
            },
        )
        try:
            deadline = time.monotonic() + 180
            while True:
                try:
                    with httpx.Client(trust_env=False, timeout=1) as client:
                        response = client.get(url + "/")
                    if response.status_code == 404:
                        break
                except httpx.TransportError:
                    pass
                if process.poll() is not None or time.monotonic() >= deadline:
                    text = log.read_text(errors="replace")[-8000:]
                    text = text.replace(str(ROOT.parents[1]), "<repo>").replace(
                        str(tmp_path), "<fixture>"
                    )
                    for name, value in (variables or {}).items():
                        if "TOKEN" in name:
                            text = text.replace(value, "<fixture-token>")
                    pytest.fail("Local workerd did not become ready:\n" + text)
                time.sleep(0.1)
            yield url
        finally:
            close_owned_group(
                process
            )  # Exact group created by this fixture, including descendants.


def exercise(url, *, key, mode="accept", value=7):
    with httpx.Client(trust_env=False, timeout=30) as client:
        response = client.post(
            url + "/qualification", json={"key": key, "mode": mode, "value": value}
        )
    assert response.status_code == 200, response.text[:2000]
    return response.json()


def test_python_do_nested_rollback_synchronous_signing_and_persistent_replay(tmp_path):
    build_current_sources()
    with server(tmp_path, sequence=1) as url:
        with httpx.Client(trust_env=False, timeout=30) as client:
            qualify_shared_signing(client, url)
            for case in ("roundtrip", "unknown-version", "foreign-state", "migration-rollback"):
                response = client.post(url + "/storage/" + case)
                assert response.status_code == 200, response.text[-3000:]
                assert response.json() and all(value is True for value in response.json().values())
        accepted = exercise(url, key="accepted")
        assert accepted["state"] == "accepted"
        assert accepted["counts"] == {"effects": 1, "fences": 0, "acknowledgments": 1}
        payload = canonical_bytes(
            {"schema_version": "1.0", "key": "accepted", "mode": "accept", "value": 7}
        )
        assert bytes.fromhex(accepted["payload_hex"]) == payload
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(accepted["public_key"])).verify(
            bytes.fromhex(accepted["signature"]), payload
        )
        rejected = exercise(url, key="rejected", mode="reject")
        assert rejected["state"] == "rejected"
        assert rejected["code"] == "INVALID_TRANSITION"
        assert rejected["counts"] == {"effects": 0, "fences": 1, "acknowledgments": 1}
        failed_hook = exercise(url, key="hook-failure", mode="hook_failure")
        assert failed_hook["state"] == "raised"
        assert failed_hook["counts"] == {"effects": 0, "fences": 0, "acknowledgments": 0}
        assert exercise(url, key="hook-failure")["state"] == "accepted"
        with ThreadPoolExecutor(max_workers=8) as pool:
            deliveries = list(pool.map(lambda _: exercise(url, key="contended"), range(8)))
        assert all(item == deliveries[0] for item in deliveries)
        assert deliveries[0]["counts"] == {"effects": 1, "fences": 0, "acknowledgments": 1}
        changed = exercise(url, key="contended", value=8)
        assert changed["state"] == "raised" and "IDEMPOTENCY_CONFLICT" in changed["error"]
        assert changed["counts"] == deliveries[0]["counts"]
    with server(tmp_path, sequence=2) as url:
        assert exercise(url, key="accepted") == accepted
        assert exercise(url, key="rejected", mode="reject") == rejected
        assert exercise(url, key="contended") == deliveries[0]
        with httpx.Client(trust_env=False, timeout=10) as client:
            denied = client.post(
                url + "/qualification",
                headers={"host": "example.invalid"},
                content=json.dumps({"key": "external", "mode": "accept", "value": 1}),
            )
        assert denied.status_code == 403


def qualify_shared_signing(client, url):
    probe = signing_probe()
    response = client.post(url + "/signing", json=probe)
    assert response.status_code == 200, response.text[-3000:]
    result = response.json()
    assert "error" not in result, result
    authority = AssignmentAuthority.model_validate(probe["authority"])
    signed = SignedAssignment.model_validate(result["assignment"])
    AssignmentVerifier(authority).verify(signed, authority_now=signed.payload.issued_at)
    verifier = ClockVerifier(authority)
    verifier.reply(
        SignedClockReply.model_validate(result["reply"]),
        ClockChallenge.model_validate(probe["challenge"]),
    )
    grant = SignedAssignmentTime.model_validate(result["grant"])
    verifier.binding(grant, signed)
    timing = verify_timing(TimingEvidence.model_validate(probe["timing"]), authority, grant, signed)
    assert timing.evidence.digest == result["timing_digest"]
    for field in ("assignment", "reply", "grant"):
        assert result[field] == probe[field]
    for changed in ("binding", "signature", "domain"):
        altered = json.loads(json.dumps(probe))
        if changed == "binding":
            altered["assignment"]["payload"]["dispatch"]["control_epoch"] += 1
        elif changed == "signature":
            altered["reply"]["signature"] = "00" * 64
        else:
            altered["reply"]["signature"] = altered["grant"]["signature"]
        rejected = client.post(url + "/signing", json=altered)
        assert rejected.status_code == 200, rejected.text[-3000:]
        assert rejected.json() == {"error": "AUTHORIZATION_DENIED"}
