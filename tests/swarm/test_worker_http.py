from __future__ import annotations

import hashlib
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Thread

import httpx
import pytest
import uvicorn

from research_swarm.api.campaign import campaign_app
from research_swarm.api.worker import WorkerGrant
from research_swarm.swarm.files import CampaignFiles
from research_swarm.swarm.reader import CampaignReader
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore
from tests.swarm.test_store import spec, trial


@contextmanager
def worker_http(path: Path) -> Iterator[httpx.Client]:
    reader = CampaignReader(path)
    grant = WorkerGrant("test", "worker-1", "test-worker-token")
    app = campaign_app(reader, "test-reader-token", worker=grant)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", ws="none"))
        thread = Thread(target=server.run, kwargs={"sockets": [sock]})
        thread.start()
        try:
            deadline = time.monotonic() + 10
            while not server.started:
                assert thread.is_alive() and time.monotonic() < deadline
                time.sleep(0.01)
            with httpx.Client(
                base_url=f"http://127.0.0.1:{sock.getsockname()[1]}", trust_env=False
            ) as client:
                yield client
        finally:
            server.should_exit = True
            thread.join(10)
            assert not thread.is_alive()


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": "Bearer " + token}


def test_scoped_worker_writes_and_replays_on_loopback(tmp_path: Path) -> None:
    path = tmp_path / "campaign.sqlite"
    store = SQLiteStore(path)
    store.create_campaign(spec(price_per_million_input_tokens=1_000_000, max_cost=1))
    store.submit(trial(1))
    store.submit(trial(2))
    store.create_campaign(spec(campaign_id="other"))
    store.submit(trial(1, campaign_id="other"))
    worker = bearer("test-worker-token")
    with pytest.raises(ValueError, match="distinct"):
        campaign_app(CampaignReader(path), "same", worker=WorkerGrant("test", "worker", "same"))
    with worker_http(path) as http:
        claim = "/v1/worker/claim?campaign_id=test&worker_id=worker-1"
        assert http.post(claim, headers=bearer("test-reader-token")).status_code == 401
        assert http.get("/v1/campaigns", headers=worker).status_code == 401
        first = http.post(claim, headers=worker).raise_for_status().json()
        assert first["attempt"]["worker_id"] == "worker-1"
        scope = "/v1/worker/" + first["attempt"]["attempt_id"]
        token = bearer(first["token"])
        assert http.post(scope + "/heartbeat", headers=worker).status_code == 401
        assert http.post(scope + "/heartbeat", headers=token).json() is True

        data = b"trial output\n"
        digest = hashlib.sha256(data).hexdigest()
        for wanted, status in [("0" * 64, 422), (digest, 200)]:
            upload = http.put(
                scope + "/artifact", content=data, headers={**token, "Content-SHA256": wanted}
            )
            assert upload.status_code == status
        destination = tmp_path / "read-back"
        CampaignFiles(path).download(upload.json()["id"], destination)
        assert destination.read_bytes() == data

        body = {"status": "ok", "metrics": {"loss": 0.25}, "input_tokens": 1}
        assert (
            http.post(scope + "/report", json={**body, "cost": 0}, headers=token).status_code == 422
        )
        assert http.post(scope + "/report", json=body, headers=token).json() is True
        assert http.post(scope + "/report", json=body, headers=token).json() is False
        assert (
            http.post(
                scope + "/report", json={**body, "status": "failed"}, headers=token
            ).status_code
            == 409
        )
        assert (
            http.put(
                scope + "/artifact", content=data, headers={**token, "Content-SHA256": digest}
            ).status_code
            == 409
        )
        assert [r.cost for r in store.results("test")] == [1]
        assert store.queue_depth("other") == (1, 0)


def test_reclaimed_attempt_cannot_report_or_put(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    store = SQLiteStore(path)
    store.create_campaign(spec())
    store.submit(trial(1))
    with worker_http(path) as http:
        auth = bearer("test-worker-token")
        first = http.post(
            "/v1/worker/claim?campaign_id=test&worker_id=worker-1", headers=auth
        ).json()
        assert store.requeue_expired("test", time.time() + 1) == 1
        second = http.post(
            "/v1/worker/claim?campaign_id=test&worker_id=worker-1", headers=auth
        ).json()
        assert second["attempt"]["number"] == 2
        old = "/v1/worker/" + first["attempt"]["attempt_id"]
        assert http.post(old + "/heartbeat", headers=bearer(second["token"])).status_code == 401
        assert (
            http.post(
                old + "/report", headers=bearer(first["token"]), json={"status": "failed"}
            ).status_code
            == 410
        )
        assert [r.status for r in store.results("test")] == ["lost"]


def test_remote_worker_cli_executes_local_argv_and_publishes(tmp_path: Path) -> None:
    path = tmp_path / "queue.db"
    script = tmp_path / "score.py"
    script.write_text(
        "import json, os, pathlib, sys\n"
        "trial = json.loads(pathlib.Path(os.environ['SWARM_TRIAL']).read_text())\n"
        "assert sys.argv[1:] == ['one argument', 'two']\n"
        "metrics = pathlib.Path(os.environ['SWARM_METRICS'])\n"
        "metrics.write_text(json.dumps({'loss': trial['seed']}))\n"
    )
    command = [sys.executable, str(script), "one argument", "two"]
    document = {
        "campaign_id": "test",
        "metric": "loss",
        "direction": "minimize",
        "command": command,
        "task_dir": str(tmp_path),
        "max_workers": 1,
        "max_trials": 1,
        "candidates": [{"candidate_id": "candidate-1", "params": {"x": 1}}],
    }
    config = tmp_path / "campaign.yaml"
    config.write_text(json.dumps(document))
    store = SQLiteStore(path)
    store.create_campaign(load_spec(config))
    store.submit(trial(1))
    token = tmp_path / "token"
    token.write_text("test-worker-token")
    token.chmod(0o600)
    with worker_http(path) as http:
        origin = Path(__file__).resolve().parents[2] / "src"
        process = subprocess.run(
            [
                sys.executable,
                "-m",
                "research_swarm",
                "--controller",
                str(http.base_url),
                "--token-file",
                str(token),
                "swarm",
                "worker",
                str(config),
                "--worker-id",
                "worker-1",
                "--once",
            ],
            cwd=tmp_path,
            env={**os.environ, "PYTHONPATH": str(origin)},
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert process.returncode == 0, process.stderr
        assert json.loads(process.stdout) == {"worker_id": "worker-1", "ran": True}
    (result,) = store.results("test")
    assert result.status == "ok" and result.metrics == {"loss": 2.0}
