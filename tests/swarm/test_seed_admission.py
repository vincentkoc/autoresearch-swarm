"""Trial seeds must survive the CLI's canonical JSON representation exactly."""

import json
import sqlite3
from pathlib import Path

import pytest

from research_swarm.cli import main
from research_swarm.domain.canonical import MAX_SAFE_INTEGER
from research_swarm.swarm.interfaces import Candidate, Result, Trial
from research_swarm.swarm.spec import load_spec
from research_swarm.swarm.store import SQLiteStore


def campaign(directory: Path, **overrides) -> Path:
    path = directory / "campaign.yaml"
    path.write_text(
        json.dumps(
            {
                "campaign_id": "example",
                "metric": "score",
                "direction": "maximize",
                "command": ["unused"],
                "evaluation_mode": "smoke",
                "max_trials": 1,
                "max_workers": 1,
                "candidates": [{"candidate_id": "example"}],
                **overrides,
            }
        )
    )
    return path


def run(path, queue, capsys):
    code = main(["swarm", "run", str(path), "--db", str(queue)])
    output = capsys.readouterr()
    return code, json.loads(output.err if code == 2 else output.out)


def preload(path, queue, seed):
    spec = load_spec(path)
    store = SQLiteStore(queue)
    store.create_campaign(spec)
    trial = Trial(
        trial_id="archived",
        campaign_id=spec.campaign_id,
        candidate=Candidate(candidate_id="example"),
        seed=seed,
        fidelity=1.0,
    )
    assert store.submit(trial)
    attempt = store.claim(spec.campaign_id, "fixture")
    assert attempt is not None
    assert store.report(Result(attempt=attempt, status="ok", metrics={"score": 1}))


@pytest.mark.parametrize(
    "location,seed", [("campaign", MAX_SAFE_INTEGER + 1), ("candidate", -MAX_SAFE_INTEGER - 1)]
)
def test_declared_seed_rejected_before_creating_a_queue(tmp_path, capsys, location, seed):
    values = (
        {"seed": seed}
        if location == "campaign"
        else {"candidates": [{"candidate_id": "example", "seed": seed}]}
    )
    path = campaign(tmp_path, max_hours=0, **values)
    queue = tmp_path / "state" / "queue.sqlite"
    code, response = run(path, queue, capsys)
    assert code == 2 and response["error"]["code"] == "PROJECT_CONTRACT_INVALID"
    assert "seed" in response["error"]["message"].lower()
    assert not queue.parent.exists()


@pytest.mark.parametrize("seed", [-MAX_SAFE_INTEGER, MAX_SAFE_INTEGER])
def test_boundary_seed_replays_as_an_exact_json_integer(tmp_path, capsys, seed):
    path = campaign(tmp_path, seed=seed, candidates=[{"candidate_id": "example", "seed": seed}])
    queue = tmp_path / "queue.sqlite"
    preload(path, queue, seed)
    code, response = run(path, queue, capsys)
    echoed = response["selected"]["result"]["attempt"]["trial"]["seed"]
    assert code == 0 and type(echoed) is int and echoed == seed
    assert response["attempts"] == 1


def test_resume_refuses_archived_proposer_seed_before_recording_a_decision(tmp_path, capsys):
    path = campaign(tmp_path)
    queue = tmp_path / "queue.sqlite"
    preload(path, queue, MAX_SAFE_INTEGER + 1)
    with sqlite3.connect(queue) as db:
        before = list(db.iterdump())
    code, response = run(path, queue, capsys)
    assert code == 2 and "seed" in response["error"]["message"].lower()
    with sqlite3.connect(queue) as db:
        assert list(db.iterdump()) == before


class WideSeedProposer:
    def __init__(self, spec):
        pass

    async def ask(self, spec, archive, k):
        return [
            Candidate(candidate_id="valid"),
            Candidate(candidate_id="wide", seed=MAX_SAFE_INTEGER + 1),
        ]

    def tell(self, result):
        pass


class IdleWorker:
    def __init__(self, spec, store, backend):
        pass

    async def run_once(self, spec, *, before_execute=None):
        return False


def test_wide_proposer_seed_rejected_before_persisting_the_batch(tmp_path, capsys):
    path = campaign(
        tmp_path,
        proposer=f"{__name__}:WideSeedProposer",
        worker=f"{__name__}:IdleWorker",
        candidates=[],
        max_workers=2,
        max_trials=2,
    )
    queue = tmp_path / "queue.sqlite"
    code, response = run(path, queue, capsys)
    assert code == 2 and "seed" in response["error"]["message"].lower()
    with sqlite3.connect(queue) as db:
        assert db.execute("SELECT count(*) FROM swarm_trials").fetchone()[0] == 0
        assert db.execute("SELECT kind FROM swarm_events ORDER BY id").fetchall() == [("created",)]
