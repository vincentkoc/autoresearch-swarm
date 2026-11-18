import json
import math
import os
import random
import subprocess
import sys

import httpx
import pytest

from benchmarks.ollama import OllamaPolicy
from benchmarks.suite import get_task, greedy_search, random_search, run_search


def test_common_budget_counts_initial_and_failed_trials():
    task = get_task("program")
    solved = dict(zip(task.space, ("lower", "hyphenate", "trim-hyphens", "identity"), strict=True))

    def propose(task, history, rng):
        if len(history) == 1:
            raise TimeoutError("model timeout")
        return solved

    runs = run_search(task, propose, budget=4, seed=11)
    assert [r.candidate for r in runs] == [task.initial, None, solved, solved]
    assert runs[1].error == "TimeoutError: model timeout"
    assert [r.best for r in runs] == [0, 0, 1, 1]
    for policy in (random_search, greedy_search):
        a = run_search(task, policy, budget=12, seed=47)
        b = run_search(task, policy, budget=12, seed=47)
        assert a[0].candidate == task.initial
        assert [r.score for r in a] == [r.score for r in b]
        assert len(a) == 12 and all(r.error is None for r in a)
        assert [r.best for r in a] == sorted(r.best for r in a)


def test_worker_entry_point(tmp_path):
    task = get_task("program")
    trial = tmp_path / "trial.json"
    metrics = tmp_path / "metrics.json"
    trial.write_text(json.dumps({"candidate": {"params": task.initial}, "seed": 23, "fidelity": 1}))
    subprocess.run(
        [sys.executable, "-m", "benchmarks.trial", "program"],
        env={**os.environ, "SWARM_TRIAL": str(trial), "SWARM_METRICS": str(metrics)},
        check=True,
    )
    assert json.loads(metrics.read_text()) == {"pass_rate": 0}


def test_local_model_proposes_scored_code(monkeypatch):
    task = get_task("program")
    lines = dict(zip(task.space, ("lower", "hyphenate", "trim-hyphens", "strip"), strict=True))
    response_text = json.dumps(lines)

    def generate(client, url, *, json):
        assert url == "http://127.0.0.1:11434/api/generate"
        assert json["format"]["required"] == list(task.space)
        return httpx.Response(
            200,
            request=httpx.Request("POST", url),
            json={
                "response": response_text,
                "prompt_eval_count": 80,
                "eval_count": 12,
            },
        )

    monkeypatch.setattr(httpx.Client, "post", generate)
    policy = OllamaPolicy("fixture")
    results = run_search(task, policy, budget=2, seed=11)
    assert (results[1].score, policy.input_tokens, policy.output_tokens) == (1, 80, 12)


@pytest.mark.parametrize("name", ["mlp", "nanogpt"])
def test_train_and_compare_on_cpu(name):
    torch = pytest.importorskip("torch")
    torch.set_num_threads(1)
    task = get_task(name)
    for policy in (random_search, greedy_search):
        results = run_search(task, policy, budget=3, seed=11)
        assert all(r.score is not None and math.isfinite(r.score) for r in results)
        scores = [r.score for r in results]
        best = max if name == "mlp" else min
        assert [r.best for r in results] == [best(scores[:i]) for i in (1, 2, 3)]
        assert task.score(results[1].candidate, seed=11) == results[1].score


def test_training_code_rejects_out_of_space_edits():
    pytest.importorskip("torch")
    from benchmarks.training import _training_code

    task = get_task("nanogpt")
    candidate = random_search(task, (), random.Random(11))
    with pytest.raises(ValueError, match="unknown training code"):
        _training_code({**candidate, "schedule": "return unknown_function()"})
