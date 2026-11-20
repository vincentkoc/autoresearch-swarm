"""Check expenditure and decision timing using historical streams and generated archives."""

from types import SimpleNamespace

import pytest

from benchmarks.equal_work import FULL_WORK, final_selection
from benchmarks.suite import comparison_seeds, get_task, panel_seed


@pytest.mark.parametrize("name", FULL_WORK)
@pytest.mark.parametrize("fidelity", (0.5, 1.0))
def test_completed_operations(name, fidelity):
    if name != "program":
        torch = pytest.importorskip("torch")
        torch.set_num_threads(1)
    count = 0

    def increment():
        nonlocal count
        count += 1

    task = get_task(name, protocol="v1", on_work=increment)
    task.score(task.initial, seed=11, fidelity=fidelity)
    assert count == FULL_WORK[name] * fidelity


def test_equal_work_membership_and_spaces():
    assert comparison_seeds("v3") == (3375328711, 1721423239, 1767091650)
    assert comparison_seeds("v3", calibration=True) == (834499900, 2181106248, 3679631634)
    task = get_task("program", protocol="v3")
    assert task.space == get_task("program", protocol="v1").space
    assert panel_seed("v3", "confirmation") == 3653294413
    assert set(task.members("search")).isdisjoint(task.members("confirmation"))


def test_nomination_uses_spent_work_and_earliest_best_full_rung():
    def result(name, fidelity, score, work):
        candidate = SimpleNamespace(params={"recipe": name})
        trial = SimpleNamespace(candidate=candidate, fidelity=fidelity)
        return SimpleNamespace(
            status="ok",
            attempt=SimpleNamespace(trial=trial),
            metrics={"score": score, "completed_work": work},
        )

    reference = result("initial", 0.5, 0.2, 2)
    screen = result("screen", 0.5, 1.0, 2)
    first = result("first", 1.0, 0.8, 4)
    tied = result("tied", 1.0, 0.8, 4)
    spec = SimpleNamespace(
        max_search_work=12,
        candidates=[reference.attempt.trial.candidate],
        metric="score",
        direction="maximize",
    )
    select = final_selection(spec)
    assert select([reference, screen, first]) is reference
    assert select([reference, screen, first, tied]) is first
