from typing import Any

import pytest

from research_swarm.control.report_selection import confirmed_cohorts, observed_cohorts


def observation(
    identifier: str,
    value: float,
    *,
    cohort: str = "same-hardware",
    eligible: bool = True,
    direction: str = "minimize",
) -> dict[str, Any]:
    return {
        "id": identifier,
        "eligible": eligible,
        "comparison_digest": cohort,
        "primary_metric": {"name": "loss", "value": value, "unit": "ratio", "direction": direction},
        "data": {"candidate_id": identifier, "trial_id": "trial-" + identifier},
    }


@pytest.mark.parametrize(
    "direction,expected", [("minimize", ["low", "tie"]), ("maximize", ["high"])]
)
def test_observed_extrema_keep_direction_ties_missingness_and_hardware_separate(
    direction: str, expected: list[str]
) -> None:
    values = [
        observation("low", 1.0, direction=direction),
        observation("tie", 1.0, direction=direction),
        observation("high", 3.0, direction=direction),
        observation("other", 100.0, cohort="different-hardware", direction=direction),
        observation("failed", 0.0, eligible=False, direction=direction),
    ]
    cohorts = {item["comparison_digest"]: item for item in observed_cohorts(values, complete=False)}
    assert len(cohorts) == 2
    assert [item["evaluation_id"] for item in cohorts["same-hardware"]["best_observed"]] == expected
    assert cohorts["different-hardware"]["best_observed"][0]["evaluation_id"] == "other"
    assert all(
        item["scope"] == "captured_cohort" and not item["history_complete"]
        for item in cohorts.values()
    )
    assert cohorts["same-hardware"]["observation_count"] == 3


def confirmation(identifier: str, revision: int, *, cohort: str = "same-lineage") -> dict[str, Any]:
    return {
        "id": identifier,
        "confirmation_status": "qualified",
        "confirmation": {
            "cohort_digest": cohort,
            "comparison_digest": "same-hardware",
            "policy_digest": "same-policy",
            "baseline_promotion_id": cohort,
            "candidate_id": identifier,
            "promotion_id": "promotion-" + identifier,
            "confirmation_plan_id": "plan-" + identifier,
            "selected_incumbent_revision": revision,
            "is_current_incumbent": identifier == "new",
            "first_seed_value": 0.0 if identifier == "old" else 100.0,
            "mean_improvement": 100.0 if identifier == "old" else 0.001,
            "p_value": 0.0001 if identifier == "old" else 0.03,
        },
    }


def test_confirmed_selection_follows_committed_revision_without_new_scalar_ranking() -> None:
    old, new = confirmation("old", 2), confirmation("new", 5)
    uncommitted = confirmation("uncommitted", 99)
    uncommitted["confirmation_status"] = "uncommitted"
    ineligible = confirmation("invalidated", 100)
    ineligible["confirmation_status"] = "ineligible"
    after_reset = confirmation("reset", 7, cohort="new-baseline")
    page = {"complete": True, "items": [old, new, uncommitted, ineligible, after_reset]}
    cohorts = {item["cohort_digest"]: item for item in confirmed_cohorts(page)}
    assert len(cohorts) == 2
    selected = cohorts["same-lineage"]["best_confirmed"]
    assert selected["candidate_id"] == "new"
    assert selected["selected_incumbent_revision"] == 5
    assert cohorts["same-lineage"]["aggregate_metric"] is None
    assert cohorts["new-baseline"]["best_confirmed"]["candidate_id"] == "reset"
    partial = confirmed_cohorts({**page, "complete": False})
    assert all(
        item["status"] == "incomplete" and item["best_confirmed"] is None for item in partial
    )


def test_inconsistent_equal_revision_does_not_select_an_arbitrary_confirmed_candidate() -> None:
    cohorts = confirmed_cohorts(
        {"complete": True, "items": [confirmation("one", 2), confirmation("two", 2)]}
    )
    assert cohorts[0]["status"] == "ambiguous"
    assert cohorts[0]["best_confirmed"] is None
