"""Protected full-panel scorer with both historical receipt shapes."""

from __future__ import annotations

import math
from typing import Any

from common import context, data, output, report, verified_input
from receipts import normalize, schema2_panel


def people_errors(
    dataset: dict[str, Any], weight: float, bias: float, *, central: bool
) -> dict[str, Any]:
    people = {}
    for person in dataset["partition"]["search"]:
        rows = dataset["people"][person]
        selected = rows[:1] if central else rows
        errors = [abs(weight * feature + bias - target) for feature, target in selected]
        people[person] = {
            "elements": len(errors),
            "windows": len(selected),
            "error_sum": sum(errors),
        }
    return people


def main() -> None:
    request = context()
    dataset, identities = data()
    model, checkpoint_digest = verified_input(request, "model")
    fit, _ = verified_input(request, "fit_receipt")
    if model["format"] != "synthetic-pose-v1" or model["units"] != "radians":
        raise ValueError("Checkpoint format or units changed")
    if model["seed"] != request["seed"] or fit["seed"] != request["seed"]:
        raise ValueError("Checkpoint or fit seed differs")
    if model["fit_people"] != dataset["partition"]["fit"]:
        raise ValueError("Fitting coverage changed")
    if fit["trial"] != request["trial_id"] or fit["checkpoint_sha256"] != checkpoint_digest:
        raise ValueError("Fit receipt does not bind this checkpoint and trial")
    if any(
        model.get(name) != value or fit.get(name) != value for name, value in identities.items()
    ):
        raise ValueError("Pinned dataset, partition, manifest, or evaluator changed")
    weight, bias = model["weight"], model["bias"]
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in (weight, bias)):
        raise ValueError("Model parameters must be finite")
    people = dataset["partition"]["search"]
    endpoint = {
        "trial": request["trial_id"],
        "checkpoint_sha256": checkpoint_digest,
        **identities,
        "units": "absolute_error_sum_radians",
        "people": people_errors(dataset, weight, bias, central=False),
    }
    full = normalize(
        endpoint,
        source_schema="campaign-schema-2",
        expected_people=people,
        expected_identity=identities,
    )["full"]
    central = schema2_panel(
        {"units": endpoint["units"], "people": people_errors(dataset, weight, bias, central=True)},
        people,
    )
    archived = {
        "trial": request["trial_id"],
        "batch_sha256": identities["batch_sha256"],
        "checkpoint_sha256": checkpoint_digest,
        "scores": {},
    }
    for name, panel in (("central", central), ("full", full)):
        archived["scores"][name] = {
            "absolute_error_radians": panel.absolute_error_sum_radians,
            "valid_joint_frames": panel.valid_elements,
            "windows": panel.windows,
            "per_person_mae_deg": panel.per_person_mae_deg,
            "person_mae_deg": panel.macro_mae_deg,
            "pooled_mae_deg": panel.pooled_mae_deg,
        }
    normalize(
        archived,
        source_schema="candidates-v2",
        expected_people=people,
        expected_identity={
            "checkpoint_sha256": checkpoint_digest,
            "batch_sha256": identities["batch_sha256"],
        },
    )
    report(
        request,
        outputs=[
            output(request, "endpoint_schema2", endpoint),
            output(request, "endpoint_candidates_v2", archived),
        ],
        measurements=[
            {
                "name": "macro_mae_deg",
                "value": full.macro_mae_deg,
                "unit": "degrees",
                "direction": "minimize",
            },
            {
                "name": "pooled_mae_deg",
                "value": full.pooled_mae_deg,
                "unit": "degrees",
                "direction": "minimize",
            },
        ],
        observed_members=people,
        diagnostics={"valid_elements": full.valid_elements, "windows": full.windows},
    )


if __name__ == "__main__":
    main()
