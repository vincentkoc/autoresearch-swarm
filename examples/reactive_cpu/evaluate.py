"""Independent protected evaluation; training diagnostics never authorize promotion."""

from common import context, data, model, mse, report


def main() -> None:
    request = context()
    configuration, _ = data()
    names = {
        "model": "reactive",
        "baseline": "fixed_initial_controls",
        "replay_model": "control_schedule_replay",
    }
    scores = {
        name: mse(model(request, name, mode)["weights"], configuration["evaluation_x"])
        for name, mode in names.items()
    }
    report(
        request,
        measurements=[
            {
                "name": "mse",
                "value": scores["model"],
                "unit": "squared_error",
                "direction": "minimize",
            }
        ],
        observed_members=[configuration["panel"]],
        diagnostics={
            "baseline_mse": scores["baseline"],
            "replay_mse": scores["replay_model"],
            "interpretation": "Synthetic execution evidence, not a validated research improvement",
        },
    )


if __name__ == "__main__":
    main()
