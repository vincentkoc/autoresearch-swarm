"""Run a native research project as one swarm trial with measured evaluation stages."""

from __future__ import annotations

import argparse
import json
import os
import sys
from graphlib import TopologicalSorter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import TypeAdapter

from research_swarm.engines import execution
from research_swarm.engines.registration import read_registration
from research_swarm.reactive.admission import training_parameters
from research_swarm.swarm.canonical import record_digest
from research_swarm.swarm.interfaces import Candidate, Trial
from research_swarm.swarm.process import run as run_command
from research_swarm.swarm.project import ResolvedProject, capture_project, resolve_project
from research_swarm.swarm.projects.source import bundle_directory, extract_bundle, validate_mutation
from research_swarm.swarm.projects.workload import (
    WorkloadContext,
    WorkloadInput,
    read_report,
    verify_outputs,
)
from research_swarm.swarm.records import ResourceRequirements
from research_swarm.workers.supervision import base_environment


def _candidate_bundle(
    project: ResolvedProject, bundle: Path, candidate: Candidate
) -> tuple[Path, str]:
    base = project.source
    if candidate.diff is None:
        return bundle, base.tree_digest
    if not candidate.source_revision or candidate.source_revision not in (
        base.base_revision,
        base.tree_digest,
    ):
        raise ValueError("Candidate diff does not match the captured source revision")

    with TemporaryDirectory(dir=bundle.parent) as workspace:
        source = Path(workspace) / "project"
        extract_bundle(bundle, source, expected_tree_digest=base.tree_digest)
        run_command(
            ["git", "apply", "--whitespace=nowarn", "-"],
            input=candidate.diff.encode("utf-8"),
            cwd=source,
            env={
                **base_environment(),
                "GIT_CEILING_DIRECTORIES": str(source.parent.resolve()),
                "GIT_CONFIG_GLOBAL": os.devnull,
                "GIT_CONFIG_NOSYSTEM": "1",
            },
            check=True,
        )
        edited = bundle.with_name("candidate.zip")
        snapshot = bundle_directory(source, edited, paths=("**",))
        if not validate_mutation(
            base, snapshot, project.manifest.mutation, protected_paths=project.protected_paths
        ):
            raise ValueError("Candidate diff did not change any project source")
    return edited, snapshot.tree_digest


def run(
    trial: Trial, attempt_id: UUID, manifest_path: str, registration_path: Path | None, root: Path
) -> dict[str, float]:
    bundle = root / "project.zip"
    capture_project(Path.cwd(), bundle, manifest_path=manifest_path)
    project = resolve_project(bundle, manifest_path=manifest_path)
    manifest = project.manifest
    if manifest.environment.network_policy != "inherit":
        raise ValueError("Native execution cannot enforce the project's network policy")
    registration = read_registration(registration_path) if registration_path else None
    if registration and registration.descriptor.device is not None:
        backend = registration.descriptor.device_backend
        allocated = os.environ.get("SWARM_DEVICE", "cpu")
        compatible = {"cuda", "cuda:0"} if backend in {"cuda", "rocm"} else {"mps"}
        if allocated not in compatible:
            raise ValueError("Campaign device allocation differs from registered engine")
        visible = os.environ.get("CUDA_VISIBLE_DEVICES")
        if (
            backend in {"cuda", "rocm"}
            and visible is not None
            and visible != str(registration.descriptor.device.index)
        ):
            raise ValueError("Allocated device differs from registered accelerator")
    frozen = execution.prepare_engine(root / "engine", registration) if registration else None
    contract = manifest.evaluation
    if contract.measurement_trust != "authenticated" or contract.expected_members is None:
        raise ValueError("Native evaluation requires the declared authenticated member set")
    candidate_bundle, candidate_digest = _candidate_bundle(project, bundle, trial.candidate)
    trial_id = str(uuid5(NAMESPACE_URL, json.dumps([trial.campaign_id, trial.trial_id])))
    digest = record_digest(
        "research.swarm-native-trial",
        {
            "trial": TypeAdapter(Trial).dump_python(trial, mode="json"),
            "protocol": project.protocol_digest,
            "source": candidate_digest,
        },
    )
    stages = {stage.id: stage for stage in manifest.stages}
    producers: dict[str, dict[str, str]] = {}
    for stage in stages.values():
        if any(
            len(set(names)) != len(names)
            for names in (stage.output_names, stage.required_artifacts)
        ):
            raise ValueError(f"Duplicate artifact name in stage {stage.id}")
        producers[stage.id] = {}
        for name in stage.required_artifacts:
            sources = [
                parent for parent in stage.dependencies if name in stages[parent].output_names
            ]
            if len(sources) != 1:
                raise ValueError(f"Stage {stage.id} requires exactly one direct producer of {name}")
            producers[stage.id][name] = sources[0]
    order = TopologicalSorter(
        {stage.id: stage.dependencies for stage in manifest.stages}
    ).static_order()
    outputs: dict[str, dict[str, WorkloadInput]] = {}
    scores: dict[str, float] = {}
    schema = {metric.name: metric for metric in contract.metrics}
    for index, stage_id in enumerate(order):
        stage = stages[stage_id]
        stage_root = root / f"stage-{index}"
        stage_root.mkdir(mode=0o700)
        source = stage_root / "project"
        if stage.kind == "evaluate":
            extract_bundle(bundle, source, expected_tree_digest=project.source.tree_digest)
        else:
            extract_bundle(candidate_bundle, source, expected_tree_digest=candidate_digest)
        output = stage_root / "outputs"
        output.mkdir(mode=0o700)
        parameters: dict[str, Any] = {
            "candidate": trial.candidate.params,
            "fidelity": trial.fidelity,
            **training_parameters(manifest.reactive, stage),
        }
        env = base_environment()
        argv = stage.argv
        if registration is not None and frozen is not None:
            parameters["engine"] = execution.context(
                registration, project.environment_contract_digest, stage
            )
            env.update(execution.environment(registration, stage))
            argv = (*frozen, *argv[1:])
        else:
            if (
                manifest.environment.profile != "python-cpu"
                or stage.resources != ResourceRequirements()
            ):
                raise ValueError("This workload requires an explicit engine registration")
            if argv[0] in {"python", "python3"}:
                argv = (sys.executable, *argv[1:])
        context = WorkloadContext(
            trial_id=trial_id,
            attempt_id=str(uuid5(attempt_id, stage.id)),
            stage_id=stage.id,
            spec_digest=digest,
            seed=trial.seed,
            time_budget_ms=manifest.timing.budget_ms,
            inputs={name: outputs[parent][name] for name, parent in producers[stage.id].items()},
            output_root=str(output),
            report_path=str(stage_root / "report.json"),
            parameters=parameters,
        ).sealed()
        context_path = stage_root / "context.json"
        context_path.write_text(context.model_dump_json(), encoding="utf-8")
        run_command(
            argv,
            cwd=source,
            env={**env, "RESEARCH_CONTEXT": str(context_path)},
            check=True,
            timeout=stage.timeout_ms / 1000,
        )
        report = read_report(context, stage)
        verified = verify_outputs(context, report, maximum_bytes=256 * 1024 * 1024)
        outputs[stage.id] = {
            item.output.name: WorkloadInput(
                path=str(output / item.output.path), digest=item.digest, size_bytes=item.size_bytes
            )
            for item in verified
        }
        if stage.kind == "evaluate":
            if set(report.observed_members) != contract.expected_members:
                raise ValueError("Evaluation observed a different member set")
            for metric in report.measurements:
                definition = schema.get(metric.name)
                if (
                    definition is None
                    or (metric.unit, metric.direction) != (definition.unit, definition.direction)
                    or metric.name in scores
                ):
                    raise ValueError("Evaluation metric differs from its declared schema")
                scores[metric.name] = metric.value
    if scores.keys() != schema.keys():
        raise ValueError("Evaluation did not produce every declared metric")
    return scores


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default="research.toml")
    parser.add_argument("--registration", type=Path)
    parser.add_argument(
        "--outputs", type=Path, help="Retain stage files in a directory per attempt"
    )
    args = parser.parse_args()
    trial = TypeAdapter(Trial).validate_json(
        Path(os.environ["SWARM_TRIAL"]).read_bytes(), strict=True
    )
    attempt = UUID(os.environ["SWARM_ATTEMPT_ID"])
    if args.outputs:
        root = args.outputs.absolute() / str(attempt)
        root.mkdir(mode=0o700, parents=True)
        scores = run(trial, attempt, args.manifest, args.registration, root)
    else:
        with TemporaryDirectory() as temporary:
            scores = run(trial, attempt, args.manifest, args.registration, Path(temporary))
    Path(os.environ["SWARM_METRICS"]).write_text(
        json.dumps(scores, allow_nan=False), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
