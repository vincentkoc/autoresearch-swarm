from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from threading import Barrier
from typing import Any

import httpx
import pytest
from pydantic import ValidationError

from research_swarm.api.http import create_app
from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.evaluations import AuthorizeEvaluation, CompleteEvaluation
from research_swarm.control.observation_models import ObserveTrial
from research_swarm.control.orchestration_models import (
    BindOrchestration,
    DriveTurnTarget,
    FenceOrchestration,
    PrepareDriveTurn,
)
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, Principal
from research_swarm.domain.orchestration import DriveAction
from research_swarm.domain.types import new_id
from research_swarm.orchestration.turns import DurableTurnDriver
from research_swarm.runtime import LocalRuntime
from research_swarm.state.artifacts import BeginUpload
from tests.control.test_evaluation_driver import role
from tests.integration.test_local_pipeline import Pipeline
from tests.integration.test_local_pipeline import pipeline as pipeline
from tests.orchestration.test_http_delivery import serving
from tests.search.test_proposals import PROPOSAL_LIMITS, baseline
from tests.search.test_search_admission import admission_intent, answered
from tests.search.test_search_observations import evaluate

CONFIG = content_digest(b"fixed-driver-configuration-v1")


@dataclass
class Drivers:
    control: ResearchClient
    evaluator: ResearchClient
    actor: Principal
    evaluator_actor: Principal
    token: str


def bind(pipeline: Pipeline, *, adapter: str = "local") -> Drivers:
    actor, control, token = role(pipeline, "*")
    evaluator_actor, evaluator, _ = role(pipeline, "*")
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    pipeline.client.command(
        "orchestration.bind",
        BindOrchestration(
            campaign_id=campaign["id"],
            adapter=adapter,
            configuration_digest=CONFIG,
            routes={"control": actor.id, "evaluator": evaluator_actor.id},
        ),
        idempotency_key="binding",
        expected_revision=campaign["revision"],
    )
    return Drivers(control, evaluator, actor, evaluator_actor, token)


def action(pipeline: Pipeline) -> DriveAction:
    trial = pipeline.client.get("trial", pipeline.trial["id"])
    return DriveAction(
        operation="search.observe_trial",
        payload=ObserveTrial(
            campaign_id=pipeline.campaign["id"],
            trial_id=trial["id"],
        ).model_dump(mode="json"),
        idempotency_key="original-observation",
        expected_revision=trial["revision"],
    )


def prepare(
    pipeline: Pipeline,
    client: ResearchClient,
    command: DriveAction,
    *,
    intent: PrepareDriveTurn | None = None,
    revision: int | None = None,
) -> dict[str, Any]:
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    intent = intent or PrepareDriveTurn(
        campaign_id=pipeline.campaign["id"],
        epoch=1,
        ordinal=binding["data"]["next_ordinal"],
        configuration_digest=CONFIG,
        planning_watermark=0,
        action=command,
    )
    return client.command(
        "orchestration.prepare_turn",
        intent,
        idempotency_key=new_id(),
        expected_revision=revision if revision is not None else binding["revision"],
    )["result"]


def execute(
    client: ResearchClient, turn: dict[str, Any], *, key: str | None = None
) -> dict[str, Any]:
    return client.command(
        "orchestration.execute_turn",
        DriveTurnTarget(turn_id=turn["id"]),
        idempotency_key=key or new_id(),
    )["result"]


def fence(pipeline: Pipeline) -> dict[str, Any]:
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    return pipeline.client.command(
        "orchestration.fence",
        FenceOrchestration(
            campaign_id=binding["id"],
            epoch=1,
            reason="End this driver's admission authority",
        ),
        idempotency_key=new_id(),
        expected_revision=binding["revision"],
    )["result"]


def test_http_turn_recovers_committed_action_after_receipt_failure_without_replanning(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, action(pipeline))

    def failed(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Fixture controller died before drive receipt")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.runtime.service.orchestration, "_finish", failed)
        with pytest.raises(RuntimeError, match="drive receipt"):
            execute(drivers.control, turn, key="lost-execute-reply")
    assert len(pipeline.client.list(RecordQuery(kind="search_observation"))["items"]) == 1
    assert pipeline.client.get("drive_turn", turn["id"])["state"] == "prepared"
    reopened = LocalRuntime.open(pipeline.runtime.root)

    def cannot_dispatch(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Recovery must not deliver or replan an acknowledged action")

    with monkeypatch.context() as patch:
        patch.setattr(reopened.service.orchestration, "dispatch", cannot_dispatch)
        with (
            serving(create_app(reopened.service, reopened.credentials)) as url,
            ResearchClient.connect(url, drivers.token) as remote,
        ):
            recovered = execute(remote, turn, key="lost-execute-reply")
            assert execute(remote, turn, key="lost-execute-reply") == recovered
            assert execute(remote, turn) == recovered
    assert recovered["response"]["disposition"] == "accepted"
    assert len(pipeline.client.list(RecordQuery(kind="drive_receipt"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="search_observation"))["items"]) == 1
    binding = pipeline.client.get("orchestration_binding", pipeline.campaign["id"])
    assert binding["data"]["pending_turn_id"] is None and binding["data"]["next_ordinal"] == 2


def test_independent_runners_prepare_and_execute_one_ordinal(pipeline: Pipeline) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    command = action(pipeline)
    intent = PrepareDriveTurn(
        campaign_id=pipeline.campaign["id"],
        epoch=1,
        ordinal=1,
        configuration_digest=CONFIG,
        planning_watermark=0,
        action=command,
    )
    barrier = Barrier(2)

    def run(index: int) -> dict[str, Any]:
        runtime = LocalRuntime.open(pipeline.runtime.root)
        client = ResearchClient(
            LocalTransport(runtime, lambda: runtime.credentials.resolve(drivers.token))
        )
        barrier.wait(timeout=5)
        saved = prepare(pipeline, client, command, intent=intent, revision=1)
        return execute(client, saved)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(run, range(2)))
    assert outcomes[0] == outcomes[1]
    assert len(pipeline.client.list(RecordQuery(kind="drive_turn"))["items"]) == 1
    assert len(pipeline.client.list(RecordQuery(kind="search_observation"))["items"]) == 1


def test_unresolved_preflight_refusal_keeps_original_turn_and_can_recover(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, action(pipeline))

    def refused(*args: Any, **kwargs: Any) -> Any:
        raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Fixture preflight is unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.runtime.service.orchestration, "dispatch", refused)
        with pytest.raises(ResearchError) as failure:
            execute(drivers.control, turn, key="retry-original")
        assert failure.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    with pytest.raises(ResearchError) as unresolved:
        drivers.control.command(
            "orchestration.reconcile_turn",
            DriveTurnTarget(turn_id=turn["id"]),
            idempotency_key="reconcile-original",
        )
    assert unresolved.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    with pytest.raises(ResearchError) as busy:
        prepare(pipeline, drivers.control, action(pipeline))
    assert busy.value.code == ErrorCode.RESOURCE_UNAVAILABLE
    assert pipeline.client.get("drive_turn", turn["id"])["state"] == "prepared"
    assert (
        execute(drivers.control, turn, key="retry-original")["response"]["disposition"]
        == "accepted"
    )
    result = drivers.control.command(
        "orchestration.reconcile_turn",
        DriveTurnTarget(turn_id=turn["id"]),
        idempotency_key="reconcile-original",
    )["result"]
    assert result["turn"]["state"] == "settled"


def test_bound_drivers_cannot_bypass_saved_actions_or_expose_private_guard(
    pipeline: Pipeline,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    command = action(pipeline)
    with pytest.raises(ResearchError) as bare:
        drivers.control.command(
            command.operation,
            command.payload,
            idempotency_key="unguarded",
            expected_revision=command.expected_revision,
        )
    assert bare.value.code == ErrorCode.AUTHORIZATION_DENIED
    with pytest.raises(ValidationError):
        CommandContext.model_validate(
            {
                "principal": drivers.actor.model_dump(),
                "idempotency_key": "forged",
                "_drive_guard": {"turn_id": new_id()},
            }
        )
    assert (
        "_drive_guard"
        not in CommandContext(principal=drivers.actor, idempotency_key="plain").model_dump()
    )
    with pytest.raises(ResearchError) as wrong_route:
        prepare(pipeline, drivers.evaluator, command)
    assert wrong_route.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    turn = prepare(pipeline, drivers.control, command)
    with pytest.raises(ResearchError) as impersonation:
        execute(drivers.evaluator, turn)
    assert impersonation.value.code == ErrorCode.AUTHORIZATION_DENIED
    assert execute(drivers.control, turn)["response"]["disposition"] == "accepted"


def test_changed_ordinal_intent_conflicts_and_fenced_actor_cannot_create_new_effects(
    pipeline: Pipeline,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    command = action(pipeline)
    turn = prepare(pipeline, drivers.control, command)
    altered = PrepareDriveTurn(
        campaign_id=pipeline.campaign["id"],
        epoch=1,
        ordinal=1,
        configuration_digest=CONFIG,
        planning_watermark=0,
        action=command.model_copy(update={"idempotency_key": "different-command"}),
    )
    with pytest.raises(ResearchError) as changed:
        prepare(pipeline, drivers.control, command, intent=altered, revision=1)
    assert changed.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    fence(pipeline)
    outcome = execute(drivers.control, turn)
    assert outcome["response"]["disposition"] == "rejected"
    assert outcome["response"]["error"]["code"] == ErrorCode.STALE_FENCE
    assert pipeline.client.list(RecordQuery(kind="search_observation"))["items"] == []
    assert execute(drivers.control, turn) == outcome
    with pytest.raises(ResearchError) as retired:
        prepare(
            pipeline, drivers.control, command.model_copy(update={"idempotency_key": "new-effect"})
        )
    assert retired.value.code == ErrorCode.STALE_FENCE


def test_bound_control_and_evaluator_execute_real_artifact_preflight_and_native_receipt(
    pipeline: Pipeline,
) -> None:
    drivers = bind(pipeline)
    pipeline.worker.execute(pipeline.worker.claim(pipeline.ready())["id"])
    job = pipeline.ready()
    authorized = execute(
        drivers.control,
        prepare(
            pipeline,
            drivers.control,
            DriveAction(
                operation="evaluation.authorize",
                payload=AuthorizeEvaluation(
                    job_id=job["id"], evaluator_id=drivers.evaluator_actor.id
                ).model_dump(mode="json"),
                idempotency_key="bound-authorization",
                expected_revision=job["revision"],
            ),
        ),
    )
    assert authorized["response"]["disposition"] == "accepted"
    authority = authorized["response"]["result"]
    attempt = pipeline.worker.claim(pipeline.ready())
    pipeline.worker.execute(attempt["id"])
    completed = execute(
        drivers.evaluator,
        prepare(
            pipeline,
            drivers.evaluator,
            DriveAction(
                operation="evaluation.complete",
                payload=CompleteEvaluation(
                    request_id=authority["id"], attempt_id=attempt["id"]
                ).model_dump(mode="json"),
                idempotency_key="bound-receipt",
            ),
        ),
    )
    assert completed["response"]["disposition"] == "accepted"
    assert (
        pipeline.client.get("trial", pipeline.trial["id"])["data"]["evaluation_status"] == "valid"
    )
    with pipeline.runtime.database.connect() as connection:
        count = connection.execute(
            "SELECT count(*) FROM records WHERE kind='artifact_reference' "
            "AND state='retained' "
            "AND json_extract(payload,'$.retention.owner_kind')='artifact_read'"
        ).fetchone()[0]
    assert count == 0


def test_fence_between_pinned_preflight_and_target_commit_releases_only_owned_pins(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    drivers = bind(pipeline)
    pipeline.worker.execute(pipeline.worker.claim(pipeline.ready())["id"])
    job = pipeline.ready()
    turn = prepare(
        pipeline,
        drivers.control,
        DriveAction(
            operation="evaluation.authorize",
            payload=AuthorizeEvaluation(
                job_id=job["id"], evaluator_id=drivers.evaluator_actor.id
            ).model_dump(mode="json"),
            idempotency_key="fenced-authorization",
            expected_revision=job["revision"],
        ),
    )
    original = pipeline.runtime.database.execute
    fenced = False

    def execute_after_fence(context: Any, operation: str, *args: Any, **kwargs: Any) -> Any:
        nonlocal fenced
        if operation == "evaluation.authorize" and not fenced:
            fenced = True
            fence(pipeline)
        return original(context, operation, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.runtime.database, "execute", execute_after_fence)
        outcome = execute(drivers.control, turn)
    assert fenced and outcome["response"]["error"]["code"] == ErrorCode.STALE_FENCE
    assert pipeline.client.list(RecordQuery(kind="evaluation_request"))["items"] == []
    with pipeline.runtime.database.connect() as connection:
        count = connection.execute(
            "SELECT count(*) FROM records WHERE kind='artifact_reference' "
            "AND state='retained' "
            "AND json_extract(payload,'$.retention.owner_kind')='artifact_read'"
        ).fetchone()[0]
    assert count == 0


@pytest.mark.parametrize("pipeline", [{"limits": PROPOSAL_LIMITS}], indirect=True)
def test_bound_proposal_admission_uses_derived_source_pins_and_one_trial_graph(
    pipeline: Pipeline,
    tmp_path: Path,
) -> None:
    baseline(pipeline)
    requested, _ = answered(pipeline, tmp_path)
    drivers = bind(pipeline)
    intent = admission_intent(pipeline, requested["request"]["id"])
    turn = prepare(
        pipeline,
        drivers.control,
        DriveAction(
            operation="search.admit_response",
            payload=intent.model_dump(mode="json"),
            idempotency_key="bound-admission",
            expected_revision=1,
        ),
    )
    outcome = execute(drivers.control, turn)
    assert outcome["response"]["disposition"] == "accepted"
    assert outcome["response"]["result"]["trial"]["state"] == "queued"
    fence(pipeline)
    assert execute(drivers.control, turn) == outcome
    assert len(pipeline.client.list(RecordQuery(kind="trial"))["items"]) == 2


def test_saved_acknowledgment_does_not_restore_removed_target_permissions(
    pipeline: Pipeline,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    turn = prepare(pipeline, drivers.control, action(pipeline))
    execute(drivers.control, turn, key="original-delivery")
    restricted = drivers.actor.model_copy(
        update={"scopes": frozenset({"read", "orchestration.drive"})}
    )
    client = ResearchClient(LocalTransport(pipeline.runtime, lambda: restricted))
    for operation, key in (
        ("orchestration.execute_turn", "original-delivery"),
        ("orchestration.reconcile_turn", "restricted-reconcile"),
    ):
        with pytest.raises(ResearchError) as denied:
            client.command(operation, DriveTurnTarget(turn_id=turn["id"]), idempotency_key=key)
        assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_target_and_delivery_keys_are_distinct_without_poisoning_saved_action(
    pipeline: Pipeline,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    command = action(pipeline)
    turn = prepare(pipeline, drivers.control, command)
    with pytest.raises(ResearchError) as collision:
        execute(drivers.control, turn, key=command.idempotency_key)
    assert collision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    another = PrepareDriveTurn(
        campaign_id=pipeline.campaign["id"],
        epoch=1,
        ordinal=2,
        configuration_digest=CONFIG,
        planning_watermark=0,
        action=command.model_copy(update={"idempotency_key": "another-target"}),
    )
    with pytest.raises(ResearchError) as future_collision:
        drivers.control.command(
            "orchestration.prepare_turn",
            another,
            idempotency_key=command.idempotency_key,
            expected_revision=2,
        )
    assert future_collision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert execute(drivers.control, turn)["response"]["disposition"] == "accepted"


def test_binding_cannot_adopt_operator_or_reuse_retired_driver(pipeline: Pipeline) -> None:
    drivers = bind(pipeline)
    fence(pipeline)
    campaign = pipeline.client.get("campaign", pipeline.campaign["id"])
    for actor_id, expected in (
        (pipeline.runtime.principal().id, ErrorCode.AUTHORIZATION_DENIED),
        (drivers.actor.id, ErrorCode.REVISION_CONFLICT),
    ):
        with pytest.raises(ResearchError) as denied:
            pipeline.client.command(
                "orchestration.bind",
                BindOrchestration(
                    campaign_id=campaign["id"],
                    configuration_digest=CONFIG,
                    routes={"control": actor_id},
                ),
                idempotency_key=new_id(),
                expected_revision=campaign["revision"],
            )
        assert denied.value.code == expected
    binding = pipeline.client.get("orchestration_binding", campaign["id"])
    assert binding["state"] == "fenced" and binding["data"]["epoch"] == 1


@pytest.mark.parametrize("fenced", [False, True])
def test_bare_bound_http_upload_calls_cannot_change_staging_bytes_or_publish_generation(
    pipeline: Pipeline,
    fenced: bool,
) -> None:
    context = CommandContext(
        principal=pipeline.runtime.principal(), idempotency_key="unbound-upload"
    )
    service = pipeline.runtime.artifacts
    payload = b"Controller-owned staged bytes"
    pending = []
    for index in range(2):
        upload = service.begin_upload(
            context.model_copy(update={"idempotency_key": f"upload-{index}"}),
            BeginUpload(digest=content_digest(payload), size_bytes=len(payload)),
        )["result"]
        pending.append(upload["id"])
    service.upload_part(context, pending[1], 0, payload)
    before = [service.upload_status(context, identifier) for identifier in pending]
    drivers = bind(pipeline)
    if fenced:
        fence(pipeline)
    with (
        serving(create_app(pipeline.runtime.service, pipeline.runtime.credentials)) as url,
        httpx.Client(base_url=url, headers={"Authorization": "Bearer " + drivers.token}) as http,
    ):
        appended = http.put(f"/v1/artifact-uploads/{pending[0]}/parts", content=payload)
        finalized = http.post(
            f"/v1/artifact-uploads/{pending[1]}/finalize",
            headers={"Idempotency-Key": "forbidden-finalize"},
        )
    assert appended.status_code == finalized.status_code == 403
    assert [service.upload_status(context, identifier) for identifier in pending] == before
    assert before[0]["transfer"]["received_bytes"] == 0
    assert before[1]["transfer"]["received_bytes"] == len(payload)
    assert not before[1]["transfer"]["published"]


def test_durable_driver_retries_same_ordinal_without_calling_planner_again(
    pipeline: Pipeline,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evaluate(pipeline, pipeline.trial)
    drivers = bind(pipeline)
    driver = DurableTurnDriver(
        drivers.control, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
    )
    calls = 0

    def plan() -> DriveAction:
        nonlocal calls
        calls += 1
        return action(pipeline)

    def lost(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError("Fixture receipt gap")

    with monkeypatch.context() as patch:
        patch.setattr(pipeline.runtime.service.orchestration, "_finish", lost)
        with pytest.raises(RuntimeError, match="receipt gap"):
            driver.advance_turn(1, plan)
    recovered = DurableTurnDriver(
        drivers.control, pipeline.campaign["id"], epoch=1, configuration_digest=CONFIG
    )
    result = recovered.advance_turn(1, plan)
    assert recovered.advance_turn(1, plan) == result
    assert calls == 1
    assert result["turn"]["state"] == "settled"
    assert len(pipeline.client.list(RecordQuery(kind="search_observation"))["items"]) == 1


@pytest.mark.parametrize("already_used", [False, True])
def test_preparation_rejects_current_or_previous_delivery_key_as_target(
    pipeline: Pipeline,
    already_used: bool,
) -> None:
    drivers = bind(pipeline)
    command = action(pipeline).model_copy(update={"idempotency_key": "reserved-delivery-key"})
    intent = PrepareDriveTurn(
        campaign_id=pipeline.campaign["id"],
        epoch=1,
        ordinal=1,
        configuration_digest=CONFIG,
        planning_watermark=0,
        action=command,
    )
    if already_used:
        invalid = intent.model_copy(
            update={
                "configuration_digest": content_digest(b"wrong-config"),
                "action": command.model_copy(update={"idempotency_key": "separate-target"}),
            }
        )
        with pytest.raises(ResearchError) as rejected:
            drivers.control.command(
                "orchestration.prepare_turn",
                invalid,
                idempotency_key="reserved-delivery-key",
                expected_revision=1,
            )
        assert rejected.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    with pytest.raises(ResearchError) as collision:
        drivers.control.command(
            "orchestration.prepare_turn",
            intent,
            idempotency_key="new-delivery-key" if already_used else "reserved-delivery-key",
            expected_revision=1,
        )
    assert collision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert pipeline.client.list(RecordQuery(kind="drive_turn"))["items"] == []
    assert (
        pipeline.client.get("orchestration_binding", pipeline.campaign["id"])["data"][
            "pending_turn_id"
        ]
        is None
    )
