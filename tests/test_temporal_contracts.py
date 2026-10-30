from dataclasses import replace

import pytest
from temporalio.api.failure.v1 import Failure
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from research_swarm.domain.canonical import content_digest
from research_swarm.domain.types import new_id
from research_swarm.orchestration.temporal.activities import TemporalActivities
from research_swarm.orchestration.temporal.models import (
    BindingReference,
    WorkflowInput,
    WorkReference,
)
from research_swarm.orchestration.temporal.runtime import DATA_CONVERTER


def reference() -> BindingReference:
    return BindingReference(
        new_id(), new_id(), 1, content_digest(b"fixture"), {"control": new_id()}
    )


def test_continued_work_cannot_change_its_campaign_binding() -> None:
    first, second = reference(), reference()
    with pytest.raises(ValueError, match="same binding"):
        WorkflowInput(
            first, pending=WorkReference(second, "turn", new_id(), second.actors["control"])
        )


@pytest.mark.asyncio
async def test_activity_rejects_foreign_workflow_before_using_registered_credentials() -> None:
    binding = reference()
    environment = ActivityEnvironment()
    environment.info = replace(environment.info, workflow_id="foreign-fixture")
    with pytest.raises(ApplicationError) as denied:
        await environment.run(TemporalActivities({}).inspect, binding)
    assert denied.value.type == "AUTHORIZATION_DENIED"
    assert denied.value.non_retryable


def test_failure_converter_preserves_retry_contract_without_private_exception_data() -> None:
    sentinel = "private-fixture-do-not-publish"
    try:
        try:
            raise ValueError(sentinel)
        except ValueError as cause:
            raise ApplicationError(
                sentinel, {"secret": sentinel}, type="AUTHORIZATION_DENIED", non_retryable=True
            ) from cause
    except ApplicationError as error:
        failure = Failure()
        DATA_CONVERTER.failure_converter.to_failure(
            error, DATA_CONVERTER.payload_converter, failure
        )
    assert sentinel.encode() not in failure.SerializeToString()
    assert failure.stack_trace == failure.cause.stack_trace == ""
    assert failure.application_failure_info.type == "AUTHORIZATION_DENIED"
    assert failure.application_failure_info.non_retryable
    assert not failure.application_failure_info.HasField("details")
    assert failure.cause.application_failure_info.type == "ResearchAdapterError"
