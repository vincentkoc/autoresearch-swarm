"""Worker construction enforces bounded failure information in provider histories."""

from temporalio.api.failure.v1 import Failure
from temporalio.client import Client
from temporalio.converter import DataConverter, DefaultFailureConverter, PayloadConverter
from temporalio.worker import Worker

from research_swarm.domain.errors import ErrorCode
from research_swarm.orchestration.temporal.activities import TemporalActivities
from research_swarm.orchestration.temporal.workflow import CampaignWorkflow


class SafeFailureConverter(DefaultFailureConverter):
    def to_failure(
        self, exception: BaseException, payload_converter: PayloadConverter, failure: Failure
    ) -> None:
        super().to_failure(exception, payload_converter, failure)
        self._sanitize(failure)

    @classmethod
    def _sanitize(cls, failure: Failure) -> None:
        allowed = {code.value for code in ErrorCode} | {
            "ResearchTransportError",
            "ResearchAdapterError",
        }
        failure.message = "Research delivery failed"
        failure.stack_trace = ""
        failure.source = ""
        failure.ClearField("encoded_attributes")
        if failure.HasField("application_failure_info"):
            info = failure.application_failure_info
            if info.type not in allowed:
                info.type = "ResearchAdapterError"
            failure.message = "Research delivery failed: " + info.type
            info.ClearField("details")
        if failure.HasField("canceled_failure_info"):
            failure.canceled_failure_info.ClearField("details")
        if failure.HasField("timeout_failure_info"):
            failure.timeout_failure_info.ClearField("last_heartbeat_details")
        if failure.HasField("reset_workflow_failure_info"):
            failure.reset_workflow_failure_info.ClearField("last_heartbeat_details")
        if failure.HasField("cause"):
            cls._sanitize(failure.cause)


DATA_CONVERTER = DataConverter(failure_converter_class=SafeFailureConverter)


def create_worker(client: Client, *, task_queue: str, activities: TemporalActivities) -> Worker:
    if not isinstance(client.data_converter.failure_converter, SafeFailureConverter):
        raise ValueError(
            "Connect Temporal with the research DATA_CONVERTER before starting its worker"
        )
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[CampaignWorkflow],
        activities=[activities.bootstrap, activities.inspect, activities.execute, activities.wait],
    )
