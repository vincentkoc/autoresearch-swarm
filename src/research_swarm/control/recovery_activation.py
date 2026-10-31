"""Atomic recovery activation effects; callers own runtime and byte verification."""

from research_swarm.domain.backup import RecoveryActivation, RecoveryAdmission
from research_swarm.domain.errors import ResearchError
from research_swarm.domain.recovery_runtime import HostedRecoveryActivation
from research_swarm.state.database import JsonObject, Transaction


def activate_in(
    transaction: Transaction, activation: RecoveryActivation | HostedRecoveryActivation
) -> JsonObject:
    transaction.observe_waits = False
    admission = RecoveryAdmission.model_validate(
        transaction.get("recovery_admission", activation.request.restore_id)["data"]
    )
    # Execution quiescence was proven independently of multi-stage trial completion.
    # Preserve ready stages, their trial projections and all queued funding.
    for identifier in admission.paused_campaign_ids:
        row = transaction.get("campaign", identifier)
        if row["state"] == "pausing":
            changed = transaction.update(
                "campaign",
                identifier,
                expected_revision=row["revision"],
                data=row["data"],
                state="paused",
            )
            transaction.event("campaign.paused", changed, {"mode": "recovery_quiescence"})
    recorded = transaction.insert(
        "recovery_activation",
        activation.request.restore_id,
        activation.model_dump(mode="json"),
        state="immutable",
    )
    gate = transaction.get("workspace_gate", transaction.workspace_id)
    transaction.update(
        "workspace_gate",
        transaction.workspace_id,
        expected_revision=gate["revision"],
        data=gate["data"],
        state="active",
    )
    transaction.event("workspace.recovery_activated", recorded, {})
    return recorded


def reject_activation(transaction: Transaction, error: ResearchError) -> None:
    # A temporary blocker must not consume the stable activation intent.
    raise error
