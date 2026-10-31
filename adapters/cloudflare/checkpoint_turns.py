"""Private recovery maintenance reuses saved-turn semantics without an action dispatcher."""

from typing import Any, Literal

from research_swarm.control.orchestration import OrchestrationControl
from research_swarm.control.orchestration_models import CloseDriveTurn, DriveTurnTarget
from research_swarm.control.recovery_turns import recovery_turn_context
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.types import ID, Revision, VersionedRecord
from research_swarm.state.database import Database, encode

if __package__:
    from .checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_source import parse_checkpoint_request
    from .checkpoint_target import load_target
    from .exclusive import exclusive
else:
    from checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from checkpoint_import import verify_target_configuration
    from checkpoint_source import parse_checkpoint_request
    from checkpoint_target import load_target
    from exclusive import exclusive


class CheckpointTurnCommand(VersionedRecord):
    request_id: ID
    expected_revision: Revision | None = None
    operation: Literal["orchestration.reconcile_turn", "orchestration.close_turn"]
    payload: dict[str, Any]


def no_dispatch(*args):
    raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Recovery cannot execute saved actions")


async def reconcile_target_turn(
    state, open_store, manifest, secrets, *, identity, credential, target_json, request_json, crypto
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    binding = custody.binding
    issuer = verify_target_configuration(
        manifest,
        secrets,
        binding.intent,
        identity=identity,
        credential=credential,
        target_seed=custody.seed,
        expected_source=binding.expected_source,
        expected_custody=binding.expected_custody,
        crypto=crypto,
    )
    request = parse_checkpoint_request(request_json, CheckpointTurnCommand)
    model = CloseDriveTurn if request.operation == "orchestration.close_turn" else DriveTurnTarget
    try:
        intent = model.model_validate(request.payload)
    except ValueError:
        raise ResearchError(ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid recovery turn") from None
    context = CommandContext(
        principal=issuer,
        idempotency_key="logical-turn:" + request.request_id,
        expected_revision=request.expected_revision,
    )
    verifier = HostedRecoveryVerifier(manifest, binding)

    async def reconcile():
        store = open_store()
        manifest.require_store(store.identity)

        def commit(sql):
            # This precedes even cached replies; imported credentials do not authorize it.
            verifier.read_admission(sql, authority=store.identity, context=context, crypto=crypto)
            database = Database.from_store(
                AdmissionStore(store.identity, sql), recovery_verifier=verifier
            )
            guarded = recovery_turn_context(
                database,
                context,
                request.operation,
                intent.model_dump(mode="json"),
                crypto_provider=crypto,
            )
            control = OrchestrationControl(database, no_dispatch, crypto_provider=crypto)
            if request.operation == "orchestration.close_turn":
                result = control.close(guarded, intent)
            else:
                result = control.reconcile(guarded, intent)
            verifier.read_admission(sql, authority=store.identity, context=context, crypto=crypto)
            return encode(result)

        return store.write(commit)

    return await exclusive(state, reconcile)
