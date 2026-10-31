"""Private target admission fences research atomically; it does not activate execution."""

from dataclasses import dataclass

from research_swarm.checkpoints.admission_state import (
    admit_in,
    reject_admission,
    research_ids,
)
from research_swarm.checkpoints.elections import verify_election
from research_swarm.control.recovery_proofs import verify_admission
from research_swarm.crypto import sign
from research_swarm.domain.backup import RecoveryAdmission
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.checkpoint_elections import SignedLogicalElection
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryGuard
from research_swarm.domain.types import utc_now
from research_swarm.state.database import Database, Transaction, encode
from research_swarm.state.recovery_elections import LOGICAL_ELECTION_KIND, retained_election
from research_swarm.state.recovery_verification import require_research_fences
from research_swarm.state.sql import StoreIdentity

if __package__:
    from .checkpoint_import import (
        inspect_imported_target,
        inspect_target_history,
        verify_target_configuration,
    )
    from .checkpoint_source import parse_checkpoint_request
    from .checkpoint_target import TargetBinding, load_target
    from .deployment import DeploymentManifest
    from .exclusive import exclusive
else:
    from checkpoint_import import (
        inspect_imported_target,
        inspect_target_history,
        verify_target_configuration,
    )
    from checkpoint_source import parse_checkpoint_request
    from checkpoint_target import TargetBinding, load_target
    from deployment import DeploymentManifest
    from exclusive import exclusive


def verify_bound_election(elected, binding, receipt, marker, crypto):
    return verify_election(
        elected,
        expected_intent=binding.intent,
        expected_backup_id=receipt.backup_id,
        expected_source=binding.expected_source,
        expected_deployment_digest=marker.source_deployment_digest,
        expected_custody=binding.expected_custody,
        crypto=crypto,
    )


@dataclass(frozen=True)
class HostedRecoveryVerifier:
    manifest: DeploymentManifest
    binding: TargetBinding

    def read_admission(self, sql, *, authority, context, crypto):
        binding = self.binding
        receipt, _, _ = inspect_target_history(
            sql,
            binding.intent,
            binding.expected_source,
            binding.expected_custody,
            phase="recovering",
        )
        row = sql.execute(
            "SELECT payload FROM records WHERE workspace_id=? AND kind='recovery_admission' "
            "AND id=? AND state='immutable' AND revision=1 AND typeof(payload)='text' "
            "AND length(CAST(payload AS BLOB))<=1048576",
            (receipt.workspace_id, receipt.restore_id),
        ).fetchone()
        if row is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Logical admission record changed")
        admission = RecoveryAdmission.model_validate_json(row[0])
        self.verify(
            sql,
            authority=authority,
            context=context,
            receipt=receipt,
            admission=admission,
            crypto=crypto,
        )
        return receipt, admission

    def verify(self, sql, *, authority, context, receipt, admission, crypto):
        manifest, binding = self.manifest, self.binding
        intent, target = binding.intent, manifest.configuration.authority
        manifest.require_store(authority)
        if (
            authority != StoreIdentity(intent.target_backend, intent.target_location)
            or manifest.digest != intent.target_deployment_digest
            or context.principal.workspace_id != target.workspace_id
            or (target.workspace_id, target.controller_id, target.control_epoch)
            != (receipt.workspace_id, receipt.controller_id, receipt.target_control_epoch)
            or admission.logical_election_digest is None
            or admission.operator_id != intent.operator_id
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Logical recovery deployment differs")
        saved, marker, _ = inspect_target_history(
            sql, intent, binding.expected_source, binding.expected_custody, phase="recovering"
        )
        if saved != receipt:
            raise ResearchError(ErrorCode.STALE_FENCE, "Logical recovery receipt differs")
        elected = retained_election(sql, receipt, admission)
        verify_bound_election(elected, binding, receipt, marker, crypto)
        verify_admission(admission, receipt, crypto_provider=crypto)
        require_research_fences(sql, receipt.workspace_id, admission)


class AdmissionStore:
    """Keep the shared command kernel inside the admitted outer write."""

    def __init__(self, identity, sql):
        self.identity, self.sql = identity, sql

    def read(self, callback):
        return self.sql.atomic(callback)

    def write(self, callback):
        return self.sql.atomic(callback)


async def admit_target(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    target_json,
    election_json,
    crypto,
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
    elected = parse_checkpoint_request(election_json, SignedLogicalElection)
    payload = elected.model_dump(mode="json")
    context = CommandContext(
        principal=issuer, idempotency_key="logical-admit:" + binding.intent.restore_id
    )
    verifier = HostedRecoveryVerifier(manifest, binding)

    async def admit():
        store = open_store()
        manifest.require_store(store.identity)

        def commit(sql):
            bound = AdmissionStore(store.identity, sql)
            database = Database.from_store(bound, recovery_verifier=verifier)
            existing = sql.execute(
                "SELECT 1 FROM records WHERE workspace_id=? AND kind='recovery_admission' AND id=?",
                (issuer.workspace_id, binding.intent.restore_id),
            ).fetchone()
            if existing is not None:
                receipt, admitted = verifier.read_admission(
                    sql, authority=store.identity, context=context, crypto=crypto
                )
                if admitted.logical_election_digest != elected.digest:
                    raise ResearchError(ErrorCode.IDEMPOTENCY_CONFLICT, "Recovery election differs")
                reply = database.replay(context, "workspace.begin_recovery", payload, "*")
                if (
                    reply is None
                    or reply.get("disposition") != "accepted"
                    or reply.get("result")
                    != (Transaction(sql, context).get("recovery_admission", receipt.restore_id))
                ):
                    raise ResearchError(ErrorCode.STALE_FENCE, "Recovery acknowledgment differs")
                return encode(reply)

            receipt, marker = inspect_imported_target(
                sql, binding.intent, binding.expected_source, binding.expected_custody
            )
            verify_bound_election(elected, binding, receipt, marker, crypto)
            bindings, campaigns = research_ids(sql, issuer.workspace_id)
            admission = RecoveryAdmission(
                signed_claim=elected.election.signed_claim,
                logical_election_digest=elected.digest,
                restoration_digest=receipt.digest,
                orchestration_campaign_ids=bindings,
                paused_campaign_ids=campaigns,
                operator_id=issuer.id,
                admitted_at=utc_now(),
                target_signature="0" * 128,
            )
            admission = admission.model_copy(
                update={
                    "target_signature": sign(
                        crypto, custody.seed, admission.proof_digest.encode("ascii")
                    ).hex()
                }
            )
            verify_admission(admission, receipt, crypto_provider=crypto)
            tx = Transaction(sql, context)
            gate = tx.get("workspace_gate", issuer.workspace_id)
            watermark = (
                sql.execute(
                    "SELECT next_sequence FROM workspaces WHERE id=?", (issuer.workspace_id,)
                ).one()[0]
                - 1
            )
            context._recovery_guard = RecoveryGuard(
                authority=(store.identity.backend, store.identity.location),
                request_digest=record_digest(
                    "research.command",
                    {
                        "operation": "workspace.begin_recovery",
                        "expected_revision": context.expected_revision,
                        "payload": payload,
                    },
                ),
                restore_id=receipt.restore_id,
                restoration_digest=receipt.digest,
                custody_pin_digest=record_digest(
                    "research.backup-custody-pin.v1",
                    binding.expected_custody.model_dump(mode="json"),
                ),
                target_public_key=binding.intent.target_public_key,
                gate_revision=gate["revision"],
                control_epoch=receipt.target_control_epoch,
                watermark=watermark,
            )

            def effects(transaction):
                transaction.insert(
                    LOGICAL_ELECTION_KIND, receipt.restore_id, payload, state="immutable"
                )
                return admit_in(transaction, admission)

            reply = database.execute(
                context,
                "workspace.begin_recovery",
                payload,
                "*",
                effects,
                on_rejection=reject_admission,
            )
            verifier.verify(
                sql,
                authority=store.identity,
                context=context,
                receipt=receipt,
                admission=admission,
                crypto=crypto,
            )
            return encode(reply)

        return store.write(commit)

    return await exclusive(state, admit)
