"""Private source sealing and signing with operator-retained custody; no election."""

import re
from dataclasses import dataclass

from research_swarm.checkpoints.capture import CaptureLimits, preflight_rows
from research_swarm.checkpoints.image_format import sign_manifest
from research_swarm.control.backup import BackupControl
from research_swarm.crypto import public_key
from research_swarm.domain.backup import BackupCustodyPin, RegisterBackupCustody, SealWorkspace
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import ID, Digest, Revision, VersionedRecord, new_id
from research_swarm.state.database import Database, Transaction
from research_swarm.state.time_providers import TimeProviders

if __package__:
    from ..platform.deployment import secret_digest
    from ..platform.exclusive import exclusive
    from .checkpoint_anchor import register_anchor_in, require_anchor_in
    from .checkpoint_capture import capture_checkpoint, require_source_operator_in
    from .checkpoint_schema import require_checkpoint_schema
    from .checkpoint_wire import encode_checkpoint
else:
    from checkpoint_anchor import register_anchor_in, require_anchor_in
    from checkpoint_capture import capture_checkpoint, require_source_operator_in
    from checkpoint_schema import require_checkpoint_schema
    from checkpoint_wire import encode_checkpoint
    from deployment import secret_digest
    from exclusive import exclusive


class SourceBinding(VersionedRecord):
    deployment_digest: Digest
    authority: WitnessAuthorityScope
    pin: BackupCustodyPin


class SealCheckpoint(VersionedRecord):
    request_id: ID
    expected_revision: Revision
    seal: SealWorkspace


class CaptureCheckpoint(VersionedRecord):
    backup_id: ID


@dataclass(frozen=True, repr=False)
class SourceCustody:
    binding: SourceBinding
    seed: bytes

    def __repr__(self):
        return "SourceCustody(<redacted>)"


def load_source(raw_json, manifest, secrets, *, credential, crypto):
    """The optional secret is read only after private bootstrap authentication."""
    secrets.require_bootstrap(credential)
    try:
        if (
            secret_digest("bootstrap", credential) != manifest.bootstrap_credential_digest
            or type(raw_json) is not str
            or len(raw_json) > 8192
            or len(raw_json.encode()) > 8192
        ):
            raise ValueError("Invalid source configuration")
        raw = strict_json_loads(raw_json)
        seed_hex = raw.pop("custody_seed_hex")
        if type(seed_hex) is not str or re.fullmatch(r"[0-9a-f]{64}", seed_hex) is None:
            raise ValueError("Invalid custody key")
        binding = SourceBinding.model_validate(raw)
        seed, scope, pin = bytes.fromhex(seed_hex), manifest.configuration.authority, binding.pin
        if (
            binding.deployment_digest != manifest.digest
            or binding.authority != scope
            or (pin.workspace_id, pin.controller_id, pin.control_epoch)
            != (scope.workspace_id, scope.controller_id, scope.control_epoch)
            or seed in {secrets.assignment_seed, secrets.dispatch_key}
            or public_key(crypto, seed).hex() != pin.public_key
        ):
            raise ValueError("Source custody differs")
        return SourceCustody(binding, seed)
    except Exception:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid checkpoint source configuration"
        ) from None


def parse_checkpoint_request(raw_json, model):
    try:
        if type(raw_json) is not str or len(raw_json) > 16384 or len(raw_json.encode()) > 16384:
            raise ValueError("Invalid request bounds")
        return model.model_validate(strict_json_loads(raw_json))
    except Exception:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid checkpoint request"
        ) from None


class SourceStore:
    """Bind shared command transactions to one admitted outer source write."""

    def __init__(self, store, sql, manifest, token, custody, *, crypto=None, recovery_binding=None):
        self.store, self.manifest, self.token, self.custody = store, manifest, token, custody
        self.sql = sql
        self.identity = store.identity
        self.crypto, self.recovery_binding = crypto, recovery_binding

    def _admit(self, sql, callback):
        manifest, scope = self.manifest, self.manifest.configuration.authority
        manifest.require_store(self.store.identity)
        principal = require_source_operator_in(sql, manifest, self.token)
        require_checkpoint_schema(
            sql, manifest, crypto=self.crypto, recovery_binding=self.recovery_binding
        )
        preflight_rows(sql, CaptureLimits())
        context = CommandContext(principal=principal, idempotency_key="checkpoint-source-admission")
        provider = TimeProviders(self.identity).load(Transaction(sql, context), scope)
        if provider.data.source != manifest.configuration.source:
            raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint witness source differs")
        rows = sql.execute(
            "SELECT state,payload FROM records WHERE workspace_id=? AND kind='backup_custody' "
            "AND json_extract(payload,'$.control_epoch')=? LIMIT 2",
            (scope.workspace_id, scope.control_epoch),
        ).fetchall()
        if rows and (
            len(rows) != 1
            or rows[0]["state"] != "immutable"
            or BackupCustodyPin.model_validate_json(rows[0]["payload"]) != self.custody.binding.pin
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint custody is already pinned")
        require_anchor_in(sql, manifest, self.identity, self.custody.binding.pin)
        return callback(sql)

    def read(self, callback):
        return self.sql.atomic(lambda sql: self._admit(sql, callback))

    def write(self, callback):
        return self.sql.atomic(lambda sql: self._admit(sql, callback))


def source_operator(manifest, secrets):
    item = next(
        item for item in manifest.credentials if item.principal.id == manifest.provisioning_actor_id
    )
    return item.principal, secrets.credentials[item.id]


async def seal_source(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    source_json,
    request_json,
    crypto,
    recovery_binding=None,
):
    custody = load_source(source_json, manifest, secrets, credential=credential, crypto=crypto)
    manifest.require_store(identity)
    request = parse_checkpoint_request(request_json, SealCheckpoint)
    scope = manifest.configuration.authority
    if (request.seal.expected_controller_id, request.seal.expected_control_epoch) != (
        scope.controller_id,
        scope.control_epoch,
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint seal authority differs")

    async def seal():
        principal, token = source_operator(manifest, secrets)
        store = open_store()
        manifest.require_store(store.identity)

        def commit(sql):
            bound = SourceStore(
                store,
                sql,
                manifest,
                token,
                custody,
                crypto=crypto,
                recovery_binding=recovery_binding,
            )
            fresh = (
                sql.execute(
                    "SELECT 1 FROM records WHERE workspace_id=? AND "
                    "((kind='workspace_gate' AND state!='active') OR "
                    "(kind='backup_custody' AND json_extract(payload,'$.control_epoch')=?)) "
                    "LIMIT 1",
                    (scope.workspace_id, scope.control_epoch),
                ).fetchone()
                is None
            )
            control = BackupControl(Database.from_store(bound))
            context = CommandContext(
                principal=principal,
                idempotency_key="checkpoint-seal:" + request.request_id,
                expected_revision=request.expected_revision,
            )
            sealed = control.seal(context, request.seal)
            registered = control.register_custody(
                context.model_copy(
                    update={
                        "idempotency_key": "checkpoint-custody:" + request.request_id,
                        "command_id": new_id(),
                    }
                ),
                RegisterBackupCustody(backup_id=request.seal.backup_id, pin=custody.binding.pin),
            )
            register_anchor_in(
                sql, manifest, store.identity, custody.binding.pin, context, allow_create=fresh
            )
            # Generated records and acknowledgments must fit too. Refusal rolls
            # back both phases instead of leaving an unexportable sealed source.
            capture_checkpoint(
                bound,
                manifest,
                token=token,
                backup_id=request.seal.backup_id,
                expected_custody=custody.binding.pin,
                crypto=crypto,
                recovery_binding=recovery_binding,
            )
            return {"schema_version": "1.0", "seal": sealed, "custody": registered}

        return store.write(commit)

    return await exclusive(state, seal)


async def capture_source(
    state,
    open_store,
    manifest,
    secrets,
    *,
    identity,
    credential,
    source_json,
    request_json,
    crypto,
    recovery_binding=None,
):
    custody = load_source(source_json, manifest, secrets, credential=credential, crypto=crypto)
    manifest.require_store(identity)
    request = parse_checkpoint_request(request_json, CaptureCheckpoint)

    async def capture():
        _, token = source_operator(manifest, secrets)
        image = capture_checkpoint(
            open_store(),
            manifest,
            token=token,
            backup_id=request.backup_id,
            expected_custody=custody.binding.pin,
            crypto=crypto,
            recovery_binding=recovery_binding,
        )
        signed = sign_manifest(image.manifest, custody.seed, crypto=crypto)
        return encode_checkpoint(signed, image.bodies)

    return await exclusive(state, capture)
