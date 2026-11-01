"""Private inactive import and verified target requests; activation stays unavailable."""

import re
from dataclasses import dataclass

from research_swarm.checkpoints.elections import verify_election_request
from research_swarm.crypto import sign
from research_swarm.domain.backup import BackupCustodyPin, SignedRestoreRequest
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.checkpoint_elections import (
    LogicalElectionRequest,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.checkpoint_images import LogicalImportIntent
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import WitnessAuthorityScope
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import encode

if __package__:
    from ..runtime.deployment import secret_digest
    from ..runtime.exclusive import exclusive
    from .checkpoint_import import (
        import_checkpoint,
        inspect_imported_target,
        verify_import_inputs,
        verify_target_configuration,
    )
    from .checkpoint_wire import decode_checkpoint, require_rpc_string
else:
    from checkpoint_import import (
        import_checkpoint,
        inspect_imported_target,
        verify_import_inputs,
        verify_target_configuration,
    )
    from checkpoint_wire import decode_checkpoint, require_rpc_string
    from deployment import secret_digest
    from exclusive import exclusive


class TargetBinding(VersionedRecord):
    intent: LogicalImportIntent
    expected_source: WitnessAuthorityScope
    expected_custody: BackupCustodyPin


@dataclass(frozen=True, repr=False)
class TargetCustody:
    binding: TargetBinding
    seed: bytes

    def __repr__(self):
        return "TargetCustody(<redacted>)"


def load_target(raw_json, manifest, secrets, *, credential):
    secrets.require_bootstrap(credential)
    try:
        if (
            secret_digest("bootstrap", credential) != manifest.bootstrap_credential_digest
            or type(raw_json) is not str
            or len(raw_json) > 16384
            or len(raw_json.encode()) > 16384
        ):
            raise ValueError("Invalid target configuration")
        raw = strict_json_loads(raw_json)
        seed_hex = raw.pop("custody_seed_hex")
        if type(seed_hex) is not str or re.fullmatch(r"[0-9a-f]{64}", seed_hex) is None:
            raise ValueError("Invalid target key")
        binding, seed = TargetBinding.model_validate(raw), bytes.fromhex(seed_hex)
        source, pin = binding.expected_source, binding.expected_custody
        if seed in {secrets.assignment_seed, secrets.dispatch_key} or (
            pin.workspace_id,
            pin.controller_id,
            pin.control_epoch,
        ) != (source.workspace_id, source.controller_id, source.control_epoch):
            raise ValueError("Invalid target custody")
        return TargetCustody(binding, seed)
    except Exception:
        raise ResearchError(
            ErrorCode.PROJECT_CONTRACT_INVALID, "Invalid checkpoint target configuration"
        ) from None


def retained_target_binding(raw_json, manifest, secrets, *, identity, crypto):
    """Load protected pins for ordinary reopen or successor export after activation."""
    if raw_json is None:
        return None
    custody = load_target(raw_json, manifest, secrets, credential=secrets.bootstrap)
    binding = custody.binding
    verify_target_configuration(
        manifest,
        secrets,
        binding.intent,
        identity=identity,
        credential=secrets.bootstrap,
        target_seed=custody.seed,
        expected_source=binding.expected_source,
        expected_custody=binding.expected_custody,
        crypto=crypto,
    )
    return binding


async def import_target(
    state, open_store, manifest, secrets, *, identity, credential, target_json, bundle_json, crypto
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    manifest.require_store(identity)
    require_rpc_string(bundle_json)
    envelope, bodies = decode_checkpoint(bundle_json)
    binding = custody.binding
    inputs = {
        "credential": credential,
        "target_seed": custody.seed,
        "envelope": envelope,
        "bodies": bodies,
        "expected_source": binding.expected_source,
        "expected_custody": binding.expected_custody,
        "crypto": crypto,
    }
    # Discard the precheck result: the actual importer independently verifies the
    # original bytes. A constructed VerifiedLogicalImage is never an authority.
    verify_import_inputs(manifest, secrets, binding.intent, identity=identity, **inputs)

    async def restore():
        store = open_store()
        manifest.require_store(store.identity)
        receipt = import_checkpoint(store, manifest, secrets, binding.intent, **inputs)
        return {"schema_version": "1.0", "receipt": receipt.model_dump(mode="json")}

    return await exclusive(state, restore)


async def request_target(
    state, open_store, manifest, secrets, *, identity, credential, target_json, crypto
):
    custody = load_target(target_json, manifest, secrets, credential=credential)
    binding = custody.binding
    verify_target_configuration(
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

    async def request():
        store = open_store()
        manifest.require_store(store.identity)

        def read(sql):
            receipt, marker = inspect_imported_target(
                sql, binding.intent, binding.expected_source, binding.expected_custody
            )
            native = receipt.claim_request()
            payload = LogicalElectionRequest(
                intent=binding.intent,
                source_authority=marker.source_authority,
                source_deployment_digest=marker.source_deployment_digest,
                signed_request=SignedRestoreRequest(
                    request=native,
                    signature=sign(crypto, custody.seed, native.digest.encode("ascii")).hex(),
                ),
            )
            signed = SignedLogicalElectionRequest(
                request=payload,
                signature=sign(crypto, custody.seed, payload.digest.encode("ascii")).hex(),
            )
            verify_election_request(
                signed,
                expected_intent=binding.intent,
                expected_backup_id=receipt.backup_id,
                expected_source=binding.expected_source,
                expected_deployment_digest=marker.source_deployment_digest,
                expected_custody=binding.expected_custody,
                crypto=crypto,
            )
            return encode(signed.model_dump(mode="json"))

        return store.read(read)

    return await exclusive(state, request)
