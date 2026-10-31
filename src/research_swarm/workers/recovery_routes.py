"""Explicit destination selection preserves original worker installation and pending deliveries."""

from pathlib import Path

from pydantic import TypeAdapter

from research_swarm.control.recovery_routes import (
    verify_logical_recovery_route,
    verify_recovery_route,
)
from research_swarm.domain.backup import BackupCustodyPin
from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.recovery_routes import SignedWorkerRecoveryRoute
from research_swarm.domain.types import ID, Digest, VersionedRecord
from research_swarm.runtime import private_state_directory, read_private, write_private
from research_swarm.workers.journal import WorkerJournal
from research_swarm.workers.recovery import _authenticate, original_recovery_lock, original_stores
from research_swarm.workers.remote_enrollment import RemoteWorkerBundle, _installation
from research_swarm.workers.resources import MachineResources

MAX_ROUTE_BYTES = 4 * 1024 * 1024


def _verify_proof(signed: SignedWorkerRecoveryRoute, trusted_source: BackupCustodyPin) -> None:
    verifier = (
        verify_logical_recovery_route
        if signed.logical_election is not None
        else verify_recovery_route
    )
    verifier(signed, trusted_source)


class InstalledRecoveryRoute(VersionedRecord):
    signed: SignedWorkerRecoveryRoute
    trusted_source: BackupCustodyPin
    installation_digest: Digest
    journal_identity: ID
    registry_identity: ID

    @property
    def digest(self) -> str:
        return record_digest("research.installed-recovery-route.v1", self.model_dump(mode="json"))


def _bind(
    root: Path,
    signed: SignedWorkerRecoveryRoute,
    trusted_source: BackupCustodyPin,
    attempt_id: str,
    bundle: RemoteWorkerBundle,
    journal: WorkerJournal,
    resources: MachineResources,
) -> InstalledRecoveryRoute:
    _verify_proof(signed, trusted_source)
    route = signed.route
    entry = journal.get(attempt_id)
    _authenticate(entry, bundle)
    if (
        route.attempt_id != attempt_id
        or route.source_controller_url != bundle.controller_url
        or route.binding != bundle.worker.binding
        or route.principal_id != bundle.worker.principal_id
        or route.credential_id != bundle.credential_id
        or route.worker_data_digest
        != record_digest("research.recovery-worker.v1", bundle.worker.model_dump(mode="json"))
        or any(
            getattr(entry.dispatch, key) != getattr(route.binding, key)
            for key in type(route.binding).model_fields
        )
    ):
        raise ResearchError(
            ErrorCode.STALE_FENCE, "Recovery route does not bind the original installation"
        )
    return InstalledRecoveryRoute(
        signed=signed,
        trusted_source=trusted_source,
        installation_digest=record_digest(
            "research.recovery-installation.v1", _installation(root).model_dump(mode="json")
        ),
        journal_identity=journal.identity,
        registry_identity=resources.identity,
    )


def install_recovery_route(
    root: Path, signed: SignedWorkerRecoveryRoute, trusted_source: BackupCustodyPin
) -> InstalledRecoveryRoute:
    signed = SignedWorkerRecoveryRoute.model_validate(signed)
    trusted_source = BackupCustodyPin.model_validate(trusted_source)
    _verify_proof(signed, trusted_source)
    with original_recovery_lock(root, signed.route.attempt_id) as original:
        bundle, journal, resources, _ = original_stores(original)
        installed = _bind(
            original, signed, trusted_source, signed.route.attempt_id, bundle, journal, resources
        )
        content = canonical_bytes(installed.model_dump(mode="json"))
        if len(content) > MAX_ROUTE_BYTES:
            raise ResearchError(
                ErrorCode.PROJECT_CONTRACT_INVALID, "Installed recovery route exceeds its limit"
            )
        directory = original / "recovery-routes"
        with private_state_directory(directory):
            path = directory / (signed.route.route_id + ".json")
            if path.exists():
                if read_private(directory, path.name, limit=MAX_ROUTE_BYTES) != content:
                    raise ResearchError(
                        ErrorCode.IDEMPOTENCY_CONFLICT,
                        "Installed recovery route ID already differs",
                    )
            else:
                write_private(path, content)
        return installed


def load_recovery_route(
    root: Path,
    route_id: str,
    attempt_id: str,
    bundle: RemoteWorkerBundle,
    journal: WorkerJournal,
    resources: MachineResources,
) -> InstalledRecoveryRoute:
    route_id = TypeAdapter(ID).validate_python(route_id)
    saved = InstalledRecoveryRoute.model_validate(
        strict_json_loads(
            read_private(root / "recovery-routes", route_id + ".json", limit=MAX_ROUTE_BYTES)
        )
    )
    if saved.signed.route.route_id != route_id:
        raise ResearchError(ErrorCode.STALE_FENCE, "Installed recovery route identifier differs")
    expected = _bind(
        root, saved.signed, saved.trusted_source, attempt_id, bundle, journal, resources
    )
    if saved != expected:
        raise ResearchError(ErrorCode.STALE_FENCE, "Original native recovery custody changed")
    return saved
