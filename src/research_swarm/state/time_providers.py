"""Transactional clock-source lifecycle. No transport or runtime clock activation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from research_swarm.control.time_witnesses import (
    AuthenticatedSourceMismatch,
    PendingTimeWitness,
    VerifiedTimeWitness,
)
from research_swarm.crypto import Ed25519Provider
from research_swarm.domain.canonical import record_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext
from research_swarm.domain.time_providers import (
    TimeProviderData,
    TimeProviderFence,
    TimeProviderGeneration,
    TimeSourceRetirement,
    source_origin_id,
)
from research_swarm.domain.time_witnesses import (
    SignedTimeWitness,
    TimeWitnessRequest,
    WitnessAuthorityScope,
    WitnessScope,
    WitnessSource,
)
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import JsonObject, Transaction
from research_swarm.state.sql import StoreIdentity
from research_swarm.state.workspace_gate import require_workspace_mutation


def provider_id(authority: WitnessAuthorityScope) -> str:
    return authority.activation_id


@dataclass(frozen=True)
class ProviderSnapshot:
    revision: int
    state: Literal["unqualified", "qualified"]
    data: TimeProviderData

    @property
    def id(self) -> str:
        return provider_id(self.data.scope.authority)


@dataclass(frozen=True)
class WitnessOperationBinding:
    store: StoreIdentity
    actor_id: str
    idempotency_key: str
    expected: ProviderSnapshot
    source: WitnessSource
    request: TimeWitnessRequest


@dataclass(frozen=True)
class ProviderDiscontinuity:
    operation: WitnessOperationBinding
    observed: SignedTimeWitness


class ProviderChanged(ResearchError):
    def __init__(self, evidence: ProviderDiscontinuity) -> None:
        super().__init__(ErrorCode.STALE_FENCE, "Admitted time source changed")
        self.evidence = evidence


def _snapshot(row: JsonObject) -> ProviderSnapshot:
    if row["state"] not in {"qualified", "unqualified"}:
        raise ResearchError(ErrorCode.STALE_FENCE, "Time provider state is invalid")
    data = TimeProviderData.model_validate(row["data"])
    if row["id"] != provider_id(data.scope.authority):
        raise ResearchError(ErrorCode.STALE_FENCE, "Time provider identity differs")
    return ProviderSnapshot(row["revision"], row["state"], data)


class TimeProviders:
    """Internal state port; callers own authorization and the exclusive operation turn.

    The supplied store identity must be the actual command authority. All writes
    also enforce the workspace gate, including direct query-triggered fences.
    """

    def __init__(
        self, store: StoreIdentity, *, crypto_provider: Ed25519Provider | None = None
    ) -> None:
        self.store = store
        self.crypto_provider = crypto_provider

    @staticmethod
    def _authority(transaction: Transaction, scope: WitnessAuthorityScope) -> None:
        current = transaction.connection.execute(
            "SELECT controller_id,control_epoch FROM workspaces WHERE id=?",
            (transaction.workspace_id,),
        ).fetchone()
        if current is None or (
            scope.workspace_id != transaction.workspace_id
            or scope.controller_id != current["controller_id"]
            or scope.control_epoch != current["control_epoch"]
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Time provider authority changed")

    def load(self, transaction: Transaction, authority: WitnessAuthorityScope) -> ProviderSnapshot:
        self._authority(transaction, authority)
        snapshot = _snapshot(transaction.get("time_provider", provider_id(authority)))
        if snapshot.data.scope.authority != authority:
            raise ResearchError(ErrorCode.STALE_FENCE, "Time provider activation differs")
        return snapshot

    def register(
        self, transaction: Transaction, authority: WitnessAuthorityScope, source: WitnessSource
    ) -> ProviderSnapshot:
        self._authority(transaction, authority)
        if transaction.context.expected_revision != 0:
            raise ResearchError(
                ErrorCode.REVISION_CONFLICT, "Provider registration requires revision zero"
            )
        require_workspace_mutation(
            transaction.connection, transaction.workspace_id, "time_provider.register"
        )
        data = TimeProviderData(
            scope=WitnessScope(**authority.model_dump(), provider_generation=new_id()),
            source=source,
        )
        row = transaction.insert(
            "time_provider",
            provider_id(authority),
            data.model_dump(mode="json"),
            state="unqualified",
        )
        self._generation(transaction, data)
        transaction.event("time_provider.registered", row, {})
        return _snapshot(row)

    def prepare(
        self,
        context: CommandContext,
        expected: ProviderSnapshot,
        operation: str,
        payload: Mapping[str, Any],
        *,
        source: WitnessSource | None = None,
    ) -> tuple[WitnessOperationBinding, PendingTimeWitness]:
        """Bind the complete caller/operation without changing command hash preimages."""
        source = source or expected.data.source
        digest = self._digest(context, expected, operation, payload)
        pending = PendingTimeWitness(
            source,
            expected.data.scope,
            context.command_id,
            digest,
            crypto_provider=self.crypto_provider,
        )
        return WitnessOperationBinding(
            self.store,
            context.principal.id,
            context.idempotency_key,
            expected,
            source,
            pending.request,
        ), pending

    def _digest(
        self,
        context: CommandContext,
        expected: ProviderSnapshot,
        name: str,
        payload: Mapping[str, Any],
    ) -> str:
        return record_digest(
            "research.time-witness-operation.v1",
            {
                "store": {"backend": self.store.backend, "location": self.store.location},
                "workspace_id": context.principal.workspace_id,
                "actor_id": context.principal.id,
                "command_id": context.command_id,
                "idempotency_key": context.idempotency_key,
                "operation": name,
                "expected_revision": context.expected_revision,
                "provider_revision": expected.revision,
                "payload": dict(payload),
            },
        )

    def _binding(
        self,
        transaction: Transaction,
        operation: WitnessOperationBinding,
        name: str,
        payload: Mapping[str, Any],
    ) -> None:
        context = transaction.context
        if (
            operation.store != self.store
            or operation.actor_id != context.principal.id
            or operation.idempotency_key != context.idempotency_key
            or operation.request.operation_id != context.command_id
            or operation.request.scope != operation.expected.data.scope
            or operation.request.request_digest
            != self._digest(context, operation.expected, name, payload)
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Time witness operation differs")
        self._authority(transaction, operation.request.scope.authority)

    def _current(
        self,
        transaction: Transaction,
        operation: WitnessOperationBinding,
        name: str,
        payload: Mapping[str, Any],
    ) -> ProviderSnapshot:
        self._binding(transaction, operation, name, payload)
        current = self.load(transaction, operation.request.scope.authority)
        if current != operation.expected:
            raise ResearchError(ErrorCode.REVISION_CONFLICT, "Time provider revision changed")
        return current

    @staticmethod
    def _proof(
        operation: WitnessOperationBinding,
        observation: VerifiedTimeWitness | AuthenticatedSourceMismatch,
    ) -> SignedTimeWitness:
        source = (
            observation.source
            if isinstance(observation, VerifiedTimeWitness)
            else observation.expected_source
        )
        if observation.witness.payload.request != operation.request or source != operation.source:
            raise ResearchError(ErrorCode.STALE_FENCE, "Time witness proof differs")
        return observation.witness

    def check(
        self,
        transaction: Transaction,
        operation: WitnessOperationBinding,
        observation: VerifiedTimeWitness | AuthenticatedSourceMismatch,
        *,
        name: str,
        payload: Mapping[str, Any],
    ) -> None:
        current = self._current(transaction, operation, name, payload)
        witness = self._proof(operation, observation)
        if current.state != "qualified" or operation.source != current.data.source:
            raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Time provider is unqualified")
        if (
            isinstance(observation, AuthenticatedSourceMismatch)
            or witness.payload.source_epoch != current.data.source_epoch
            or self._retirement(transaction, current.data.source, witness.payload.source_epoch)
            is not None
        ):
            raise ProviderChanged(ProviderDiscontinuity(operation, witness))

    def requalify(
        self,
        transaction: Transaction,
        operation: WitnessOperationBinding,
        proposed: VerifiedTimeWitness,
        *,
        name: str,
        payload: Mapping[str, Any],
    ) -> ProviderSnapshot:
        current = self._current(transaction, operation, name, payload)
        witness = self._proof(operation, proposed)
        require_workspace_mutation(
            transaction.connection, transaction.workspace_id, "time_provider.requalify"
        )
        if current.state != "unqualified":
            raise ResearchError(ErrorCode.INVALID_TRANSITION, "Time provider is already qualified")
        if transaction.context.expected_revision != current.revision:
            raise ResearchError(
                ErrorCode.REVISION_CONFLICT, "Time provider management revision differs"
            )
        self._require_unretired(transaction, proposed.source, witness.payload.source_epoch)
        data = TimeProviderData(
            scope=current.data.scope.model_copy(update={"provider_generation": new_id()}),
            source=proposed.source,
            source_epoch=witness.payload.source_epoch,
        )
        row = transaction.update(
            "time_provider",
            current.id,
            expected_revision=current.revision,
            data=data.model_dump(mode="json"),
            state="qualified",
        )
        self._generation(transaction, data, witness)
        transaction.event(
            "time_provider.requalified", row, {"generation": data.scope.provider_generation}
        )
        return _snapshot(row)

    def fence(
        self,
        transaction: Transaction,
        evidence: ProviderDiscontinuity,
        *,
        name: str,
        payload: Mapping[str, Any],
    ) -> ProviderSnapshot:
        operation = evidence.operation
        self._binding(transaction, operation, name, payload)
        require_workspace_mutation(
            transaction.connection, transaction.workspace_id, "time_provider.fence"
        )
        expected = operation.expected
        if expected.state != "qualified" or expected.data.source_epoch is None:
            raise ResearchError(ErrorCode.STALE_FENCE, "No admitted source can be retired")
        if (
            evidence.observed.payload.request != operation.request
            or operation.source != expected.data.source
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Source retirement proof differs")
        retired_origin = self._retirement(
            transaction, expected.data.source, expected.data.source_epoch
        )
        observed = evidence.observed.payload
        if (
            retired_origin is None
            and observed.source_epoch == expected.data.source_epoch
            and observed.source_id == expected.data.source.id
            and observed.native_profile == expected.data.source.native_profile
            and observed.policy == expected.data.source.policy
        ):
            raise ResearchError(ErrorCode.STALE_FENCE, "Source retirement requires a discontinuity")
        receipt = TimeProviderFence(
            provider=expected.data,
            provider_revision=expected.revision,
            origin_id=source_origin_id(expected.data.source, expected.data.source_epoch),
            observation_digest=record_digest(
                "research.time-source-discontinuity.v1", evidence.observed.model_dump(mode="json")
            ),
        )
        current = self.load(transaction, operation.request.scope.authority)
        if current != expected:
            if current == ProviderSnapshot(expected.revision + 1, "unqualified", expected.data):
                previous = transaction.get(
                    "time_provider_fence", expected.data.scope.provider_generation
                )
                if previous["state"] == "immutable" and previous["data"] == receipt.model_dump(
                    mode="json"
                ):
                    return current
            raise ResearchError(
                ErrorCode.REVISION_CONFLICT, "Source retirement belongs to an old provider"
            )
        retired = TimeSourceRetirement(
            provider=expected.data,
            provider_revision=expected.revision,
            observed=evidence.observed,
            retired_at=utc_now(),
        )
        if retired_origin is None:
            transaction.insert(
                "time_source_retirement",
                retired.origin_id,
                retired.model_dump(mode="json"),
                state="immutable",
            )
        transaction.insert(
            "time_provider_fence",
            expected.data.scope.provider_generation,
            receipt.model_dump(mode="json"),
            state="immutable",
        )
        row = transaction.update(
            "time_provider",
            current.id,
            expected_revision=current.revision,
            data=current.data.model_dump(mode="json"),
            state="unqualified",
        )
        transaction.event("time_provider.fenced", row, {"origin_id": retired.origin_id})
        return _snapshot(row)

    @staticmethod
    def _retirement(
        transaction: Transaction, source: WitnessSource, epoch: str
    ) -> TimeSourceRetirement | None:
        identity = source_origin_id(source, epoch)
        found = transaction.connection.execute(
            "SELECT 1 FROM records WHERE workspace_id=? AND kind='time_source_retirement' AND id=?",
            (transaction.workspace_id, identity),
        ).fetchone()
        if found is None:
            return None
        row = transaction.get("time_source_retirement", identity)
        retired = TimeSourceRetirement.model_validate(row["data"])
        if row["state"] != "immutable" or retired.origin_id != identity:
            raise ResearchError(ErrorCode.STALE_FENCE, "Time source retirement identity differs")
        return retired

    def _require_unretired(
        self, transaction: Transaction, source: WitnessSource, epoch: str
    ) -> None:
        if self._retirement(transaction, source, epoch) is not None:
            raise ResearchError(ErrorCode.STALE_FENCE, "Time source origin was retired")

    @staticmethod
    def _generation(
        transaction: Transaction, data: TimeProviderData, witness: SignedTimeWitness | None = None
    ) -> None:
        transaction.insert(
            "time_provider_generation",
            data.scope.provider_generation,
            TimeProviderGeneration(
                provider=data,
                admitted_by=transaction.context.principal.id,
                recorded_at=utc_now(),
                qualification=witness,
            ).model_dump(mode="json"),
            state="immutable",
            parent_id=provider_id(data.scope.authority),
        )
