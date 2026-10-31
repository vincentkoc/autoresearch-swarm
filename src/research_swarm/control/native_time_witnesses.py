"""Native clock source primitive; network authentication and DO admission are separate."""

from __future__ import annotations

from research_swarm.control.authority_clock import AuthorityClock
from research_swarm.control.time_witnesses import WitnessSigner
from research_swarm.crypto import Ed25519Provider, public_key, resolve_crypto
from research_swarm.domain.clocks import ClockPolicy
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.time_witnesses import (
    SignedTimeWitness,
    TimeWitnessPayload,
    TimeWitnessRequest,
    WitnessAuthorityScope,
    WitnessSource,
    witness_key_id,
)
from research_swarm.workers.clocks import NATIVE_CLOCK_PROFILES, NativeClock, native_clock


class NativeWitnessSource:
    """One fixed authority scope and continuous clock origin, with a sticky fault latch.

    A transport must authenticate its pinned consumer before calling observe.
    Provider generation is signed evidence owned by the consumer, not source authority.
    """

    def __init__(
        self,
        authority: WitnessAuthorityScope,
        source_id: str,
        private_key: bytes,
        *,
        policy: ClockPolicy | None = None,
        clock: NativeClock | None = None,
        crypto_provider: Ed25519Provider | None = None,
    ) -> None:
        self._authority = WitnessAuthorityScope.model_validate(authority)
        native = clock or native_clock()
        if native.profile not in NATIVE_CLOCK_PROFILES:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Unsupported native witness clock"
            )
        qualified_policy = ClockPolicy.model_validate(policy or ClockPolicy())
        provider = resolve_crypto(crypto_provider)
        public = public_key(provider, private_key).hex()
        source = WitnessSource(
            id=source_id,
            public_key=public,
            key_id=witness_key_id(public),
            native_profile=native.profile,
            policy=qualified_policy,
        )
        self._signer = WitnessSigner(source, private_key, crypto_provider=provider)
        self._clock = AuthorityClock(native, qualified_policy)

    @property
    def authority(self) -> WitnessAuthorityScope:
        return self._authority

    @property
    def source(self) -> WitnessSource:
        return self._signer.source

    def observe(self, request: TimeWitnessRequest) -> SignedTimeWitness:
        request = TimeWitnessRequest.model_validate(request)
        if request.scope.authority != self.authority:
            raise ResearchError(ErrorCode.AUTHORIZATION_DENIED, "Witness authority scope differs")
        observation = self._clock.observe()
        return self._signer.sign(
            TimeWitnessPayload(
                request=request,
                source_id=self.source.id,
                source_epoch=observation.epoch,
                native_profile=observation.profile,
                policy=observation.policy,
                authority_time_ms=observation.milliseconds,
            )
        )
