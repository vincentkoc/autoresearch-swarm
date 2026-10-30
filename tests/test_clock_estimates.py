from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from fractions import Fraction

import pytest

from research_swarm.domain.clocks import (
    AssignmentTimePayload,
    ClockChallenge,
    ClockPolicy,
    ClockReplyPayload,
    timed_assignment_digest,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.workers.clock_estimates import (
    NS_PER_MS,
    ClockEstimate,
    ClockSampler,
    timestamp_ms,
)
from research_swarm.workers.clock_signatures import (
    ClockSigner,
    ClockVerifier,
    VerifiedAssignmentTime,
    clock_signing_bytes,
)
from research_swarm.workers.clocks import ClockReading
from tests.test_assignment_signatures import (
    NOW,
    PRIVATE_KEY,
    assignment_payload,
    identifier,
    signer,
)


def fixture(
    *,
    policy: ClockPolicy | None = None,
    roundtrip_ms: int = 100,
) -> tuple[ClockEstimate, VerifiedAssignmentTime, ClockSigner, ClockChallenge]:
    payload = assignment_payload()
    authority = signer(payload)
    signed = authority.sign(payload)
    clock = ClockSigner(authority.authority.binding, PRIVATE_KEY)
    verifier = ClockVerifier(clock.authority)
    challenge = ClockChallenge(
        worker_id=clock.authority.binding.worker_id,
        session_id=clock.authority.binding.worker_session_id,
        nonce=identifier(50),
    )
    sample = ClockReplyPayload(
        binding=clock.authority.binding,
        clock_epoch=identifier(51),
        nonce=challenge.nonce,
        authority_time_ms=timestamp_ms(NOW) + 1000,
        clock_profile="linux-boottime-v1",
        policy=policy or ClockPolicy(),
    )
    reply = verifier.reply(clock.reply(sample), challenge)
    grant = verifier.binding(
        clock.bind(
            AssignmentTimePayload(
                binding=sample.binding,
                clock_epoch=sample.clock_epoch,
                assignment_digest=timed_assignment_digest(signed),
                policy_digest=sample.policy.digest,
            )
        ),
        signed,
    )
    sent = ClockReading(10_000 * NS_PER_MS, 9000 * NS_PER_MS, 2_000_000 * NS_PER_MS, 0)
    received = advance(sent, roundtrip_ms * NS_PER_MS)
    return (
        ClockEstimate(reply, sent, received, identifier(52), "linux-boottime-v1"),
        grant,
        clock,
        challenge,
    )


def advance(sample: ClockReading, nanoseconds: int) -> ClockReading:
    return ClockReading(
        sample.continuous_ns + nanoseconds,
        sample.awake_ns + nanoseconds,
        sample.wall_ns + nanoseconds,
        sample.read_uncertainty_ns,
    )


def test_intervals_cover_asymmetric_delays_and_both_rate_extremes() -> None:
    estimate, _, _, _ = fixture()
    anchor = estimate.verified.reply.payload.authority_time_ms * NS_PER_MS
    for elapsed_ms in (0, 1, 100, 5000, 29_999):
        interval = estimate.interval(advance(estimate.received, elapsed_ms * NS_PER_MS))
        for return_delay_ms in (0, 1, 50, 99, 100):
            for rate in (Fraction(999, 1000), Fraction(1), Fraction(1001, 1000)):
                actual = anchor + (return_delay_ms + elapsed_ms) * NS_PER_MS * rate
                assert interval.lower_ns <= actual <= interval.upper_ns


def test_deadline_mapping_rounds_down_and_cannot_refund_observed_time() -> None:
    estimate, grant, _, _ = fixture()
    now = advance(estimate.received, 10_000 * NS_PER_MS)
    caps = estimate.deadlines(grant, now)
    payload = estimate.verified.reply.payload
    upper = estimate.interval(estimate.received).upper_ns
    rate = Fraction(1_000_000 + payload.policy.relative_drift_ppm, 1_000_000)
    dispatch = grant.assignment.assignment.payload.dispatch
    for cap, deadline in (
        (caps.launch_ns, dispatch.expires_at),
        (caps.hard_ns, dispatch.hard_deadline_at),
    ):
        at_cap = upper + (cap - estimate.received.continuous_ns) * rate
        assert at_cap <= timestamp_ms(deadline) * NS_PER_MS
    assert estimate.deadlines(grant, advance(now, 1000 * NS_PER_MS)) == caps
    widened = replace(now, read_uncertainty_ns=10_000_000)
    stricter = estimate.deadlines(grant, widened)
    assert stricter == caps


@pytest.mark.parametrize("change", ["nonce", "binding", "policy", "clock_epoch", "time"])
def test_signed_sample_rejects_tampering(change: str) -> None:
    estimate, _, clock, challenge = fixture()
    signed = estimate.verified.reply
    changes = {
        "nonce": {"nonce": identifier(70)},
        "binding": {"binding": signed.payload.binding.model_copy(update={"control_epoch": 2})},
        "policy": {"policy": ClockPolicy(relative_drift_ppm=10)},
        "clock_epoch": {"clock_epoch": identifier(71)},
        "time": {"authority_time_ms": signed.payload.authority_time_ms + 1},
    }
    altered = signed.model_copy(
        update={"payload": signed.payload.model_copy(update=changes[change])}
    )
    with pytest.raises(ResearchError) as failure:
        ClockVerifier(clock.authority).reply(altered, challenge)
    assert failure.value.code == ErrorCode.AUTHORIZATION_DENIED


def test_valid_signature_cannot_answer_another_challenge_or_bind_another_assignment() -> None:
    estimate, grant, clock, challenge = fixture()
    verifier = ClockVerifier(clock.authority)
    with pytest.raises(ResearchError, match="challenge differs"):
        verifier.reply(
            estimate.verified.reply, challenge.model_copy(update={"nonce": identifier(99)})
        )
    changed = grant.assignment.assignment.model_copy(update={"signature": "00" * 64})
    with pytest.raises(ResearchError, match="Timed assignment differs"):
        verifier.binding(grant.grant, changed)
    swapped = estimate.verified.reply.model_copy(update={"signature": grant.grant.signature})
    with pytest.raises(ResearchError, match="authentication failed"):
        verifier.reply(swapped, challenge)


@pytest.mark.parametrize("field", ["clock_epoch", "policy_digest"])
def test_authenticated_but_incompatible_time_domain_is_ineligible(field: str) -> None:
    estimate, grant, clock, _ = fixture()
    changed = {
        "clock_epoch": identifier(72),
        "policy_digest": ClockPolicy(relative_drift_ppm=7).digest,
    }
    new_grant = clock.bind(grant.grant.payload.model_copy(update={field: changed[field]}))
    verified = ClockVerifier(clock.authority).binding(new_grant, grant.assignment.assignment)
    with pytest.raises(ResearchError, match="time domain differs"):
        estimate.deadlines(verified, estimate.received)


def test_roundtrip_age_uncertainty_and_horizon_have_explicit_bounds() -> None:
    with pytest.raises(ResearchError, match="roundtrip"):
        fixture(roundtrip_ms=1001)
    with pytest.raises(ResearchError, match="too uncertain"):
        fixture(policy=ClockPolicy(maximum_uncertainty_ms=50))
    estimate, grant, _, _ = fixture()
    with pytest.raises(ResearchError, match="stale"):
        estimate.interval(advance(estimate.received, 30_001 * NS_PER_MS))
    short, grant, _, _ = fixture(policy=ClockPolicy(maximum_deadline_horizon_ms=200_000))
    with pytest.raises(ResearchError, match="horizon"):
        short.deadlines(grant, short.received)


def test_future_issue_expired_grant_and_suspend_never_gain_positive_allowance() -> None:
    estimate, grant, clock, challenge = fixture()
    earlier = estimate.verified.reply.payload.model_copy(
        update={"authority_time_ms": timestamp_ms(NOW)}
    )
    reply = ClockVerifier(clock.authority).reply(clock.reply(earlier), challenge)
    future = replace(estimate, verified=reply)
    with pytest.raises(ResearchError, match="qualified interval"):
        future.deadlines(grant, future.received)
    with pytest.raises(ResearchError, match="qualified interval"):
        estimate.deadlines(grant, advance(estimate.received, 29_000 * NS_PER_MS))
    suspended = replace(
        advance(estimate.received, 5000 * NS_PER_MS), awake_ns=estimate.received.awake_ns
    )
    with pytest.raises(ResearchError, match="suspend"):
        estimate.interval(suspended)


def test_refresh_intersection_cannot_extend_caps_or_cross_local_origins() -> None:
    estimate, grant, _, _ = fixture()
    caps = estimate.deadlines(grant, estimate.received)
    later = replace(caps, launch_ns=caps.launch_ns + 1000, hard_ns=caps.hard_ns + 2000)
    assert caps.intersect(later) == caps
    with pytest.raises(ResearchError, match="authority differs"):
        caps.intersect(replace(caps, origin_session_id=identifier(75)))
    with pytest.raises(ResearchError, match="authority differs"):
        caps.intersect(replace(caps, origin_profile="windows-qpc-unbiased-v1"))


def test_time_signature_golden_and_public_only_verification_in_another_process() -> None:
    estimate, grant, clock, challenge = fixture()
    reply = estimate.verified.reply
    assert hashlib.sha256(
        clock_signing_bytes("research.worker-clock", reply.key_id, reply.payload)
    ).hexdigest() == ("b12a29e269ec377130de57fe57a0dce9a870172f61ef25d52870a1152a6b0aeb")
    assert reply.signature == (
        "27dbb298c532c895f437ede84347e7477543ccadf03526f02b5ac64a99bfe6289d4"
        "c980931bd3936d8aa467ede56bcc35018e61708739127740613a47de7a004"
    )
    assert grant.grant.signature == (
        "b1d2983335fc53b62f984c8480f95288b9dffaaa9019422f7a20ffe23655b97776"
        "b964fcc579ffd9fab84a285c46d6793fe6dc287acb6058bb4b67c2f58c6206"
    )
    wire = json.dumps(
        {
            "authority": clock.authority.model_dump(mode="json"),
            "challenge": challenge.model_dump(mode="json"),
            "reply": reply.model_dump(mode="json"),
        }
    )
    assert PRIVATE_KEY.hex() not in wire
    code = (
        "import json,sys; from research_swarm.domain.assignments import AssignmentAuthority; "
        "from research_swarm.domain.clocks import ClockChallenge,SignedClockReply; "
        "from research_swarm.workers.clock_signatures import ClockVerifier; "
        "w=json.load(sys.stdin); "
        "v=ClockVerifier(AssignmentAuthority.model_validate(w['authority'])); "
        "r=v.reply(SignedClockReply.model_validate(w['reply']),"
        "ClockChallenge.model_validate(w['challenge'])); print(r.reply.payload.clock_epoch)"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        input=wire,
        text=True,
        capture_output=True,
        timeout=10,
        check=True,
    )
    assert result.stdout.strip() == reply.payload.clock_epoch


def test_one_use_challenge_cannot_reanchor_an_old_signed_reply() -> None:
    estimate, _, clock, _ = fixture()
    origin = ClockSampler(clock.authority)
    pending = origin.begin()
    reply = clock.reply(
        estimate.verified.reply.payload.model_copy(update={"nonce": pending.challenge.nonce})
    )
    observed = pending.complete(reply)
    assert pending.complete(reply) is observed
    assert observed.origin_session_id == origin.origin_session_id
    other = origin.begin()
    with pytest.raises(ResearchError, match="challenge differs"):
        other.complete(reply)
    fresh = clock.reply(reply.payload.model_copy(update={"nonce": other.challenge.nonce}))
    with pytest.raises(ResearchError, match="already consumed"):
        other.complete(fresh)
    with pytest.raises(ResearchError, match="reply changed"):
        pending.complete(fresh)


def test_sampling_origin_cannot_change_counter_profile() -> None:
    _, _, clock, _ = fixture()
    origin = ClockSampler(clock.authority)
    pending = origin.begin()
    origin._clock = replace(origin._clock, profile="unqualified")
    with pytest.raises(ResearchError, match="origin changed"):
        origin.read()
    assert pending.challenge.worker_id == clock.authority.binding.worker_id


def test_concurrent_completion_preserves_one_receive_anchor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    estimate, _, clock, _ = fixture()
    origin = ClockSampler(clock.authority)
    pending = origin.begin()
    reply = clock.reply(
        estimate.verified.reply.payload.model_copy(update={"nonce": pending.challenge.nonce})
    )
    read = origin.read
    entered, release = threading.Event(), threading.Event()

    def held_read() -> ClockReading:
        entered.set()
        assert release.wait(5)
        return read()

    monkeypatch.setattr(origin, "read", held_read)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(pending.complete, reply)
        try:
            assert entered.wait(5)
            second = pool.submit(pending.complete, reply)
        finally:
            release.set()
        assert first.result(timeout=5) is second.result(timeout=5)
