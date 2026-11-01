"""Exercise actual envelope codecs in workerd, with public synthetic fixtures."""

import sys

from durable_sql import DurableObjectStore
from ed25519 import NobleEd25519

from research_swarm.domain.assignments import AssignmentAuthority, SignedAssignment
from research_swarm.domain.clocks import ClockChallenge, SignedAssignmentTime, SignedClockReply
from research_swarm.domain.types import VersionedRecord
from research_swarm.state.database import SCHEMA
from research_swarm.workers.clock_signatures import ClockSigner, ClockVerifier
from research_swarm.workers.signatures import AssignmentSigner, AssignmentVerifier
from research_swarm.workers.timed_launch import TimingEvidence, verify_timing


class SigningProbe(VersionedRecord):
    authority: AssignmentAuthority
    assignment: SignedAssignment
    reply: SignedClockReply
    challenge: ClockChallenge
    grant: SignedAssignmentTime
    timing: TimingEvidence


def exercise(storage, object_id, body, module):
    probe = SigningProbe.model_validate_json(body)
    provider = NobleEd25519(module)
    store = DurableObjectStore(
        storage, namespace="signing-qualification", object_id=object_id, schema=SCHEMA
    )

    def operation(sql):
        # Every crypto operation below runs synchronously inside transactionSync.
        assignment = AssignmentVerifier(probe.authority, crypto_provider=provider).verify(
            probe.assignment, authority_now=probe.assignment.payload.issued_at
        )
        verifier = ClockVerifier(probe.authority, crypto_provider=provider)
        verifier.reply(probe.reply, probe.challenge)
        verifier.binding(probe.grant, probe.assignment)
        timing = verify_timing(
            probe.timing,
            probe.authority,
            probe.grant,
            probe.assignment,
            crypto_provider=provider,
        )
        # Public test seed; never supplied by the request or used for enrollment.
        seed = bytes(range(32))
        signer = AssignmentSigner(probe.authority.binding, seed, crypto_provider=provider)
        clock = ClockSigner(probe.authority.binding, seed, crypto_provider=provider)
        if signer.authority != probe.authority or clock.authority != probe.authority:
            raise ValueError("Only the public qualification key is supported")
        assert (
            "cryptography" not in sys.modules and "research_swarm.crypto_native" not in sys.modules
        )
        return {
            "assignment": signer.sign(assignment.assignment.payload).model_dump(mode="json"),
            "reply": clock.reply(probe.reply.payload).model_dump(mode="json"),
            "grant": clock.bind(probe.grant.payload).model_dump(mode="json"),
            "timing_digest": timing.evidence.digest,
        }

    return store.write(operation)
