"""One original source elects one exact destination without changing portable evidence."""

from research_swarm.checkpoints.elections import verify_election, verify_election_request
from research_swarm.crypto import sign
from research_swarm.domain.backup import RestoreClaim, SignedRestoreClaim
from research_swarm.domain.checkpoint_elections import (
    LogicalElection,
    SignedLogicalElection,
    SignedLogicalElectionRequest,
)
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id, utc_now
from research_swarm.state.database import encode

if __package__:
    from ..runtime.exclusive import exclusive
    from .checkpoint_anchor import TABLE, require_anchor_in
    from .checkpoint_capture import capture_checkpoint
    from .checkpoint_source import (
        SourceStore,
        load_source,
        parse_checkpoint_request,
        source_operator,
    )
else:
    from checkpoint_anchor import TABLE, require_anchor_in
    from checkpoint_capture import capture_checkpoint
    from checkpoint_source import (
        SourceStore,
        load_source,
        parse_checkpoint_request,
        source_operator,
    )
    from exclusive import exclusive


async def elect_source(
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
    request = parse_checkpoint_request(request_json, SignedLogicalElectionRequest)
    intent = request.request.intent
    if (
        intent.target_location == identity.location
        or intent.target_deployment_digest == manifest.digest
        or intent.operator_id != manifest.provisioning_actor_id
    ):
        raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint election destination differs")
    pins = {
        "expected_source": custody.binding.authority,
        "expected_deployment_digest": custody.binding.deployment_digest,
        "expected_custody": custody.binding.pin,
        "crypto": crypto,
    }

    async def elect():
        _, token = source_operator(manifest, secrets)
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

            def select(admitted):
                anchor = require_anchor_in(
                    admitted, manifest, store.identity, custody.binding.pin, required=True
                )
                if anchor.election is not None:
                    saved = anchor.election
                    original = saved.election.signed_request
                    backup_id = original.request.signed_request.request.backup_id
                    verify_election(
                        saved,
                        expected_intent=original.request.intent,
                        expected_backup_id=backup_id,
                        **pins,
                    )
                    verify_election_request(
                        request, expected_intent=intent, expected_backup_id=backup_id, **pins
                    )
                    if request != original:
                        raise ResearchError(
                            ErrorCode.IDEMPOTENCY_CONFLICT,
                            "Checkpoint already elected a destination",
                        )
                    return encode(saved.model_dump(mode="json"))

                image = capture_checkpoint(
                    bound,
                    manifest,
                    token=token,
                    backup_id=request.request.signed_request.request.backup_id,
                    expected_custody=custody.binding.pin,
                    crypto=crypto,
                    recovery_binding=recovery_binding,
                ).manifest
                if intent.manifest_digest != image.digest:
                    raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint election image changed")
                verify_election_request(
                    request,
                    expected_intent=intent,
                    expected_backup_id=image.seal.backup_id,
                    **pins,
                )
                claim = RestoreClaim(
                    claim_id=new_id(),
                    request=request.request.signed_request.request,
                    source_public_key=custody.binding.pin.public_key,
                    claimed_at=utc_now(),
                )
                election = LogicalElection(
                    signed_request=request,
                    signed_claim=SignedRestoreClaim(
                        claim=claim,
                        signature=sign(crypto, custody.seed, claim.digest.encode("ascii")).hex(),
                    ),
                )
                saved = SignedLogicalElection(
                    election=election,
                    signature=sign(crypto, custody.seed, election.digest.encode("ascii")).hex(),
                )
                payload = encode(saved.model_dump(mode="json"))
                if len(payload.encode()) > 16384:
                    raise ResearchError(
                        ErrorCode.CAPABILITY_UNAVAILABLE, "Checkpoint proof too large"
                    )
                admitted.execute(
                    f"UPDATE {TABLE} SET state='elected',election_payload=? "
                    "WHERE activation_id=? AND custody_id=? AND anchor_digest=? AND state='open'",
                    (
                        payload,
                        anchor.anchor.source_authority.activation_id,
                        custody.binding.pin.custody_id,
                        anchor.anchor.digest,
                    ),
                )
                if (
                    require_anchor_in(
                        admitted, manifest, store.identity, custody.binding.pin, required=True
                    ).election
                    != saved
                ):
                    raise ResearchError(ErrorCode.STALE_FENCE, "Checkpoint election commit differs")
                return payload

            return bound.write(select)

        return store.write(commit)

    return await exclusive(state, elect)
