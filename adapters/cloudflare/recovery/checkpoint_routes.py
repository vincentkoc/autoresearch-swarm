"""Issue a signed original-worker destination under private elected-target custody."""

from dataclasses import asdict

from research_swarm.control.recovery_authority import recovery_authority
from research_swarm.control.recovery_route_issuance import (
    normalize_route_request,
    persist_route,
    route_from_snapshot,
)
from research_swarm.control.recovery_routes import verify_logical_recovery_route
from research_swarm.crypto import sign
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import CommandContext, RecoveryRouteGuard
from research_swarm.domain.recovery_routes import RecoveryRouteRequest, SignedWorkerRecoveryRoute
from research_swarm.state.database import Database, Transaction, _request_digest, encode
from research_swarm.state.recovery_elections import retained_election
from research_swarm.state.recovery_routes import OPERATION, require_recovery_route

if __package__:
    from ..runtime.exclusive import exclusive
    from .checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from .checkpoint_import import verify_target_configuration
    from .checkpoint_source import parse_checkpoint_request
    from .checkpoint_target import load_target
else:
    from checkpoint_admission import AdmissionStore, HostedRecoveryVerifier
    from checkpoint_import import verify_target_configuration
    from checkpoint_source import parse_checkpoint_request
    from checkpoint_target import load_target
    from exclusive import exclusive


async def issue_target_route(
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
    request = normalize_route_request(parse_checkpoint_request(request_json, RecoveryRouteRequest))
    payload = {"restore_id": binding.intent.restore_id, "request": request.model_dump(mode="json")}
    context = CommandContext(
        principal=issuer,
        idempotency_key="logical-route:" + binding.intent.restore_id + ":" + request.route_id,
    )
    verifier = HostedRecoveryVerifier(manifest, binding)

    async def issue():
        store = open_store()
        manifest.require_store(store.identity)

        def commit(sql):
            receipt, admission = verifier.read_admission(
                sql, authority=store.identity, context=context, crypto=crypto
            )
            route = route_from_snapshot(
                sql, context, request, receipt, admission, nonexpiring_credential=True
            )
            elected = retained_election(sql, receipt, admission)
            signed = SignedWorkerRecoveryRoute(
                route=route,
                logical_election=elected,
                target_signature=sign(crypto, custody.seed, route.digest.encode("ascii")).hex(),
            )
            verify_logical_recovery_route(signed, binding.expected_custody, crypto_provider=crypto)
            if len(encode(signed.model_dump(mode="json")).encode()) > 4 * 1024**2:
                raise ResearchError(
                    ErrorCode.RESOURCE_UNAVAILABLE, "Recovery route exceeds its limit"
                )
            database = Database.from_store(
                AdmissionStore(store.identity, sql), recovery_verifier=verifier
            )
            authority = recovery_authority(database, context, crypto_provider=crypto)
            if authority is None:
                raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route requires admission")
            context._recovery_route = RecoveryRouteGuard(
                **asdict(authority),
                request_digest=_request_digest(context, OPERATION, payload),
                route_id=route.route_id,
                route_digest=route.digest,
                nonexpiring_credential=True,
            )
            # Authorization and the retained record are checked even before cached acknowledgments.
            require_recovery_route(
                sql,
                context,
                store.identity,
                OPERATION,
                payload,
                context._recovery_route.request_digest,
            )
            existing = sql.execute(
                "SELECT 1 FROM records WHERE workspace_id=? "
                "AND kind='worker_recovery_route' AND id=?",
                (issuer.workspace_id, route.route_id),
            ).fetchone()
            if existing is not None:
                persist_route(Transaction(sql, context), signed)
            result = database.execute(
                context, OPERATION, payload, "*", lambda tx: persist_route(tx, signed)
            )
            if result.get("disposition") != "accepted" or result.get("result") != Transaction(
                sql, context
            ).get("worker_recovery_route", route.route_id):
                raise ResearchError(ErrorCode.STALE_FENCE, "Recovery route acknowledgment differs")
            verifier.read_admission(sql, authority=store.identity, context=context, crypto=crypto)
            return encode(result)

        return store.write(commit)

    return await exclusive(state, issue)
