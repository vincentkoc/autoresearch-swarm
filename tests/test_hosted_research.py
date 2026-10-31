"""Ordinary source-to-claim transactions; async byte faults are not provider proof."""

import asyncio
import io
import zipfile
from contextlib import contextmanager
from types import SimpleNamespace

import httpx
import pytest

from adapters.cloudflare.hosted_http import handle
from adapters.cloudflare.research_router import HostedResearchConfiguration
from research_swarm.client import ResearchClient
from research_swarm.control.queries import RecordQuery
from research_swarm.control.workers import AttemptData, WorkerData
from research_swarm.domain.canonical import strict_json_loads
from research_swarm.domain.clocks import AuthorityClockAdmission
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id
from research_swarm.state.database import encode
from research_swarm.storage.async_r2 import BoundedR2Configuration, BoundedR2Objects
from research_swarm.workers.clock_estimates import ClockSampler
from research_swarm.workers.clock_signatures import ClockVerifier
from research_swarm.workers.local_enrollment import cpu_profile
from tests.support import LIMITS
from tests.test_async_r2 import Transport
from tests.test_hosted_artifacts import command, current, pending
from tests.test_hosted_http import send
from tests.test_hosted_router import counts
from tests.test_hosted_router import hosted as hosted
from tests.test_manifest import capture
from tests.test_manifest import project as project


@pytest.fixture
async def research(hosted):
    h = hosted
    storage = BoundedR2Configuration(
        workspace_id=h.principal.workspace_id,
        bucket_identity="synthetic-bucket",
        prefix="research-admission",
        max_object_bytes=1024**2,
    )
    h.config = HostedResearchConfiguration(
        **h.config.model_dump(exclude={"profile"}),
        storage=storage,
        storage_limit_bytes=4 * 1024**2,
    )
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.transport = Transport(storage)
    h.objects = BoundedR2Objects(storage, h.transport)
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    return h


@contextmanager
def sdk(h, loop, *, token=None):
    token = token or h.operator.token

    def route(request):
        async def deliver():
            async def read(limit):
                assert len(request.content) <= limit
                return request.content

            return await handle(
                h.router, request.method, str(request.url), dict(request.headers), read
            )

        result = asyncio.run_coroutine_threadsafe(deliver(), loop).result(timeout=10)
        return httpx.Response(result.status, content=result.body, headers=dict(result.headers))

    with ResearchClient.connect("http://127.0.0.1", token) as client:
        client.transport.http.close()
        client.transport.http = httpx.Client(
            base_url="http://127.0.0.1",
            transport=httpx.MockTransport(route),
            headers={"authorization": "Bearer " + token},
        )
        yield client


def admit(client, root, *, input_artifacts=None):
    """The same unchanged SDK sequence is also exercised against real workerd."""
    registered = client.register_project(root, idempotency_key="source")
    project = registered["result"]["project"]
    revision = registered["result"]["project_revision"]
    campaign = client.command(
        "campaign.create",
        {
            "project_id": project["id"],
            "name": "Hosted ordinary project",
            "primary_metric": "loss",
            "limits": [item.model_dump(mode="json") for item in LIMITS],
            "input_artifacts": input_artifacts or {},
        },
        idempotency_key="campaign",
    )["result"]
    assert client.command(
        "campaign.validate",
        {"campaign_id": campaign["id"]},
        idempotency_key="validate-campaign",
        expected_revision=campaign["revision"],
    )["result"]
    for operation in ("campaign.seal", "campaign.start"):
        campaign = client.command(
            operation,
            {"campaign_id": campaign["id"]},
            idempotency_key=operation,
            expected_revision=campaign["revision"],
        )["result"]
    proposed = client.command(
        "candidate.propose",
        {
            "campaign_id": campaign["id"],
            "source_artifact": revision["data"]["source_artifact"],
            "hypothesis": "Replay the immutable baseline",
        },
        idempotency_key="candidate",
        expected_revision=campaign["revision"],
    )["result"]
    validation = client.command(
        "candidate.validate",
        {"candidate_id": proposed["id"]},
        idempotency_key="validate",
        expected_revision=proposed["revision"],
    )["result"]
    submission = {
        "campaign_id": campaign["id"],
        "candidate_id": proposed["id"],
        "validation_id": validation["id"],
        "seed": 42,
    }
    trial = client.command(
        "trial.submit", submission, idempotency_key="trial", expected_revision=campaign["revision"]
    )
    jobs = client.list(RecordQuery(kind="job", campaign_id=campaign["id"], state="ready"))["items"]
    assert len(jobs) == 1 and client.get("job", jobs[0]["id"])["state"] == "ready"
    return SimpleNamespace(
        registered=registered,
        project=project,
        revision=revision,
        campaign=campaign,
        candidate=proposed,
        trial=trial,
        submission=submission,
        job=jobs[0],
    )


def enrollment(principal_id, policy_digest, **changes):
    return {
        "worker_id": new_id(),
        "session_id": new_id(),
        "principal_id": principal_id,
        "name": "Hosted CPU claim fixture",
        "clock_mode": "bounded-continuous-v1",
        "clock_admission": AuthorityClockAdmission(policy_digest=policy_digest).model_dump(
            mode="json"
        ),
        "profiles": [cpu_profile(cpu_slots=1, fallback_identity="fixture").model_dump(mode="json")],
        **changes,
    }


def enroll_and_claim(client, admitted, principal_id, *, worker_client=None, project_ids=None):
    worker_client = worker_client or client
    caps = client.capabilities()
    assert caps["execution"] in {
        "cpu-claim-only",
        "cpu-claim-and-inputs",
        "cpu-claim-and-transfers",
        "cpu-result-publication",
    }
    enrolled = client.command(
        "worker.enroll",
        enrollment(
            principal_id,
            caps["clock"]["policy_digest"],
            **({"project_ids": project_ids} if project_ids is not None else {}),
        ),
        idempotency_key="worker",
    )["result"]
    worker = WorkerData.model_validate(enrolled["data"])
    sampler = ClockSampler(worker.assignment_authority, clock_admission=worker.clock_admission)
    waiting = sampler.begin()
    estimate = waiting.complete(worker_client.worker_clock(waiting.challenge))
    claim = {
        "worker_id": worker.binding.worker_id,
        "session_id": worker.binding.worker_session_id,
        "job_id": admitted.job["id"],
    }
    claimed = worker_client.command(
        "worker.claim_job",
        claim,
        idempotency_key="claim",
        expected_revision=admitted.job["revision"],
    )
    attempt = AttemptData.model_validate(claimed["result"]["data"])
    grant = ClockVerifier(worker.assignment_authority).binding(
        attempt.assignment_time, attempt.signed_assignment
    )
    assert grant.grant.payload.clock_epoch == estimate.verified.reply.payload.clock_epoch
    assert grant.grant.payload.policy_digest == worker.clock_admission.policy_digest
    assert attempt.signed_assignment.payload.spec.seed == 42
    assert (
        attempt.signed_assignment.payload.source_tree_digest
        == admitted.revision["data"]["resolved"]["source"]["tree_digest"]
    )
    assert len(client.list(RecordQuery(kind="acceptance_lease"))["items"]) == 1
    return claimed, claim


async def setup(h, project):
    loop = asyncio.get_running_loop()

    def run():
        with sdk(h, loop) as client:
            return admit(client, project)

    return await asyncio.to_thread(run)


async def test_ordinary_sdk_admission_signed_claim_and_exact_replay_without_io(research, project):
    h, loop = research, asyncio.get_running_loop()

    def exercise():
        with sdk(h, loop) as client:
            admitted = admit(client, project)
            claimed, claim = enroll_and_claim(client, admitted, h.principal.id)
            return admitted, claimed, claim

    admitted, claimed, claim = await asyncio.to_thread(exercise)
    before, calls = counts(h), (len(h.transport.calls), len(h.witness.requests))
    h.witness.failure = ResearchError(ErrorCode.TRANSPORT_UNAVAILABLE, "Source offline")
    h.transport.hide_reads = True
    h.router = await h.reopen(configuration=h.config, objects=h.objects)
    for operation, payload, key, revision, expected in (
        (
            "project.register",
            {"source_artifact": admitted.revision["data"]["source_artifact"]},
            "source",
            None,
            admitted.registered,
        ),
        (
            "trial.submit",
            admitted.submission,
            "trial",
            admitted.campaign["revision"],
            admitted.trial,
        ),
        ("worker.claim_job", claim, "claim", admitted.job["revision"], claimed),
    ):
        assert (
            await h.router.command(
                h.operator.token,
                operation,
                payload,
                idempotency_key=key,
                expected_revision=revision,
            )
            == expected
        )
    assert counts(h) == before and (len(h.transport.calls), len(h.witness.requests)) == calls


async def test_shared_validation_rejects_protected_mutation(research, project):
    h, loop = research, asyncio.get_running_loop()
    admitted = await setup(h, project)
    (project / "evaluate.py").write_text("metric = 'forged'\n")

    def exercise():
        with sdk(h, loop) as client:
            source = client.capture_project(project, idempotency_key="mutated")
            candidate = client.command(
                "candidate.propose",
                {
                    "campaign_id": admitted.campaign["id"],
                    "source_artifact": source.model_dump(mode="json"),
                    "hypothesis": "Alter a protected evaluator",
                },
                idempotency_key="mutated-candidate",
                expected_revision=admitted.campaign["revision"],
            )["result"]
            validation = client.command(
                "candidate.validate",
                {"candidate_id": candidate["id"]},
                idempotency_key="mutated-validation",
                expected_revision=candidate["revision"],
            )["result"]
            assert validation["state"] == "invalid"
            with pytest.raises(ResearchError) as refused:
                client.command(
                    "trial.submit",
                    {
                        **admitted.submission,
                        "candidate_id": candidate["id"],
                        "validation_id": validation["id"],
                    },
                    idempotency_key="mutated-trial",
                    expected_revision=admitted.campaign["revision"],
                )
            assert refused.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
            assert len(client.list(RecordQuery(kind="trial"))["items"]) == 1

    await asyncio.to_thread(exercise)


async def test_inner_source_digest_checked_before_witness_or_project_write(
    research, project, tmp_path
):
    h, path = research, tmp_path / "source.zip"
    capture(project, path)
    corrupted = io.BytesIO()
    with zipfile.ZipFile(path) as original, zipfile.ZipFile(corrupted, "w") as output:
        for item in original.infolist():
            body = original.read(item)
            if item.filename == "train.py":
                body = body.replace(b"0.1", b"0.2")
            output.writestr(item, body)
    uploaded = await pending(h, corrupted.getvalue())
    finalized = await command(h, "finalize_upload", uploaded["id"])
    reference = finalized["result"]["data"]["reference"]
    before, calls = counts(h), len(h.witness.requests)
    with pytest.raises(ResearchError) as refused:
        await h.router.command(
            h.operator.token,
            "project.register",
            {"source_artifact": reference},
            idempotency_key="corrupted",
        )
    assert refused.value.code == ErrorCode.ARTIFACT_UNVERIFIED
    assert counts(h) == before and len(h.witness.requests) == calls


@pytest.mark.parametrize("fault", ["admission", "project", "cpu", "revision"])
async def test_claim_preflight_refusal_cannot_reserve_or_acquire_time(research, project, fault):
    h = research
    admitted = await setup(h, project)
    changes = {}
    if fault == "admission":
        changes["clock_admission"] = None
    elif fault == "project":
        other = await h.router.command(
            h.operator.token,
            "project.register",
            {"source_artifact": admitted.revision["data"]["source_artifact"]},
            idempotency_key="other-project",
        )
        changes["project_ids"] = [other["result"]["project"]["id"]]
    elif fault == "cpu":
        changes["profiles"] = [
            {
                "name": "python-cpu",
                "backend": "pytorch",
                "cpu_slots": 1,
                "execution_fingerprint": "sha256:" + "b" * 64,
            }
        ]
    request = enrollment(h.principal.id, h.config.source.policy.digest, **changes)
    await h.router.command(h.operator.token, "worker.enroll", request, idempotency_key="worker")
    before, calls = counts(h), len(h.witness.requests)
    with pytest.raises(ResearchError):
        await h.router.command(
            h.operator.token,
            "worker.claim_job",
            {
                "worker_id": request["worker_id"],
                "session_id": request["session_id"],
                "job_id": admitted.job["id"],
            },
            idempotency_key="claim",
            expected_revision=99 if fault == "revision" else admitted.job["revision"],
        )
    assert counts(h) == before and len(h.witness.requests) == calls


async def test_competing_claims_reserve_exactly_one_attempt(research, project):
    h = research
    admitted = await setup(h, project)
    requests = [enrollment(h.principal.id, h.config.source.policy.digest) for _ in range(2)]
    for request in requests:
        await h.router.command(h.operator.token, "worker.enroll", request, idempotency_key=new_id())
    results = await asyncio.gather(
        *(
            h.router.command(
                h.operator.token,
                "worker.claim_job",
                {
                    "worker_id": request["worker_id"],
                    "session_id": request["session_id"],
                    "job_id": admitted.job["id"],
                },
                idempotency_key=new_id(),
                expected_revision=admitted.job["revision"],
            )
            for request in requests
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(result, dict) for result in results) == 1
    failure = next(result for result in results if isinstance(result, ResearchError))
    assert failure.code == ErrorCode.REVISION_CONFLICT
    assert len((await h.router.query(h.operator.token, "list", {"kind": "attempt"}))["items"]) == 1
    assert current(h, "job", admitted.job["id"])["state"] == "reserved"
    assert (
        h.store.read(lambda sql: sql.execute("SELECT COUNT(*) FROM budget_reservations").one()[0])
        == 1
    )


async def test_research_profiles_and_read_scopes_are_explicit(research):
    h = research
    response = await send(
        h, "POST", "/v1/query", payload={"kind": "project"}, token=h.worker_credential.token
    )
    assert response.status == 403
    response = await send(h, "POST", "/v1/query", payload={"kind": "artifact_read_session"})
    assert response.status == 422
    assert strict_json_loads(response.body)["error"]["code"] == ErrorCode.PROJECT_CONTRACT_INVALID
    with pytest.raises(ResearchError) as denied:
        await h.router.command(
            h.operator.token, "evaluation.authorize", {}, idempotency_key="not-qualified"
        )
    assert denied.value.code == ErrorCode.CAPABILITY_UNAVAILABLE


@pytest.mark.parametrize("fault", ["catalog-read", "catalog-witness", "credential", "cancel"])
async def test_prepared_source_never_outlives_its_catalog_or_authority(
    research, project, tmp_path, fault, monkeypatch
):
    h, path = research, tmp_path / "source.zip"
    capture(project, path)
    uploaded = await pending(h, path.read_bytes())
    finalized = await command(h, "finalize_upload", uploaded["id"])
    reference = finalized["result"]["data"]["reference"]
    before = counts(h)
    original_read, original_witness = h.objects.read, h.router.acquire

    def damage():
        if fault == "credential":
            h.credentials.revoke(h.principal, h.operator.id)
        elif fault == "cancel":
            asyncio.current_task().cancel()
        else:
            h.store.write(
                lambda sql: sql.execute(
                    "UPDATE records SET revision=revision+1 WHERE kind='artifact' AND id=?",
                    (reference["id"],),
                )
            )

    async def read(*args):
        assert h.router.boundary._turn is None
        body = await original_read(*args)
        if fault == "catalog-read":
            damage()
        return body

    async def witness(request):
        result = await original_witness(request)
        damage()
        return result

    monkeypatch.setattr(h.objects, "read", read)
    if fault != "catalog-read":
        monkeypatch.setattr(h.router, "acquire", witness)
    task = asyncio.create_task(
        h.router.command(
            h.operator.token,
            "project.register",
            {"source_artifact": reference},
            idempotency_key="stale-source",
        )
    )
    with pytest.raises(asyncio.CancelledError if fault == "cancel" else ResearchError):
        await task
    assert counts(h) == before
    assert h.router.boundary._turn is None and not h.gate.lock.locked()


async def test_old_profile_rejects_research_before_body_consumption(hosted):
    consumed = []
    response = await send(
        hosted, "POST", "/v1/commands/project.register", payload={}, consume=consumed
    )
    assert response.status == 501 and consumed == []

    response = await send(
        hosted, "POST", "/v1/query", payload={"kind": "project"}, consume=consumed
    )
    assert response.status == 501 and consumed == []


async def test_read_permission_is_current_even_for_exact_source_replay(research, project):
    h = research
    admitted = await setup(h, project)
    narrowed = h.principal.model_copy(update={"scopes": {"project.manage"}})
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE credentials SET principal=? WHERE id=?",
            (encode(narrowed.model_dump(mode="json")), h.operator.id),
        )
    )
    payload = {"source_artifact": admitted.revision["data"]["source_artifact"]}
    before, calls = counts(h), (len(h.transport.calls), len(h.witness.requests))
    with pytest.raises(ResearchError) as denied:
        await h.router.command(
            h.operator.token, "project.register", payload, idempotency_key="source"
        )
    assert denied.value.code == ErrorCode.AUTHORIZATION_DENIED
    consumed = []
    response = await send(
        h,
        "POST",
        "/v1/commands/project.register",
        payload=payload,
        headers={"idempotency-key": "source"},
        consume=consumed,
    )
    assert response.status == 403 and consumed == []
    assert counts(h) == before and (len(h.transport.calls), len(h.witness.requests)) == calls
