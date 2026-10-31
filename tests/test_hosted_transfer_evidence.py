"""An assigned independent evaluator must finish before its driver can retire."""

import pytest

from adapters.cloudflare.research_router import HostedTransferConfiguration
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.state.database import encode
from tests.test_hosted_artifacts import current
from tests.test_hosted_evaluations import execute_evaluation
from tests.test_hosted_orchestration import evaluation as evaluation
from tests.test_hosted_orchestration import execute, prepare
from tests.test_hosted_orchestration import hosted as hosted
from tests.test_hosted_orchestration import lifecycle as lifecycle
from tests.test_hosted_orchestration import orchestrated as orchestrated
from tests.test_hosted_orchestration import project as project
from tests.test_hosted_orchestration import publication as publication
from tests.test_hosted_orchestration import research as research
from tests.test_hosted_orchestration import scientific as scientific
from tests.test_hosted_transfers import command, issue, request


@pytest.mark.parametrize("publication", ["evaluation"], indirect=True)
async def test_retiring_evaluator_blocks_transfer_until_its_independent_receipt_completes(
    orchestrated,
):
    e, h = orchestrated, orchestrated.h
    h.config = HostedTransferConfiguration(**h.config.model_dump(exclude={"profile"}))
    h.store.write(
        lambda sql: sql.execute(
            "UPDATE hosted_authority_config SET digest=?,payload=? WHERE singleton=1",
            (h.config.digest, encode(h.config.model_dump(mode="json"))),
        )
    )
    h.router = await h.reopen(configuration=h.config, objects=h.objects, acquire=e.case.acquire)
    e.admitted = e.case.admitted
    e.successor, e.next_evaluator = issue(h, {"*"}), issue(h, {"*"})
    _, _, intent = await execute_evaluation(e)
    payload, revision = await request(e)
    before = len(h.witness.requests), len(h.transport.calls)
    binding = current(h, "orchestration_binding", payload["campaign_id"])
    with pytest.raises(ResearchError) as pending:
        await command(e, "orchestration.transfer", payload, revision=revision)
    assert pending.value.code == ErrorCode.EVALUATION_INCOMPLETE
    assert (len(h.witness.requests), len(h.transport.calls)) == before
    assert current(h, "orchestration_binding", payload["campaign_id"]) == binding
    saved = await prepare(
        e, "evaluation.complete", intent.model_dump(mode="json"), credential=e.evaluator
    )
    completed = (await execute(e, saved, credential=e.evaluator))["result"]["response"]["result"]
    assert completed["state"] == "valid"
    payload, revision = await request(e)
    accepted = await command(e, "orchestration.transfer", payload, revision=revision)
    assert accepted["result"]["data"]["successor"]["epoch"] == 2
    assert current(h, "evaluation_receipt", completed["id"]) == completed
