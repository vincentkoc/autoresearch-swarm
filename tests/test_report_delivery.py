from typing import Any

import httpx
from mcp import Client

from research_swarm.api.http import create_app
from research_swarm.api.mcp import MAX_TOOL_BYTES
from research_swarm.control.commands import ProposeCandidate
from research_swarm.control.queries import RecordQuery
from research_swarm.control.reports import MAX_REPORT_BYTES, ReportCampaign
from research_swarm.domain.canonical import canonical_bytes, record_digest
from research_swarm.domain.evidence import ArtifactRef
from tests.test_cli_mcp import stdio
from tests.test_http_delivery import serving
from tests.test_local_pipeline import Pipeline
from tests.test_local_pipeline import pipeline as pipeline


async def test_large_report_keeps_agent_identity_and_authenticated_detail_navigation(
    pipeline: Pipeline,
) -> None:
    client = pipeline.client
    campaign_id = pipeline.campaign["id"]
    source = client.get("candidate", pipeline.trial["data"]["candidate_id"])["data"][
        "source_artifact"
    ]
    for index in range(50):
        client.command(
            "candidate.propose",
            ProposeCandidate(
                campaign_id=campaign_id,
                source_artifact=ArtifactRef.model_validate(source),
                hypothesis=f"{index:02d} " + "界" * 1021,
            ),
            idempotency_key=f"report-candidate-{index}",
            expected_revision=client.get("campaign", campaign_id)["revision"],
        )
    request = ReportCampaign(campaign_id=campaign_id, recent_limit=50)
    command = {"request": request.model_dump(mode="json"), "idempotency_key": "large-report"}
    before_campaign = client.get("campaign", campaign_id)
    before_budget = client.budget(campaign_id)
    async with Client(stdio(pipeline.runtime.root), read_timeout_seconds=20, cache=None) as agent:
        created = await agent.call_tool("research_report_create", command)
        assert not created.is_error and created.structured_content is not None
        acknowledgment = created.structured_content
        assert acknowledgment["disposition"] == "accepted"
        summary = acknowledgment["result"]
        full = client.get("report", summary["id"])
        assert MAX_TOOL_BYTES < len(canonical_bytes(full)) < MAX_REPORT_BYTES
        content = full["data"]
        expected_data = {
            "report_digest": content["report_digest"],
            "captured_at": content["snapshot"]["captured_at"],
            "evidence_through": content["snapshot"]["evidence_through"],
            "campaign_revision": content["snapshot"]["campaign_revision"],
            "detail_available": True,
            "detail": {
                "kind": "report",
                "id": full["id"],
                "http_path": f"/v1/records/report/{full['id']}",
                "required_scope": "read",
            },
        }
        assert summary["data"] == expected_data
        replay = await agent.call_tool("research_report_create", command)
        assert replay.structured_content == acknowledgment
        sdk_replay = client.command("report.create", request, idempotency_key="large-report")
        assert sdk_replay["command_id"] == acknowledgment["command_id"]
        assert sdk_replay["result"] == full
        arguments = {"kind": "report", "identifier": full["id"]}
        read = await agent.call_tool("research_get_record", arguments)
        assert read.structured_content is not None
        assert read.structured_content["result"] == summary
        page = await agent.call_tool(
            "research_query_records",
            {"request": {"kind": "report", "campaign_id": campaign_id}},
        )
        assert page.structured_content is not None
        assert page.structured_content["result"]["items"] == [summary]
        oversized = await agent.call_tool("research_get_record", {**arguments, "detail": True})
        assert oversized.is_error and oversized.structured_content is not None
        error = oversized.structured_content["error"]
        assert error["code"] == "CAPABILITY_UNAVAILABLE"
        assert error["details"]["record"] == summary
        assert error["details"]["response_limit_bytes"] == MAX_TOOL_BYTES
        assert error["details"]["response_bytes"] > MAX_TOOL_BYTES
        for response in (created, replay, read, page, oversized):
            assert len(canonical_bytes(response.structured_content)) < MAX_TOOL_BYTES
    runtime = pipeline.runtime
    credential = runtime.credentials.issue(runtime.principal(), runtime.principal())
    with (
        serving(create_app(runtime.service, runtime.credentials)) as url,
        httpx.Client(base_url=url) as http,
    ):
        path = expected_data["detail"]["http_path"]
        assert http.get(path).status_code == 401
        fetched = http.get(path, headers={"Authorization": f"Bearer {credential.token}"})
        assert fetched.status_code == 200
        received: dict[str, Any] = fetched.json()
        assert received == full
    assert (
        record_digest(
            "research.report",
            {key: value for key, value in content.items() if key != "report_digest"},
        )
        == content["report_digest"]
    )
    assert client.get("campaign", campaign_id) == before_campaign
    assert client.budget(campaign_id) == before_budget
    assert client.get("report", full["id"]) == full
    assert len(client.list(RecordQuery(kind="report"))["items"]) == 1
