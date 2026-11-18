"""Authenticated HTTP reads and local stdio tools for one campaign database."""

from __future__ import annotations

import hmac
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse
from mcp.server.mcpserver import MCPServer
from mcp_types import ToolAnnotations

from research_swarm.api.campaign_ui import campaign_shell
from research_swarm.api.worker import WorkerGrant, worker_router
from research_swarm.control.report_view import render_report_html
from research_swarm.domain.experiments import MetricDefinition
from research_swarm.swarm.reader import CampaignReader


def campaign_app(
    reader: CampaignReader, token: str, *, worker: WorkerGrant | None = None
) -> FastAPI:
    if not token or not token.isascii() or any(c.isspace() for c in token):
        raise ValueError("Choose a nonempty ASCII bearer token without whitespace")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

    def authorize(
        response: Response, authorization: Annotated[str | None, Header()] = None
    ) -> None:
        presented = (authorization or "").partition(" ")
        if presented[0].lower() != "bearer" or not hmac.compare_digest(
            presented[2].encode(), token.encode()
        ):
            raise HTTPException(
                401, "Bearer token required", headers={"WWW-Authenticate": "Bearer"}
            )

        response.headers["Cache-Control"] = "no-store"

    protected = [Depends(authorize)]

    @app.exception_handler(ValueError)
    async def invalid_request(request: Any, error: ValueError) -> JSONResponse:
        status = 404 if str(error).startswith("Unknown campaign ") else 400
        return JSONResponse(
            {"detail": str(error)}, status_code=status, headers={"Cache-Control": "no-store"}
        )

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/", response_class=HTMLResponse)
    def home() -> HTMLResponse:
        return HTMLResponse(campaign_shell(), headers={"Cache-Control": "no-store"})

    @app.get("/v1/campaigns", dependencies=protected)
    def campaigns(
        cursor: str | None = None, limit: Annotated[int, Query(ge=1, le=100)] = 100
    ) -> dict[str, Any]:
        return reader.campaigns(cursor, limit)

    @app.get("/v1/campaigns/{campaign_id:path}/snapshot", dependencies=protected)
    def snapshot(campaign_id: str) -> dict[str, Any]:
        result = reader.snapshot(campaign_id)
        return {**result, "observed_cohorts": [{"comparison_digest": result["comparison_digest"]}]}

    @app.get(
        "/v1/campaigns/{campaign_id:path}/view", dependencies=protected, response_class=HTMLResponse
    )
    def view(campaign_id: str, comparison_digest: str | None = None) -> HTMLResponse:
        result = reader.snapshot(campaign_id)
        cohort = result["comparison_digest"]
        if comparison_digest is not None and comparison_digest != cohort:
            raise ValueError("Unknown campaign comparison cohort")
        metric = MetricDefinition.model_validate(result["research"]["objective"]["primary_metric"])
        return HTMLResponse(
            render_report_html(
                [result], comparison_digest=cohort, metric=metric, title=campaign_id
            ),
            headers={"Cache-Control": "no-store"},
        )

    @app.get("/v1/campaigns/{campaign_id:path}/budget", dependencies=protected)
    def budget(campaign_id: str) -> dict[str, Any]:
        return reader.budget(campaign_id)

    @app.get("/v1/events", dependencies=protected)
    def events(after: int = 0, limit: int = 100, campaign_id: str | None = None) -> dict[str, Any]:
        return reader.events(after, limit, campaign_id)

    if worker is not None:
        if worker.token == token:
            raise ValueError("Read and worker bearers must be distinct")
        app.include_router(worker_router(reader.database, worker))
    return app


def campaign_tools(reader: CampaignReader) -> MCPServer:
    server = MCPServer("research-campaign")
    read = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)
    server.tool(name="research_campaigns", annotations=read)(reader.campaigns)
    server.tool(name="research_campaign_snapshot", annotations=read)(reader.snapshot)
    server.tool(name="research_budget", annotations=read)(reader.budget)
    server.tool(name="research_events", annotations=read)(reader.events)
    return server
