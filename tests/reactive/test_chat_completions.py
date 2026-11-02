from __future__ import annotations

import asyncio
import json

import httpx
import pytest
from pydantic import ValidationError

from research_swarm.reactive.chat_completions import ChatCompletionsConfig, ChatCompletionsPolicy
from research_swarm.reactive.contracts import DecisionRequest, ReactiveContract
from research_swarm.reactive.journal import DecisionJournal
from tests.reactive.support import request_for


def config(**changes: object) -> ChatCompletionsConfig:
    return ChatCompletionsConfig.model_validate(
        {
            "endpoint": "https://api.example.test/v1/chat/completions",
            "model": "fixture-model",
            **changes,
        }
    )


def bound(
    journal: DecisionJournal, settings: ChatCompletionsConfig, **changes: object
) -> tuple[DecisionRequest, ReactiveContract]:
    contract = ReactiveContract.model_validate(
        journal.inspect().contract.model_dump() | {"policy_digest": settings.digest} | changes
    )
    request = request_for(journal).model_copy(
        update={"contract_digest": contract.digest, "policy_digest": contract.policy_digest}
    )
    return request, contract


def envelope(content: str | None = None) -> dict[str, object]:
    return {
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "message": {
                    "role": "assistant",
                    "content": content
                    or json.dumps(
                        {
                            "kind": "set",
                            "values": [{"name": "lr", "value": 0.2}],
                            "rationale": "fixture change",
                        }
                    ),
                },
            }
        ],
        "usage": {"prompt_tokens": 100, "completion_tokens": 20},
    }


async def test_wire_contract_bounds_context_and_key_separation(journal: DecisionJournal) -> None:
    settings = config(max_output_tokens=64)
    request, contract = bound(journal, settings, max_output_tokens=32)
    captured: list[httpx.Request] = []

    def handler(wire: httpx.Request) -> httpx.Response:
        captured.append(wire)
        return httpx.Response(200, json=envelope())

    policy = ChatCompletionsPolicy(
        settings, api_key="fixture-api-key", transport=httpx.MockTransport(handler)
    )
    proposal = await policy.decide(request, contract)
    assert len(captured) == 1
    wire = captured[0]
    body = json.loads(wire.content)
    assert body["n"] == 1 and body["stream"] is False and body["store"] is False
    assert body["max_completion_tokens"] == 32 and "max_tokens" not in body
    assert body["response_format"]["json_schema"]["strict"] is True
    context = json.loads(body["messages"][1]["content"])
    assert context["contract"]["controls"][0]["minimum"] == 0.0
    assert context["contract"]["controls"][0]["maximum"] == 1.0
    assert context["contract"]["diagnostics"][0]["source"] == "train"
    assert "JSON" in body["messages"][0]["content"]
    assert wire.headers["Authorization"] == "Bearer fixture-api-key"
    assert (
        "fixture-api-key"
        not in settings.model_dump_json() + wire.content.decode() + proposal.model_dump_json()
    )
    assert (
        proposal.request_digest == request.digest
        and proposal.base_revision == request.control_revision
    )
    assert proposal.usage is not None and proposal.usage.source == "provider_reported"
    assert proposal.usage.output_tokens == 20


async def test_json_mode_and_legacy_token_field_require_explicit_configuration(
    journal: DecisionJournal,
) -> None:
    settings = config(response_format="json_object", token_parameter="max_tokens")
    request, contract = bound(journal, settings)

    def handler(wire: httpx.Request) -> httpx.Response:
        body = json.loads(wire.content)
        assert body["response_format"] == {"type": "json_object"}
        assert "max_tokens" in body and "max_completion_tokens" not in body
        return httpx.Response(200, json=envelope())

    await ChatCompletionsPolicy(settings, transport=httpx.MockTransport(handler)).decide(
        request, contract
    )
    assert settings.digest != config().digest


@pytest.mark.parametrize("status", [302, 429, 500])
async def test_no_redirects_retries_or_format_fallback(
    journal: DecisionJournal, status: int
) -> None:
    settings = config()
    request, contract = bound(journal, settings)
    calls = 0

    def handler(wire: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(status, headers={"Location": "https://elsewhere.example.test"})

    with pytest.raises(ValueError, match="successful response"):
        await ChatCompletionsPolicy(settings, transport=httpx.MockTransport(handler)).decide(
            request, contract
        )
    assert calls == 1


@pytest.mark.parametrize(
    "content",
    [
        '{"kind":"keep","kind":"set","values":[],"rationale":""}',
        '{"kind":"set","values":[{"name":"lr","value":NaN}],"rationale":""}',
        '{"kind":"set","values":[{"name":"lr","value":9.0}],"rationale":""}',
        '{"kind":"set","values":[{"name":"other","value":0.2}],"rationale":""}',
        '{"kind":"set","values":[{"name":"lr","value":true}],"rationale":""}',
        '{"kind":"set","values":[{"name":"lr","value":"0.2"}],"rationale":""}',
        '{"kind":"keep","values":[],"rationale":"","applied":true}',
        '{"kind":"keep","values":[],"rationale":"","request_id":"invented"}',
        '{"kind":"set","values":[{"name":"lr","value":0.2},{"name":"lr","value":0.3}],"rationale":""}',
    ],
)
async def test_strict_json_and_domain_validation(journal: DecisionJournal, content: str) -> None:
    settings = config()
    request, contract = bound(journal, settings)
    policy = ChatCompletionsPolicy(
        settings,
        transport=httpx.MockTransport(lambda wire: httpx.Response(200, json=envelope(content))),
    )
    with pytest.raises(ValueError, match="Invalid or out-of-contract"):
        await policy.decide(request, contract)


@pytest.mark.parametrize(
    "choice",
    [
        {"finish_reason": "length", "message": {"role": "assistant", "content": "{}"}},
        {
            "finish_reason": "stop",
            "message": {"role": "assistant", "content": "{}", "refusal": "declined"},
        },
        {
            "finish_reason": "stop",
            "message": {"role": "assistant", "content": "{}", "tool_calls": [{"id": "fixture"}]},
        },
    ],
)
async def test_partial_refused_or_tool_responses_are_rejected(
    journal: DecisionJournal, choice: dict[str, object]
) -> None:
    settings = config()
    request, contract = bound(journal, settings)
    policy = ChatCompletionsPolicy(
        settings,
        transport=httpx.MockTransport(lambda wire: httpx.Response(200, json={"choices": [choice]})),
    )
    with pytest.raises(ValueError, match="Invalid or out-of-contract"):
        await policy.decide(request, contract)


async def test_framing_limit_is_checked_before_network(journal: DecisionJournal) -> None:
    settings = config()
    request, contract = bound(journal, settings, max_prompt_bytes=2048)

    def forbidden(wire: httpx.Request) -> httpx.Response:
        raise AssertionError("No HTTP call should occur")

    with pytest.raises(ValueError, match="Framed LLM"):
        await ChatCompletionsPolicy(settings, transport=httpx.MockTransport(forbidden)).decide(
            request, contract
        )


async def test_http_bytes_and_deadline_are_bounded(journal: DecisionJournal) -> None:
    settings = config(max_http_response_bytes=1024, timeout_ms=10)
    request, contract = bound(journal, settings)
    oversized = httpx.MockTransport(lambda wire: httpx.Response(200, content=b"x" * 2000))
    with pytest.raises(ValueError, match="HTTP byte limit"):
        await ChatCompletionsPolicy(settings, transport=oversized).decide(request, contract)

    async def blocked(wire: httpx.Request) -> httpx.Response:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    with pytest.raises(ValueError, match="deadline"):
        await ChatCompletionsPolicy(settings, transport=httpx.MockTransport(blocked)).decide(
            request, contract
        )


def test_endpoint_cannot_embed_credentials_or_silently_use_plain_http() -> None:
    for endpoint in (
        "https://user:secret@example.test/chat",
        "https://example.test/chat?key=secret",
        "http://localhost:8080/chat",
    ):
        with pytest.raises(ValidationError):
            config(endpoint=endpoint)
    assert config(endpoint="http://localhost:8080/v1/chat/completions", allow_insecure_http=True)
