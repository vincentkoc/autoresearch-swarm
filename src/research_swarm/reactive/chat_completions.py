"""Opt-in Chat Completions HTTP policy with explicit endpoint and wire format."""

from __future__ import annotations

import asyncio
from typing import Any, Literal, Self
from urllib.parse import urlsplit

import httpx
from pydantic import Field, model_validator

from research_swarm.domain.canonical import canonical_bytes, record_digest, strict_json_loads
from research_swarm.domain.types import Name, Record, VersionedRecord
from research_swarm.reactive.contracts import (
    ControlName,
    DecisionProposal,
    DecisionRequest,
    NumericValue,
    ReactiveContract,
    ReportedUsage,
    Scalar,
    ordered,
    proposed_controls,
    validate_request,
)

_INSTRUCTIONS = (
    "Return one JSON object with kind (keep or set), values (an array of name/value "
    "objects), and rationale (a short string). Keep requires empty values; set requires "
    "at least one value. Propose only declared numeric controls within their bounds. "
    "Training and development diagnostics are observations, not evaluator authority. "
    "You propose changes; you cannot claim they were applied. Do not return request IDs, "
    "revisions, code, tool calls or additional fields."
)


class ChatCompletionsConfig(VersionedRecord):
    endpoint: str = Field(min_length=1, max_length=2048)
    model: Name
    prompt: str = Field(
        default="Choose a bounded training control adjustment or keep the current values.",
        max_length=2048,
    )
    response_format: Literal["json_schema", "json_object"] = "json_schema"
    token_parameter: Literal["max_completion_tokens", "max_tokens"] = "max_completion_tokens"
    max_output_tokens: int = Field(default=1024, strict=True, ge=1, le=16384)
    timeout_ms: int = Field(default=30000, strict=True, ge=1, le=120000)
    max_http_response_bytes: int = Field(default=65536, strict=True, ge=1024, le=262144)
    allow_insecure_http: bool = False

    @model_validator(mode="after")
    def valid_endpoint(self) -> Self:
        parsed = urlsplit(self.endpoint)
        if (
            any(ord(character) <= 32 for character in self.endpoint)
            or parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Endpoint must be an absolute HTTP URL without credentials, query or fragment"
            )
        if parsed.scheme == "http" and not self.allow_insecure_http:
            raise ValueError("Plain HTTP requires explicit allow_insecure_http configuration")
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("Invalid endpoint port")
        return self

    @property
    def digest(self) -> str:
        return record_digest(
            "research.reactive-chat-completions",
            {
                "config": self.model_dump(mode="json"),
                "instructions": _INSTRUCTIONS,
                "output_schema": "reactive-decision-v1",
            },
        )


class _ControlValue(Record):
    name: ControlName
    value: Scalar


class _ModelReply(Record):
    kind: Literal["keep", "set"]
    values: tuple[_ControlValue, ...] = Field(max_length=32)
    rationale: str = Field(max_length=2048)


def _response_format(config: ChatCompletionsConfig, contract: ReactiveContract) -> dict[str, Any]:
    if config.response_format == "json_object":
        return {"type": "json_object"}
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "reactive_decision_v1",
            "strict": True,
            "schema": {
                "type": "object",
                "additionalProperties": False,
                "required": ["kind", "values", "rationale"],
                "properties": {
                    "kind": {"type": "string", "enum": ["keep", "set"]},
                    "rationale": {"type": "string"},
                    "values": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["name", "value"],
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "enum": [item.name for item in contract.controls],
                                },
                                "value": {"type": "number"},
                            },
                        },
                    },
                },
            },
        },
    }


class ChatCompletionsPolicy:
    def __init__(
        self,
        config: ChatCompletionsConfig,
        *,
        api_key: str | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.config = ChatCompletionsConfig.model_validate(config)
        if api_key is not None and (
            not 1 <= len(api_key) <= 4096
            or any(not 33 <= ord(character) <= 126 for character in api_key)
        ):
            raise ValueError("API key must be a bounded printable header value")
        self._key, self._transport = api_key, transport

    @property
    def digest(self) -> str:
        return self.config.digest

    def _body(self, request: DecisionRequest, contract: ReactiveContract) -> bytes:
        validate_request(contract, request)
        if self.digest != contract.policy_digest:
            raise ValueError("HTTP policy differs from the admitted fingerprint")
        context = canonical_bytes(
            {
                "contract": contract.model_dump(mode="json"),
                "request": request.model_dump(mode="json"),
            }
        ).decode("utf-8")
        body = canonical_bytes(
            {
                "model": self.config.model,
                "n": 1,
                "stream": False,
                "store": False,
                self.config.token_parameter: min(
                    self.config.max_output_tokens, contract.max_output_tokens
                ),
                "response_format": _response_format(self.config, contract),
                "messages": [
                    {"role": "system", "content": _INSTRUCTIONS + "\n" + self.config.prompt},
                    {"role": "user", "content": context},
                ],
            }
        )
        # Count all prompt/schema/config framing, not just diagnostic JSON.
        if len(body) > contract.max_prompt_bytes:
            raise ValueError("Framed LLM request exceeds the prompt byte limit")
        return body

    async def decide(
        self, request: DecisionRequest, contract: ReactiveContract
    ) -> DecisionProposal:
        body = self._body(request, contract)
        timeout = min(self.config.timeout_ms, contract.decision_timeout_ms) / 1000
        headers = {"Content-Type": "application/json", "Accept-Encoding": "identity"}
        if self._key is not None:
            headers["Authorization"] = "Bearer " + self._key
        transport = self._transport or httpx.AsyncHTTPTransport(retries=0, trust_env=False)
        try:
            async with (
                asyncio.timeout(timeout),
                httpx.AsyncClient(
                    transport=transport,
                    timeout=timeout,
                    follow_redirects=False,
                    trust_env=False,
                ) as client,
                client.stream(
                    "POST", self.config.endpoint, content=body, headers=headers
                ) as response,
            ):
                if response.status_code != 200:
                    raise ValueError("LLM endpoint did not return a successful response")
                if response.headers.get("Content-Encoding", "identity").lower() != "identity":
                    raise ValueError("Compressed LLM responses are not accepted")
                content = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=8192):
                    if len(content) + len(chunk) > self.config.max_http_response_bytes:
                        raise ValueError("LLM response exceeds the HTTP byte limit")
                    content.extend(chunk)
            return self._parse(bytes(content), request, contract)
        except (httpx.HTTPError, TimeoutError):
            raise ValueError("LLM HTTP call failed or exceeded its deadline") from None

    def _parse(
        self, content: bytes, request: DecisionRequest, contract: ReactiveContract
    ) -> DecisionProposal:
        try:
            envelope = strict_json_loads(content)
            choices = envelope["choices"]
            if (
                not isinstance(choices, list)
                or len(choices) != 1
                or choices[0].get("finish_reason") != "stop"
            ):
                raise ValueError("LLM response must have one complete choice")
            message = choices[0]["message"]
            if (
                message.get("role") != "assistant"
                or message.get("refusal") is not None
                or message.get("tool_calls") not in (None, [])
                or message.get("function_call") is not None
            ):
                raise ValueError("LLM response is not a plain assistant decision")
            text = message["content"]
            if not isinstance(text, str) or len(text.encode("utf-8")) > contract.max_response_bytes:
                raise ValueError("LLM content exceeds its bound or is not text")
            reply = _ModelReply.model_validate(strict_json_loads(text))
            reported = envelope.get("usage")
            usage = (
                None
                if reported is None
                else ReportedUsage(
                    source="provider_reported",
                    input_tokens=reported.get("prompt_tokens"),
                    output_tokens=reported.get("completion_tokens"),
                )
            )
            proposal = DecisionProposal(
                request_id=request.id,
                request_digest=request.digest,
                base_revision=request.control_revision,
                kind=reply.kind,
                values=ordered(
                    tuple(NumericValue(name=item.name, value=item.value) for item in reply.values)
                ),
                rationale=reply.rationale,
                usage=usage,
            )
            proposed_controls(contract, request, proposal, step=request.requested_step)
            return proposal
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
            raise ValueError("Invalid or out-of-contract LLM response") from None
