"""Small synchronous R2 object operations, with an optional S3 SDK boundary."""

from __future__ import annotations

import importlib
import os
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Any, BinaryIO, Protocol

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.storage.r2_config import R2Config


class ObjectAbsent(Exception):
    """An absent read is observation, never proof that an outstanding write has ended."""


@dataclass(frozen=True)
class RemoteObject:
    body: BinaryIO
    size_bytes: int
    metadata: Mapping[str, str]


class R2Transport(Protocol):
    @property
    def configuration_digest(self) -> str: ...
    def get(self, key: str) -> AbstractContextManager[RemoteObject]: ...
    def put(
        self,
        key: str,
        body: BinaryIO,
        size_bytes: int,
        metadata: Mapping[str, str],
        *,
        if_absent: bool,
    ) -> bool: ...


def unavailable() -> ResearchError:
    return ResearchError(
        ErrorCode.TRANSPORT_UNAVAILABLE, "R2 operation was not confirmed", retryable=True
    )


class S3R2Transport:
    def __init__(self, config: R2Config) -> None:
        try:
            boto3 = importlib.import_module("boto3")
            sdk_config = importlib.import_module("botocore.config").Config
        except ImportError:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE,
                "Install the optional s3 dependency for R2 storage",
            ) from None
        access, secret = (
            os.environ.get(config.access_key_env),
            os.environ.get(config.secret_key_env),
        )
        if not access or not secret:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "Configured R2 credential sources are unavailable"
            )
        self.config = config
        self.client = boto3.client(
            "s3",
            endpoint_url=config.endpoint,
            region_name="auto",
            aws_access_key_id=access,
            aws_secret_access_key=secret,
            config=sdk_config(
                signature_version="s3v4",
                connect_timeout=10,
                read_timeout=30,
                retries={"total_max_attempts": 1},
                proxies={},
                s3={"addressing_style": "path"},
                request_checksum_calculation="when_required",
                response_checksum_validation="when_required",
            ),
        )
        members = self.client.meta.service_model.operation_model("PutObject").input_shape.members
        if "IfNoneMatch" not in members:
            raise ResearchError(
                ErrorCode.CAPABILITY_UNAVAILABLE, "R2 requires conditional PUT support"
            )

    @property
    def configuration_digest(self) -> str:
        return self.config.digest

    @staticmethod
    def _status(error: Exception) -> int | None:
        response = getattr(error, "response", {})
        if not isinstance(response, dict):
            return None
        metadata = response.get("ResponseMetadata", {})
        value = metadata.get("HTTPStatusCode") if isinstance(metadata, dict) else None
        return value if type(value) is int else None

    @contextmanager
    def get(self, key: str) -> Iterator[RemoteObject]:
        try:
            response = self.client.get_object(Bucket=self.config.bucket, Key=key)
        except Exception as error:
            if self._status(error) == 404:
                raise ObjectAbsent from None
            raise unavailable() from None
        body = response.get("Body")
        if body is None:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "R2 response has no body")
        try:
            size, metadata = response.get("ContentLength"), response.get("Metadata", {})
            if (
                type(size) is not int
                or size < 0
                or not isinstance(metadata, dict)
                or any(
                    not isinstance(k, str) or not isinstance(v, str) for k, v in metadata.items()
                )
            ):
                raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Invalid R2 object metadata")
            yield RemoteObject(body, size, metadata)
        except ResearchError:
            raise
        except Exception:
            raise unavailable() from None
        finally:
            try:
                body.close()
            except Exception:
                raise unavailable() from None

    def put(
        self,
        key: str,
        body: BinaryIO,
        size_bytes: int,
        metadata: Mapping[str, str],
        *,
        if_absent: bool,
    ) -> bool:
        arguments: dict[str, Any] = {
            "Bucket": self.config.bucket,
            "Key": key,
            "Body": body,
            "ContentLength": size_bytes,
            "Metadata": dict(metadata),
            "ContentType": "application/octet-stream",
        }
        if if_absent:
            arguments["IfNoneMatch"] = "*"
        try:
            response = self.client.put_object(**arguments)
        except Exception as error:
            if if_absent and self._status(error) == 412:
                return False
            raise unavailable() from None
        if response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 200:
            raise unavailable()
        return True
