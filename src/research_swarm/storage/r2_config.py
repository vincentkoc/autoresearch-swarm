"""Immutable R2 destination and credential-source names; never credential values."""

from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, StringConstraints, model_validator

from research_swarm.domain.canonical import record_digest
from research_swarm.domain.types import VersionedRecord

MAX_OBJECT_BYTES = 5 * 1024**3
EnvironmentName = Annotated[str, StringConstraints(pattern=r"^[A-Z_][A-Z0-9_]{0,127}$")]


class R2Config(VersionedRecord):
    kind: Literal["r2"] = "r2"
    endpoint: str = Field(max_length=512)
    bucket: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
    prefix: str = Field(
        default="research", pattern=r"^[a-zA-Z0-9_-]+(?:/[a-zA-Z0-9_-]+)*$", max_length=256
    )
    max_object_bytes: int = Field(default=MAX_OBJECT_BYTES, strict=True, ge=0, le=MAX_OBJECT_BYTES)
    access_key_env: EnvironmentName = "R2_ACCESS_KEY_ID"
    secret_key_env: EnvironmentName = "R2_SECRET_ACCESS_KEY"

    @model_validator(mode="after")
    def fixed_destination(self) -> "R2Config":
        url = urlsplit(self.endpoint)
        if (
            url.scheme != "https"
            or url.username
            or url.password
            or url.port is not None
            or url.path not in {"", "/"}
            or url.query
            or url.fragment
            or not url.hostname
            or not url.hostname.endswith(".r2.cloudflarestorage.com")
            or self.endpoint != "https://" + url.hostname
            or self.access_key_env == self.secret_key_env
        ):
            raise ValueError(
                "R2 requires a fixed HTTPS account endpoint and distinct credential sources"
            )
        return self

    @property
    def digest(self) -> str:
        return record_digest("research.r2-storage-v1", self.model_dump(mode="json"))
