"""Persisted storage selection, independent of optional provider SDKs."""

from typing import Annotated, Literal

from pydantic import Field, TypeAdapter

from research_swarm.domain.types import VersionedRecord
from research_swarm.storage.r2_config import R2Config


class FilesystemConfig(VersionedRecord):
    kind: Literal["filesystem"] = "filesystem"


StorageConfig = Annotated[FilesystemConfig | R2Config, Field(discriminator="kind")]
storage_configuration: TypeAdapter[StorageConfig] = TypeAdapter(StorageConfig)
