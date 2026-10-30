"""Opaque file identities for a locally frozen registered launch."""

from pydantic import field_validator

from research_swarm.domain.types import Digest, VersionedRecord
from research_swarm.projects.workload import absolute_path


class EngineFile(VersionedRecord):
    path: str
    digest: Digest
    _path = field_validator("path")(absolute_path)


class EngineBinding(VersionedRecord):
    registration: EngineFile
    bootstrap: EngineFile
    probe: EngineFile
    payload: EngineFile
