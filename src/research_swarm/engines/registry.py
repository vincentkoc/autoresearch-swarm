"""Small engine registry, without importing optional training frameworks."""

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.engines.models import EngineDefinition

ENGINE_DEFINITIONS = (
    EngineDefinition(name="python-cpu", framework="python", device_backend="cpu"),
    EngineDefinition(
        name="pytorch-cuda", framework="pytorch", device_backend="cuda", import_name="torch"
    ),
    EngineDefinition(
        name="pytorch-rocm", framework="pytorch", device_backend="rocm", import_name="torch"
    ),
    EngineDefinition(
        name="pytorch-mps", framework="pytorch", device_backend="mps", import_name="torch"
    ),
    EngineDefinition(
        name="mlx-metal", framework="mlx", device_backend="metal", import_name="mlx.core"
    ),
)


def engine_definition(name: str) -> EngineDefinition:
    for definition in ENGINE_DEFINITIONS:
        if definition.name == name:
            return definition
    raise ResearchError(ErrorCode.CAPABILITY_UNAVAILABLE, "Execution profile is not registered")
