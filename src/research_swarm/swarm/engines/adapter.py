"""Version the code and semantics that precede project execution."""

from importlib.resources import files

from research_swarm.swarm.canonical import content_digest, record_digest


def launch_adapter_digest(bootstrap_digest: str, probe_digest: str) -> str:
    return record_digest(
        "research.python-launch-adapter.v1",
        {
            "bootstrap": bootstrap_digest,
            "probe": probe_digest,
            "launch_mode": "isolated-verify-then-project-v1",
        },
    )


def script_bytes(name: str) -> bytes:
    # Existing environment registrations pin these standalone package resources.
    return files("research_swarm.engines").joinpath(name).read_bytes()


def current_adapter_digest() -> str:
    return launch_adapter_digest(
        content_digest(script_bytes("_launch.py")),
        content_digest(script_bytes("_probe.py")),
    )
