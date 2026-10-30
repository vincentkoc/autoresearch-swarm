"""Version the code and semantics that precede project execution."""

from pathlib import Path

from research_swarm.domain.canonical import content_digest, record_digest


def launch_adapter_digest(bootstrap_digest: str, probe_digest: str) -> str:
    return record_digest(
        "research.python-launch-adapter.v1",
        {
            "bootstrap": bootstrap_digest,
            "probe": probe_digest,
            "launch_mode": "isolated-verify-then-project-v1",
        },
    )


def current_adapter_digest() -> str:
    root = Path(__file__).parent
    return launch_adapter_digest(
        content_digest((root / "_launch.py").read_bytes()),
        content_digest((root / "_probe.py").read_bytes()),
    )
