"""Shared R2 generation layout; payloads and permanent retirement use one key."""

from research_swarm.domain.artifacts import StoredBlob


def generation_key(prefix: str, workspace_id: str, blob: StoredBlob) -> str:
    return "/".join((prefix, workspace_id, blob.reference.digest[7:], blob.generation))


def generation_metadata(blob: StoredBlob, *, retired: bool = False) -> dict[str, str]:
    return {
        "research-state": "retired" if retired else "payload",
        "research-generation": blob.generation,
        "research-digest": blob.reference.digest[7:],
        "research-size": str(blob.reference.size_bytes),
    }
