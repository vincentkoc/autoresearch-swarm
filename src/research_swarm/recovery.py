"""Verify frozen snapshot items before a caller installs a recovered read model."""

from __future__ import annotations

import base64
import binascii
from collections.abc import Iterator

from research_swarm.client import ResearchClient
from research_swarm.domain.canonical import content_digest, strict_json_loads
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.event_recovery import RecoveryItem, RecoveryPage, RecoverySnapshot
from research_swarm.state.database import JsonObject


class SnapshotReader:
    """Stage every item before installing; iteration never releases the retention pin."""

    def __init__(
        self,
        client: ResearchClient,
        snapshot: RecoverySnapshot,
        *,
        max_item_bytes: int = 32 * 1024 * 1024,
    ) -> None:
        if type(max_item_bytes) is not int or not 1 <= max_item_bytes <= 256 * 1024 * 1024:
            raise ValueError("Snapshot item bound must be between one byte and 256 MiB")
        self.client, self.snapshot, self.max_item_bytes = client, snapshot, max_item_bytes
        self.completed = False

    def _item(self, descriptor: JsonObject) -> JsonObject:
        size = descriptor["size_bytes"]
        if type(size) is not int or not 0 < size <= self.max_item_bytes:
            raise ResearchError(
                ErrorCode.RESOURCE_UNAVAILABLE, "Recovery item exceeds the reader bound"
            )
        content = bytearray()
        while len(content) < size:
            request = RecoveryItem(
                snapshot_id=self.snapshot.id, ordinal=descriptor["ordinal"], offset=len(content)
            )
            part = self.client.snapshot_item(request)
            try:
                block = base64.b64decode(part["content_base64"].encode("ascii"), validate=True)
                if (
                    part["snapshot_id"] != self.snapshot.id
                    or part["ordinal"] != request.ordinal
                    or part["offset"] != len(content)
                    or part["next_offset"] != len(content) + len(block)
                    or part["size_bytes"] != size
                    or part["content_digest"] != descriptor["content_digest"]
                    or not 0 < len(block) <= min(request.limit, size - len(content))
                ):
                    raise ValueError("Recovery chunk differs from its manifest")
            except (KeyError, TypeError, ValueError, UnicodeError, binascii.Error) as error:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Invalid recovery chunk"
                ) from error
            content.extend(block)
        if content_digest(bytes(content)) != descriptor["content_digest"]:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Recovery content digest differs")
        value = strict_json_loads(bytes(content))
        if not isinstance(value, dict):
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Recovery item is not an object")
        return value

    def items(self, *, page_size: int = 50) -> Iterator[JsonObject]:
        self.completed = False
        cursor = None
        count = total = 0
        while True:
            page = self.client.snapshot_page(
                RecoveryPage(snapshot_id=self.snapshot.id, limit=page_size, cursor=cursor)
            )
            if RecoverySnapshot.model_validate(page["snapshot"]) != self.snapshot:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Recovery snapshot metadata changed"
                )
            for descriptor in page["items"]:
                if descriptor["ordinal"] != count + 1 or count >= self.snapshot.item_count:
                    raise ResearchError(
                        ErrorCode.ARTIFACT_UNVERIFIED, "Recovery manifest order changed"
                    )
                value = self._item(descriptor)
                count += 1
                total += descriptor["size_bytes"]
                yield value
            following = page["next_cursor"]
            if following is None:
                break
            if following == cursor or not page["items"]:
                raise ResearchError(
                    ErrorCode.ARTIFACT_UNVERIFIED, "Recovery cursor made no progress"
                )
            cursor = following
        if count != self.snapshot.item_count or total != self.snapshot.size_bytes:
            raise ResearchError(ErrorCode.ARTIFACT_UNVERIFIED, "Recovery snapshot is incomplete")
        self.completed = True
