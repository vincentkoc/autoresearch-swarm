from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from research_swarm.client import HTTPTransport
from research_swarm.control.worker_artifacts import WorkerArtifactRead
from research_swarm.domain.artifacts import ArtifactRecord
from research_swarm.domain.canonical import content_digest
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.evidence import ArtifactRef
from research_swarm.domain.types import new_id, utc_now


class BrokenStream(httpx.SyncByteStream):
    closed = False
    yielded = False

    def __iter__(self) -> Iterator[bytes]:
        self.yielded = True
        yield b"x" * (64 * 1024)
        raise httpx.ReadError("private transport detail")

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize("worker", [False, True])
@pytest.mark.parametrize("failure", ["open", "body", "existing-destination"])
def test_download_failures_are_typed_and_preserve_existing_files(
    tmp_path: Path, worker: bool, failure: str
) -> None:
    content = b"x" * (64 * 1024) + b"!"
    reference = ArtifactRef(id=new_id(), digest=content_digest(content), size_bytes=len(content))
    record = ArtifactRecord(
        reference=reference,
        storage_generation=new_id(),
        workspace_id=new_id(),
        published_by=new_id(),
        verified_at=utc_now(),
    )
    stream = BrokenStream()

    def response(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/content"):
            if failure == "open":
                raise httpx.ConnectError("private transport detail")
            return httpx.Response(200, stream=stream)
        metadata = reference if worker else record
        return httpx.Response(200, json=metadata.model_dump(mode="json"))

    transport = HTTPTransport("http://127.0.0.1", "fixture-token")
    transport.http.close()
    transport.http = httpx.Client(
        base_url="http://fixture", transport=httpx.MockTransport(response)
    )
    destination = tmp_path / "download.bin"
    if failure == "existing-destination":
        destination.write_bytes(b"keep existing bytes")
    try:
        expected = FileExistsError if failure == "existing-destination" else ResearchError
        with pytest.raises(expected) as caught:
            if worker:
                transport.worker_download(
                    WorkerArtifactRead(
                        worker_id=new_id(),
                        session_id=new_id(),
                        attempt_id=new_id(),
                        artifact_id=reference.id,
                    ),
                    destination,
                )
            else:
                transport.download(reference.id, destination)
        if failure == "existing-destination":
            assert destination.read_bytes() == b"keep existing bytes"
            assert not stream.yielded
        else:
            error = caught.value
            assert isinstance(error, ResearchError)
            assert error.code == ErrorCode.TRANSPORT_UNAVAILABLE
            assert error.retryable
            assert error.details == {"recovery": "retry_query"}
            assert "private transport detail" not in str(error.as_dict())
            assert not destination.exists()
        if failure != "open":
            assert stream.closed
        if failure == "body":
            assert stream.yielded
    finally:
        transport.close()
