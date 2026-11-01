"""Exact optional-SDK requests; these tests do not contact a hosted bucket."""

import io

import pytest

pytest.importorskip("boto3")
from botocore.response import StreamingBody
from botocore.stub import ANY, Stubber

from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.storage.r2_config import R2Config
from research_swarm.storage.r2_transport import ObjectAbsent, S3R2Transport


@pytest.fixture
def transport(monkeypatch):
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "synthetic-test-access")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "synthetic-test-secret")
    return S3R2Transport(
        R2Config(endpoint="https://fixture.r2.cloudflarestorage.com", bucket="fixture-bucket")
    )


def test_payload_is_conditional_and_retirement_is_unconditional(transport):
    base = {
        "Bucket": "fixture-bucket",
        "Key": "fixture-key",
        "Body": ANY,
        "ContentLength": 3,
        "Metadata": {"research-state": "payload"},
        "ContentType": "application/octet-stream",
    }
    with Stubber(transport.client) as stub:
        stub.add_response(
            "put_object",
            {"ResponseMetadata": {"HTTPStatusCode": 200}},
            {**base, "IfNoneMatch": "*"},
        )
        assert transport.put("fixture-key", io.BytesIO(b"abc"), 3, base["Metadata"], if_absent=True)
        stub.add_client_error(
            "put_object",
            service_error_code="PreconditionFailed",
            http_status_code=412,
            expected_params={**base, "IfNoneMatch": "*"},
        )
        assert not transport.put(
            "fixture-key", io.BytesIO(b"abc"), 3, base["Metadata"], if_absent=True
        )
        fence = {**base, "ContentLength": 0, "Metadata": {"research-state": "retired"}}
        stub.add_response("put_object", {"ResponseMetadata": {"HTTPStatusCode": 200}}, fence)
        assert transport.put("fixture-key", io.BytesIO(), 0, fence["Metadata"], if_absent=False)
        stub.assert_no_pending_responses()
    assert transport.client.meta.config.retries["total_max_attempts"] == 1
    assert transport.client.meta.config.proxies == {}


def test_object_stream_closes_on_consumer_failure_and_absence_is_distinct(transport):
    body = StreamingBody(io.BytesIO(b"abc"), 3)
    parameters = {"Bucket": "fixture-bucket", "Key": "fixture-key"}
    with Stubber(transport.client) as stub:
        stub.add_response(
            "get_object",
            {"Body": body, "ContentLength": 3, "Metadata": {"research-state": "payload"}},
            parameters,
        )
        with pytest.raises(ResearchError) as failed, transport.get("fixture-key") as remote:
            assert remote.body.read(1) == b"a"
            raise OSError("private fixture error")
        assert failed.value.code == ErrorCode.TRANSPORT_UNAVAILABLE
        assert "private" not in str(failed.value)
        assert body._raw_stream.closed
        stub.add_client_error(
            "get_object",
            service_error_code="NoSuchKey",
            http_status_code=404,
            expected_params=parameters,
        )
        with pytest.raises(ObjectAbsent), transport.get("fixture-key"):
            pytest.fail("An absent key cannot expose a body")


def test_authentication_and_transport_errors_are_sanitized(transport):
    with Stubber(transport.client) as stub:
        stub.add_client_error(
            "get_object",
            service_error_code="AccessDenied",
            service_message="private credential detail",
            http_status_code=403,
            expected_params={"Bucket": "fixture-bucket", "Key": "fixture-key"},
        )
        with pytest.raises(ResearchError) as denied, transport.get("fixture-key"):
            pytest.fail("Denied read cannot expose a body")
        assert denied.value.retryable and "private" not in str(denied.value)


def test_named_credentials_are_required_without_ambient_fallback(monkeypatch):
    monkeypatch.delenv("R2_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("R2_SECRET_ACCESS_KEY", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "unrelated-fixture-access")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "unrelated-fixture-secret")
    with pytest.raises(ResearchError) as absent:
        S3R2Transport(
            R2Config(endpoint="https://fixture.r2.cloudflarestorage.com", bucket="fixture-bucket")
        )
    assert absent.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
