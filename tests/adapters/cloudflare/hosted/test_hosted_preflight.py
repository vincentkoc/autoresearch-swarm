"""Shared preflight seams do not enable hosted execution or qualify its clock."""

import pytest

from research_swarm.control.auth import CredentialStore
from research_swarm.control.dispatch import COMMANDS, validate_command_input
from research_swarm.control.workers import EnrollWorker
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.types import new_id


@pytest.mark.parametrize("expiring", [False, True])
def test_nonexpiring_resolution_never_consults_wall_time(
    database, principal, monkeypatch, expiring
):
    credentials = CredentialStore(database)
    issued = credentials.issue(
        principal, principal, expires_at="2999-01-01T00:00:00.000Z" if expiring else None
    )
    monkeypatch.setattr(
        "research_swarm.control.auth.utc_now", lambda: pytest.fail("Unqualified UTC was read")
    )
    if expiring:
        with pytest.raises(ResearchError) as refused:
            credentials.resolve(issued.token, allow_expiring=False)
        assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    else:
        assert credentials.resolve(issued.token, allow_expiring=False) == principal
        credentials.revoke(principal, issued.id)
        with pytest.raises(ResearchError):
            credentials.resolve(issued.token, allow_expiring=False)


def test_default_expiring_credentials_keep_native_expiry_semantics(
    database, principal, monkeypatch
):
    credentials = CredentialStore(database)
    issued = credentials.issue(principal, principal, expires_at="2999-01-01T00:00:00.000Z")
    assert credentials.resolve(issued.token) == principal
    monkeypatch.setattr("research_swarm.control.auth.utc_now", lambda: "2999-01-01T00:00:00.000Z")
    with pytest.raises(ResearchError) as refused:
        credentials.resolve(issued.token)
    assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED


@pytest.mark.parametrize("mode", ["unknown", "scope", "additional_scope", "schema"])
def test_preflight_checks_shared_catalog_permissions_before_schema(context, mode):
    command = context()
    operation = "worker.enroll"
    if mode == "unknown":
        operation = "fixture.unknown"
    elif mode == "scope":
        command = command.model_copy(
            update={"principal": command.principal.model_copy(update={"scopes": frozenset()})}
        )
    elif mode == "additional_scope":
        operation = "population.retain"
        command = command.model_copy(
            update={"principal": command.principal.model_copy(update={"scopes": {"search.manage"}})}
        )
    with pytest.raises(ResearchError) as refused:
        validate_command_input(command, operation, {"name": "private-untrusted-fixture-input"})
    assert (
        refused.value.code
        == {
            "unknown": ErrorCode.CAPABILITY_UNAVAILABLE,
            "scope": ErrorCode.AUTHORIZATION_DENIED,
            "additional_scope": ErrorCode.AUTHORIZATION_DENIED,
            "schema": ErrorCode.PROJECT_CONTRACT_INVALID,
        }[mode]
    )
    assert "private-untrusted-fixture-input" not in str(refused.value.details)


@pytest.mark.parametrize("mode", ["local-utc-v1", "bounded-continuous-v1"])
def test_preflight_returns_the_shared_model_without_claiming_a_kernel_payload(context, mode):
    payload = {
        "worker_id": new_id(),
        "session_id": new_id(),
        "principal_id": new_id(),
        "name": "Public fixture",
        "profiles": [
            {
                "name": "cpu",
                "backend": "process",
                "cpu_slots": 1,
                "execution_fingerprint": "sha256:" + "b" * 64,
            }
        ],
        "clock_mode": mode,
    }
    definition, request = validate_command_input(context(), "worker.enroll", payload)
    assert definition is COMMANDS["worker.enroll"]
    assert isinstance(request, EnrollWorker) and request.clock_mode == mode
    assert request.model_dump(mode="json")["clock_mode"] == mode
    assert "schema_version" not in payload  # Input remains untouched.
