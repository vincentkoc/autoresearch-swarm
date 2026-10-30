"""Privileged approval freezes the checker and the supported training contract."""

import json
import shutil
from pathlib import Path

import pytest

from research_swarm.client import LocalTransport, ResearchClient
from research_swarm.control.dispatch import COMMANDS
from research_swarm.control.learning_training_models import RegisterPolicyTrainingProfile
from research_swarm.control.queries import RecordQuery
from research_swarm.domain.errors import ErrorCode, ResearchError
from research_swarm.domain.identity import Principal
from research_swarm.domain.types import new_id
from research_swarm.runtime import LocalRuntime
from research_swarm.state.database import Transaction


def training_project(client, tmp_path, *, change=None):
    source = tmp_path / "training-project"
    shutil.copytree(Path(__file__).resolve().parents[1] / "examples/cpu_policy_learning", source)
    if change:
        change(source)
    return client.register_project(source, idempotency_key="training-project")["result"]


def profile_intent(registered):
    return RegisterPolicyTrainingProfile(
        profile_id=new_id(), project_revision_id=registered["project_revision"]["id"]
    )


def register_profile(client, intent, key="approve-profile"):
    return client.command("learning.register_training_profile", intent, idempotency_key=key)[
        "result"
    ]


def scoped_client(runtime, scopes):
    principal = Principal(
        id=new_id(), workspace_id=runtime.identity.workspace_id, scopes=frozenset(scopes)
    )
    return ResearchClient(LocalTransport(runtime, lambda: principal))


def test_profile_approval_uses_exact_scopes_and_recovers_committed_reply(tmp_path, monkeypatch):
    runtime = LocalRuntime.open(tmp_path / "state")
    client = ResearchClient(LocalTransport(runtime))
    registered = training_project(client, tmp_path)
    intent = profile_intent(registered)
    definition = COMMANDS["learning.register_training_profile"]
    scopes = {definition.scope, *definition.additional_scopes}
    assert scopes == {"learning.profile.manage", "read"}
    for missing in scopes:
        with pytest.raises(ResearchError) as refused:
            register_profile(scoped_client(runtime, scopes - {missing}), intent, missing)
        assert refused.value.code == ErrorCode.AUTHORIZATION_DENIED
    execute = runtime.database.execute

    def lose_ack(context, operation, *args, **kwargs):
        result = execute(context, operation, *args, **kwargs)
        if operation == "learning.register_training_profile":
            raise OSError("lost profile reply")
        return result

    monkeypatch.setattr(runtime.database, "execute", lose_ack)
    approver = scoped_client(runtime, scopes)
    row = register_profile(approver, intent)
    monkeypatch.setattr(runtime.database, "execute", execute)
    assert row["data"]["configuration"]["learning_rate"] == 0.5
    assert (
        row["data"]["source_artifact"] == registered["project_revision"]["data"]["source_artifact"]
    )

    def unavailable(*args, **kwargs):
        raise OSError("source store unavailable")

    monkeypatch.setattr(runtime.artifacts.store, "fetch", unavailable)
    assert register_profile(approver, intent) == row
    assert register_profile(approver, intent, "fresh-replay") == row
    with pytest.raises(ResearchError) as collision:
        register_profile(
            approver, intent.model_copy(update={"checker_stage_id": "different"}), "changed"
        )
    assert collision.value.code == ErrorCode.IDEMPOTENCY_CONFLICT
    assert len(client.list(RecordQuery(kind="learning_profile"))["items"]) == 1


@pytest.mark.parametrize(
    ("before", "after"),
    [
        ('allow = ["train.py"]', 'allow = ["*.py"]'),
        ('["common.py", "verify.py", "training.json"]', '["common.py", "training.json"]'),
        ('argv = ["python", "verify.py"]', 'argv = ["python", "train.py"]'),
        ('argv = ["python", "verify.py"]', 'argv = ["python", "-m", "verify"]'),
        ('expected_members = ["frozen-development-batch"]', 'expected_members = ["other"]'),
        ('aggregation = "maximum_absolute_residual"', 'aggregation = "mean"'),
        ('required_artifacts = ["behavior", "trajectories"]', 'required_artifacts = ["behavior"]'),
    ],
)
def test_rejects_mutable_checker_and_incompatible_contract(tmp_path, before, after):
    runtime = LocalRuntime.open(tmp_path / "state")
    client = ResearchClient(LocalTransport(runtime))

    def change(source):
        path = source / "research.toml"
        content = path.read_text()
        assert before in content
        path.write_text(content.replace(before, after))

    registered = training_project(client, tmp_path, change=change)
    with pytest.raises(ResearchError) as refused:
        register_profile(client, profile_intent(registered))
    assert refused.value.code == ErrorCode.PROJECT_CONTRACT_INVALID
    assert not client.list(RecordQuery(kind="learning_profile"))["items"]


@pytest.mark.parametrize(
    "field,value", [("epochs", 2), ("learning_rate", True), ("maximum_update_error", 0.1)]
)
def test_rejects_unsupported_training_configuration(tmp_path, field, value):
    runtime = LocalRuntime.open(tmp_path / "state")
    client = ResearchClient(LocalTransport(runtime))

    def change(source):
        path = source / "training.json"
        payload = json.loads(path.read_bytes())
        payload[field] = value
        path.write_text(json.dumps(payload))

    registered = training_project(client, tmp_path, change=change)
    with pytest.raises(ResearchError) as refused:
        register_profile(client, profile_intent(registered))
    assert refused.value.code == ErrorCode.PROJECT_CONTRACT_INVALID


def test_registration_event_failure_rolls_back_profile_and_retention(tmp_path, monkeypatch):
    runtime = LocalRuntime.open(tmp_path / "state")
    client = ResearchClient(LocalTransport(runtime))
    intent = profile_intent(training_project(client, tmp_path))
    event = Transaction.event

    def fail(transaction, event_type, *args, **kwargs):
        if event_type == "learning.training_profile_registered":
            raise RuntimeError("profile event failed")
        return event(transaction, event_type, *args, **kwargs)

    monkeypatch.setattr(Transaction, "event", fail)
    with pytest.raises(RuntimeError, match="profile event failed"):
        register_profile(client, intent)
    assert not client.list(RecordQuery(kind="learning_profile"))["items"]
    count = runtime.database.store.read(
        lambda sql: sql.execute(
            "SELECT COUNT(*) FROM records WHERE kind='artifact_reference' "
            "AND json_extract(payload,'$.retention.owner_id')=?",
            (intent.profile_id,),
        ).one()[0]
    )
    assert count == 0
    monkeypatch.setattr(Transaction, "event", event)
    assert register_profile(client, intent)["id"] == intent.profile_id
