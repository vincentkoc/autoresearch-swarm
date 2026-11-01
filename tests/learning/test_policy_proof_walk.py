"""Repeated policy generations share proofs only inside one immutable read walk."""

import pytest

from research_swarm.control import policy_adoption as module
from research_swarm.domain.errors import ErrorCode, ResearchError


def test_shared_ancestors_are_verified_once_and_fresh_operations_recheck(monkeypatch):
    transaction = object()
    verified = []
    result = object()

    def verify(tx, identifier):
        verified.append(identifier)
        number = int(identifier)
        if number:
            for _ in range(10):
                assert module.require_current_adoption(tx, str(number - 1)) is result
        return result

    monkeypatch.setattr(module, "_verify_current_adoption", verify)
    assert module.require_current_adoption(transaction, "5") is result
    assert verified == ["5", "4", "3", "2", "1", "0"]
    module.require_current_adoption(transaction, "5")
    assert verified == ["5", "4", "3", "2", "1", "0"] * 2


def test_nested_transactions_do_not_share_ancestor_proofs(monkeypatch):
    outer, inner = object(), object()
    calls = []
    result = object()

    def verify(tx, identifier):
        calls.append((tx, identifier))
        if tx is outer and identifier == "root":
            module.require_current_adoption(tx, "ancestor")
            module.require_current_adoption(inner, "ancestor")
            module.require_current_adoption(tx, "ancestor")
        return result

    monkeypatch.setattr(module, "_verify_current_adoption", verify)
    module.require_current_adoption(outer, "root")
    assert calls == [(outer, "root"), (outer, "ancestor"), (inner, "ancestor")]


def test_failed_verification_and_cycles_never_leave_a_cached_success(monkeypatch):
    transaction = object()
    failures = []
    result = object()

    def verify(tx, identifier):
        if identifier == "root":
            for _ in range(2):
                with pytest.raises(ResearchError, match="cycle"):
                    module.require_current_adoption(tx, "cycle")
        elif identifier == "cycle":
            failures.append(identifier)
            return module.require_current_adoption(tx, "cycle")
        return result

    monkeypatch.setattr(module, "_verify_current_adoption", verify)
    assert module.require_current_adoption(transaction, "root") is result
    assert failures == ["cycle", "cycle"]
    assert module._proof_walk.get() is None


def test_depth_bound_returns_a_domain_refusal_and_clears_walk(monkeypatch):
    visited = []

    def verify(tx, identifier):
        visited.append(identifier)
        return module.require_current_adoption(tx, str(int(identifier) + 1))

    monkeypatch.setattr(module, "_verify_current_adoption", verify)
    with pytest.raises(ResearchError) as refused:
        module.require_current_adoption(object(), "0")
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert len(visited) == module.MAX_ADOPTION_DEPTH
    assert module._proof_walk.get() is None


def test_exact_maximum_depth_passes_but_one_more_ancestor_refuses(monkeypatch):
    result = object()

    def verify(tx, identifier):
        remaining = int(identifier)
        return module.require_current_adoption(tx, str(remaining - 1)) if remaining else result

    monkeypatch.setattr(module, "_verify_current_adoption", verify)
    assert module.require_current_adoption(object(), str(module.MAX_ADOPTION_DEPTH - 1)) is result
    with pytest.raises(ResearchError) as refused:
        module.require_current_adoption(object(), str(module.MAX_ADOPTION_DEPTH))
    assert refused.value.code == ErrorCode.CAPABILITY_UNAVAILABLE
    assert module._proof_walk.get() is None
