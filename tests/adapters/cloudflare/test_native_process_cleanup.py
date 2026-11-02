"""Process-group cleanup contracts independent of native runtime opt-in."""

from types import SimpleNamespace

import pytest

from tests.adapters.cloudflare import test_cloudflare_native as native


class ExitedLeader:
    pid = 12345

    def __init__(self):
        self.polls = 0
        self.waits = 0

    def poll(self):
        self.polls += 1
        return 0

    def wait(self, *, timeout):
        assert timeout == 10
        self.waits += 1
        return 0


def bounded_clock(monkeypatch):
    ticks = []
    sleeps = []
    monkeypatch.setattr(native, "signal", SimpleNamespace(SIGTERM=15, SIGKILL=9))

    def monotonic():
        ticks.append(len(ticks))
        return ticks[-1]

    monkeypatch.setattr(native, "time", SimpleNamespace(monotonic=monotonic, sleep=sleeps.append))
    return ticks, sleeps


def test_probe_permission_error_waits_for_group_disappearance(monkeypatch):
    process = ExitedLeader()
    calls = []
    _, sleeps = bounded_clock(monkeypatch)

    def killpg(pid, action):
        assert pid == process.pid
        calls.append(action)
        if action == 0:
            if calls.count(0) == 1:
                raise PermissionError("Synthetic transient probe denial")
            raise ProcessLookupError("Synthetic group disappearance")

    monkeypatch.setattr(native, "os", SimpleNamespace(killpg=killpg))
    native.close_owned_group(process)

    assert calls == [native.signal.SIGTERM, 0, 0]
    assert process.polls == 3
    assert process.waits == 1
    assert sleeps == [0.02]


def test_persistent_probe_denial_refuses_reuse_even_after_leader_exit(monkeypatch):
    process = ExitedLeader()
    calls = []
    ticks, sleeps = bounded_clock(monkeypatch)

    def killpg(pid, action):
        assert pid == process.pid
        calls.append(action)
        if action == 0:
            raise PermissionError("Synthetic persistent probe denial")

    monkeypatch.setattr(native, "os", SimpleNamespace(killpg=killpg))
    with pytest.raises(AssertionError, match="refusing state reuse"):
        native.close_owned_group(process)

    assert [action for action in calls if action] == [
        native.signal.SIGTERM,
        native.signal.SIGKILL,
    ]
    assert process.waits == 0
    assert process.polls == 2 * calls.count(0)
    assert len(ticks) == 22
    assert sleeps == [0.02] * 18


@pytest.mark.parametrize("denied_signal", [15, 9])
def test_actual_signal_permission_error_propagates(monkeypatch, denied_signal):
    process = ExitedLeader()
    bounded_clock(monkeypatch)
    calls = []

    def killpg(pid, action):
        assert pid == process.pid
        calls.append(action)
        if action == denied_signal:
            raise PermissionError("Synthetic signal refusal")
        if action == 0:
            raise PermissionError("Synthetic probe denial")

    monkeypatch.setattr(native, "os", SimpleNamespace(killpg=killpg))
    with pytest.raises(PermissionError, match="signal refusal"):
        native.close_owned_group(process)

    assert calls[-1] == denied_signal
    assert process.waits == 0
