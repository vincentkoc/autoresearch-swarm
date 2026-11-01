"""Callback type preservation complements real workerd rollback qualification."""

import pytest

from adapters.cloudflare.platform.atomic import atomic
from research_swarm.domain.errors import ErrorCode, ResearchError


class Bridge:
    def transactionSync(self, callback):
        try:
            callback()
        except BaseException as error:
            raise RuntimeError("Synthetic foreign-runtime wrapper") from error


def test_original_domain_exception_and_return_value_survive_foreign_bridge():
    failure = ResearchError(ErrorCode.REVISION_CONFLICT, "Original typed rejection")

    def fail():
        raise failure

    with pytest.raises(ResearchError) as observed:
        atomic(Bridge(), fail)
    assert observed.value is failure
    value = object()
    assert atomic(Bridge(), lambda: value) is value


def test_commit_failure_is_not_reclassified_as_domain_failure():
    class BrokenCommit:
        def transactionSync(self, callback):
            callback()
            raise OSError("Synthetic commit failure")

    with pytest.raises(OSError, match="commit failure"):
        atomic(BrokenCommit(), lambda: 1)


def test_async_callback_and_runtime_reexecution_are_rejected():
    async def coroutine():
        return 1

    with pytest.raises(TypeError, match="synchronously"):
        atomic(Bridge(), coroutine)

    class Repeated:
        def transactionSync(self, callback):
            callback()
            callback()

    with pytest.raises(RuntimeError, match="more than once"):
        atomic(Repeated(), lambda: 1)
