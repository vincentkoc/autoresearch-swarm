"""A bounded asynchronous caller; training ownership and model application stay separate."""

from __future__ import annotations

import asyncio
from types import TracebackType
from typing import Literal, Self

from research_swarm.reactive.calls import fence_call, finish_call, start_call
from research_swarm.reactive.contracts import DecisionProposal, DecisionRequest, proposed_controls
from research_swarm.reactive.journal import DecisionJournal, JournalConflict
from research_swarm.reactive.journal_records import DecisionCall
from research_swarm.reactive.policies import DecisionPolicy


class DecisionRunner:
    def __init__(self, journal: DecisionJournal, policy: DecisionPolicy) -> None:
        self.journal, self.policy = journal, policy
        self.contract = journal.inspect().contract
        if policy.digest != self.contract.policy_digest:
            raise JournalConflict("Decision policy differs from the admitted fingerprint")
        self._task: asyncio.Task[None] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._call: DecisionCall | None = None
        self._request: DecisionRequest | None = None
        self._error: BaseException | None = None
        self._closed = False

    def poll(self) -> Literal["started", "busy", "idle", "closed"]:
        """Start at most one persisted call. Never wait for inference or cancellation."""
        if self._error is not None:
            raise RuntimeError("Decision-call persistence failed") from self._error
        if self._closed:
            return "closed"
        if self._task is not None:
            if not self._task.done():
                return "busy"
            self._task.result()
            self._task = None
        state = self.journal.inspect()
        if state.status != "active":
            self.close()
            return "closed"
        if state.inflight_call_request_id is not None:
            return "busy"
        pending = self.journal.pending()
        if pending is None or pending.status != "pending" or pending.call is not None:
            return "idle"
        loop = asyncio.get_running_loop()
        call = start_call(self.journal, pending.request.id, self.policy.digest)
        deadline = loop.time() + call.timeout_ms / 1000
        self._call, self._request = call, pending.request
        self._task = loop.create_task(self._invoke(pending.request, call, deadline))
        self._timer = loop.call_at(deadline, self._expire, self._task, pending.request, call)
        timer = self._timer
        self._task.add_done_callback(lambda task: self._done(task, pending.request, call, timer))
        return "started"

    async def _invoke(self, request: DecisionRequest, call: DecisionCall, deadline: float) -> None:
        proposal = None
        failure: Literal["exception", "invalid_response", "cancelled"] | None = None
        try:
            output = await self.policy.decide(request, self.contract)
        except asyncio.CancelledError:
            failure = "cancelled"
        except Exception:
            failure = "exception"
        else:
            try:
                proposal = DecisionProposal.model_validate(output)
                proposed_controls(self.contract, request, proposal, step=request.requested_step)
            except (ValueError, TypeError):
                proposal, failure = None, "invalid_response"
        if asyncio.get_running_loop().time() >= deadline:
            fence_call(self.journal, request.id, call.id, "timeout")
        finish_call(self.journal, request.id, call.id, proposal=proposal, failure=failure)

    def _expire(
        self, task: asyncio.Task[None], request: DecisionRequest, call: DecisionCall
    ) -> None:
        if task.done():
            return
        try:
            fence_call(self.journal, request.id, call.id, "timeout")
        except BaseException as error:
            self._error = error
        finally:
            if not task.cancelling():
                task.cancel()

    def _done(
        self,
        task: asyncio.Task[None],
        request: DecisionRequest,
        call: DecisionCall,
        timer: asyncio.TimerHandle,
    ) -> None:
        timer.cancel()
        try:
            if task.cancelled():
                # Cancellation before the coroutine starts cannot enter its exception handler.
                finish_call(self.journal, request.id, call.id, failure="cancelled")
            else:
                task.result()
        except BaseException as error:
            self._error = error

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._timer is not None:
            self._timer.cancel()
        if self._task is not None and not self._task.done():
            assert self._call is not None and self._request is not None
            try:
                fence_call(self.journal, self._request.id, self._call.id, "cancelled")
            finally:
                if not self._task.cancelling():
                    self._task.cancel()

    async def run(self, *, poll_interval_ms: int = 10) -> None:
        """Serve one explicitly opened local session; does not claim training ownership."""
        if type(poll_interval_ms) is not int or not 1 <= poll_interval_ms <= 1000:
            raise ValueError("Invalid decision polling interval")
        try:
            while self.poll() != "closed":
                await asyncio.sleep(poll_interval_ms / 1000)
        finally:
            self.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()
