"""Requests to the server over one session with the computer's token: each
bounded in time, the failures of each kind noted in the audit once while they
last, and one backoff for all of them, which a request getting through ends."""

from __future__ import annotations

lazy import asyncio
lazy from collections.abc import Mapping

from bazaar_contrib_audit import Audit, AuditContext, AuditError
from bazaar_contrib_timer import Timer, timeout
lazy import aiohttp

# a request that is not a listen waits as long as 0.2's commands did
REQUEST_MS = 10_000
# a failed request waits a second, twice as long after each failure in a
# row, never longer than 0.2's node took to start; any request that gets
# through starts over
FIRST_BACKOFF_MS = 1_000
LAST_BACKOFF_MS = 60_000

# the node's own events belong to no agent, as its health beat does not
CONTEXT = AuditContext(agent_id="")


class Rejected(RuntimeError):
    """The server does not take the token: it is wrong, or the computer is gone."""


class Link:
    def __init__(
        self, session: aiohttp.ClientSession, base: str, timer: Timer, audit: Audit
    ) -> None:
        self._session = session
        self._base = base
        self._timer = timer
        self._audit = audit
        self._backoff_ms = FIRST_BACKOFF_MS
        # the kinds of request failing now
        self._failing: set[str] = set()
        # set while the last request got through: the server is back, and
        # whatever waits out the backoff goes again at once
        self._through = asyncio.Event()

    async def send(
        self,
        request: str,
        method: str,
        path: str,
        limit_ms: int,
        *,
        params: Mapping[str, str] | None = None,
        body: bytes | None = None,
    ) -> bytes | None:
        """What the server answered, nothing when the request did not get
        through; Rejected when the server does not take the token."""
        try:
            async with (
                timeout(self._timer, limit_ms),
                self._session.request(
                    method,
                    f"{self._base}/{path}",
                    params=params,
                    data=body,
                    headers=None
                    if body is None
                    else {"Content-Type": "application/json"},
                ) as response,
            ):
                status = response.status
                answer = await response.read()
        except (aiohttp.ClientError, TimeoutError) as error:
            await self._failed(
                request, AuditError(kind=type(error).__name__, message=str(error))
            )
            return None
        if status == 401:
            raise Rejected(f"the server at {self._base} does not take the token")
        if not 200 <= status < 300:
            await self._failed(request, AuditError(kind="http", message=str(status)))
            return None
        self._backoff_ms = FIRST_BACKOFF_MS
        self._through.set()
        if request in self._failing:
            self._failing.discard(request)
            await self._audit.publish(
                "starlink.recovered", context=CONTEXT, payload={"request": request}
            )
        return answer

    async def _failed(self, request: str, error: AuditError) -> None:
        self._through.clear()
        if request not in self._failing:
            self._failing.add(request)
            await self._audit.publish(
                "starlink.failed",
                context=CONTEXT,
                payload={"request": request, "server": self._base},
                error=error,
            )

    async def back_off(self) -> None:
        """Wait out the backoff, or until another request gets through, and
        lengthen it for the next failure."""
        delay = self._backoff_ms
        self._backoff_ms = min(delay * 2, LAST_BACKOFF_MS)
        try:
            async with timeout(self._timer, delay):
                await self._through.wait()
        except TimeoutError:
            pass
