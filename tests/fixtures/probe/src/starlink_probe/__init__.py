"""What a node exposes to the server in starlink's tests: an echo, an answer
of any size, an answer that takes its time, and a member it keeps to itself."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy from collections.abc import AsyncIterator

from bazaar_contrib_sidecar import RemoteEntry
from bazaar_contrib_timer import TIMER, Timer
from bazaar_core import Context, Contrib, ServiceKey

# the slow answers being worked on, for a test to see them start and end
under_way: list[int] = []


class Probe:
    def __init__(self, timer: Timer) -> None:
        self._timer = timer

    def echo(self, text: str) -> str:
        return text

    def sized(self, size: int) -> str:
        return "x" * size

    async def slow(self, ms: int) -> str:
        under_way.append(ms)
        try:
            await self._timer.create(ms).wait()
        finally:
            under_way.remove(ms)
        return "done"

    def kept(self) -> str:
        return "kept"


PROBE = ServiceKey[Probe]("starlink.probe")


@asynccontextmanager
async def lifespan(ctx: Context) -> AsyncIterator[None]:
    ctx.offer(PROBE, Probe(ctx.accept(TIMER)))
    yield


probe = Contrib(accepts=[TIMER], offers=[PROBE], lifespan=lifespan)
remote = RemoteEntry(PROBE, ("echo", "sized", "slow"))
