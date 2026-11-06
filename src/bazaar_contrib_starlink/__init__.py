"""A node's link to a Bazaar Compute server: it takes the server's calls, runs
them through the node's sidecar, hands back their answers and reports the
node's audit events, each over a request of its own with the computer's token."""

from __future__ import annotations

from contextlib import asynccontextmanager
lazy import asyncio
lazy import os
lazy from collections.abc import AsyncIterator

from bazaar_contrib_audit import AUDIT, AUDIT_EVENT, AuditEvent
from bazaar_contrib_sidecar import CLIENT
from bazaar_contrib_timer import TIMER
from bazaar_core import Context, Contrib, ServiceKey
from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field
lazy import aiohttp

from .reports import Reports
lazy from .calls import Calls
lazy from .link import Link, Rejected

__all__ = ["Config", "starlink"]


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    # the server's address, as its page for connecting a computer gives it
    server: AnyHttpUrl
    token: str | None = None
    token_env: str | None = Field(default=None, min_length=1)


REPORTS = ServiceKey[Reports]("starlink.reports")


@asynccontextmanager
async def lifespan(ctx: Context, config: Config) -> AsyncIterator[None]:
    token = config.token
    if token is None and config.token_env is not None:
        token = os.environ.get(config.token_env)
    if token is None or not token.strip():
        raise ValueError("starlink credential is missing: configure token or token_env")
    timer = ctx.accept(TIMER)
    async with aiohttp.ClientSession(
        headers={"Authorization": f"Bearer {token}"},
        # every wait is the timer's
        timeout=aiohttp.ClientTimeout(total=None),
    ) as session:
        link = Link(
            session,
            f"{str(config.server).rstrip('/')}/starlink/v1",
            timer,
            ctx.accept(AUDIT),
        )
        reports = Reports(link)
        ctx.offer(REPORTS, reports)
        try:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(
                    Calls(link, ctx.accept(CLIENT), timer, tasks).listen()
                )
                tasks.create_task(reports.send())
                try:
                    yield
                finally:
                    # the calls under way and the events not yet confirmed
                    # end with the node
                    tasks.cancel()
        except* Rejected as rejected:
            raise rejected.exceptions[0] from None


async def _queued(ctx: Context, event: AuditEvent) -> None:
    ctx.accept(REPORTS).put(event)


starlink = Contrib(
    config=Config,
    accepts=[TIMER, AUDIT, CLIENT],
    offers=[REPORTS],
    lifespan=lifespan,
    listen={AUDIT_EVENT: _queued},
)
