"""A server with its computers as installed, and a node whose starlink links
to it: the node exposes the probe's members, and a module on the server calls
them as a page would and keeps what the computers report."""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
lazy import asyncio
lazy import json
lazy import logging
lazy import re
lazy import socket
lazy import sys
lazy from collections.abc import AsyncIterator, Callable
lazy from typing import Any

import httpx
from bazaar_contrib_audit import AUDIT, AUDIT_EVENT, Audit, AuditEvent
from bazaar_contrib_http import HTTP
from bazaar_contrib_server import SERVER
from bazaar_contrib_server_computer import COMPUTERS, REPORTED, Reported
from bazaar_contrib_sidecar import RemoteError
from bazaar_core import (
    Context,
    Contrib,
    ContribEntry,
    ContribHub,
    ContribSpec,
    ContribState,
    load,
)
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

SERVER_ENTRIES = (
    ("timer-wheel", {}),
    ("server-database", {}),
    ("server-migrations", {}),
    ("server", {}),
    ("server-theme-night", {}),
    ("server-theme-day", {}),
    ("server-access", {}),
    ("server-overview", {}),
    ("server-relay-memory", {}),
    ("server-computer", {}),
)


def free_port() -> int:
    """A port nothing listens on now, for a server to come up on later."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@dataclass
class Served:
    url: str
    password: str
    # what the computers reported, as it was announced
    reported: list[Reported] = field(default_factory=list)


@asynccontextmanager
async def serving(port: int = 0) -> AsyncIterator[Served]:
    found: dict[str, Any] = {}
    printed: list[str] = []

    class Printed(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            printed.append(record.getMessage())

    @asynccontextmanager
    async def look(ctx: Context) -> AsyncIterator[None]:
        found["url"] = ctx.accept(HTTP).url
        yield

    @asynccontextmanager
    async def probe(ctx: Context) -> AsyncIterator[None]:
        """Calls a computer what a page would: POST /ui/probe/<ref>/<method>
        with the arguments as a JSON list."""
        server = ctx.accept(SERVER)
        computers = ctx.accept(COMPUTERS)

        async def ask(request: Request) -> Response:
            ids = await server.refs.values([request.path_params["ref"]])
            assert ids is not None
            try:
                value = await computers.call(
                    ids[0],
                    "probe",
                    request.path_params["method"],
                    *json.loads(await request.body()),
                )
            except TimeoutError:
                return JSONResponse({"silent": True}, status_code=504)
            except RemoteError as error:
                return JSONResponse({"failed": error.type}, status_code=502)
            return JSONResponse({"value": value})

        async with server.module(
            "probe",
            ui=Starlette(routes=[Route("/{ref}/{method}", ask, methods=["POST"])]),
        ):
            yield

    reported: list[Reported] = []

    async def heard(ctx: Context, value: Reported) -> None:
        del ctx
        reported.append(value)

    composition = await asyncio.to_thread(
        load,
        [
            ContribSpec(contrib="http", config={"listen": f"127.0.0.1:{port}"}),
            *(
                ContribSpec(contrib=name, config=config)
                for name, config in SERVER_ENTRIES
            ),
        ],
    )
    composition = composition.model_copy(
        update={
            "entries": (
                *composition.entries,
                ContribEntry(
                    name="probe",
                    definition=Contrib(
                        accepts=[SERVER, COMPUTERS],
                        lifespan=probe,
                        listen={REPORTED: heard},
                    ),
                ),
                ContribEntry(
                    name="look", definition=Contrib(accepts=[HTTP], lifespan=look)
                ),
            )
        }
    )
    logger = logging.getLogger("bazaar_contrib_server_access")
    handler = Printed()
    logger.addHandler(handler)
    try:
        async with ContribHub() as hub, hub.enable(composition):
            password = next(
                (
                    created.group(1)
                    for message in printed
                    if (created := re.search(r"with password (\S+);", message))
                ),
                "",
            )
            yield Served(found["url"], password, reported)
    finally:
        logger.removeHandler(handler)


@asynccontextmanager
async def client() -> AsyncIterator[httpx.AsyncClient]:
    # building the client loads certificates from disk, which is not the loop's work
    async with await asyncio.to_thread(httpx.AsyncClient, timeout=90) as session:
        yield session


async def admin(served: Served, session: httpx.AsyncClient) -> None:
    await session.post(
        f"{served.url}/login/", data={"name": "admin", "password": served.password}
    )


async def enrol(
    served: Served, session: httpx.AsyncClient, name: str
) -> tuple[str, str]:
    """A computer the account connects; its ref and its token, as the page
    showing the commands gives them."""
    page = await session.post(f"{served.url}/ui/computers/", data={"name": name})
    assert page.status_code == 200, (page.status_code, page.text[:3000])
    token = re.search(r"--option token=([^\"&\s]+)\"", page.text)
    ref = re.search(r'hx-get="/ui/computers/(\d+)"', page.text)
    assert token is not None and ref is not None
    return ref.group(1), token.group(1)


async def ask(
    served: Served, session: httpx.AsyncClient, ref: str, method: str, *args: object
) -> httpx.Response:
    """Call the probe's member on the computer through the server."""
    return await session.post(
        f"{served.url}/ui/probe/{ref}/{method}", content=json.dumps(args)
    )


@dataclass
class Node:
    hub: ContribHub
    audit: Audit
    # what the node's audit published, as it was announced
    heard: list[AuditEvent]

    def state(self) -> ContribState[str]:
        return next(
            status.state for status in self.hub.snapshots if status.name == "starlink"
        )

    def failing(self, request: str) -> bool:
        """Whether starlink noted the request failing."""
        return any(
            event.event_name == "starlink.failed"
            and isinstance(event.payload, dict)
            and event.payload.get("request") == request
            for event in self.heard
        )


@asynccontextmanager
async def node(*links: dict[str, object]) -> AsyncIterator[Node]:
    """A node with the probe's members and a starlink entry set as given for
    each link."""
    found: dict[str, Audit] = {}
    heard: list[AuditEvent] = []

    @asynccontextmanager
    async def speak(ctx: Context) -> AsyncIterator[None]:
        found["audit"] = ctx.accept(AUDIT)
        yield

    async def hear(ctx: Context, event: AuditEvent) -> None:
        del ctx
        heard.append(event)

    composition = await asyncio.to_thread(
        load,
        [
            ContribSpec(contrib="timer-wheel"),
            ContribSpec(contrib="audit"),
            ContribSpec(
                contrib="sidecar-windows" if sys.platform == "win32" else "sidecar-unix"
            ),
            ContribSpec(contrib="sidecar"),
            ContribSpec(contrib="probe"),
            *(
                ContribSpec(contrib="starlink", config=link, location=f"links[{index}]")
                for index, link in enumerate(links)
            ),
        ],
    )
    composition = composition.model_copy(
        update={
            "entries": (
                *composition.entries,
                ContribEntry(
                    name="speak",
                    definition=Contrib(
                        accepts=[AUDIT], lifespan=speak, listen={AUDIT_EVENT: hear}
                    ),
                ),
            )
        }
    )
    async with ContribHub() as hub, hub.enable(composition):
        yield Node(hub, found["audit"], heard)


async def until(check: Callable[[], bool], seconds: float = 90) -> None:
    """Wait for the check to hold, looking again every tenth of a second."""
    for _ in range(int(seconds * 10)):
        if check():
            return
        await asyncio.sleep(0.1)
    raise TimeoutError(f"not so after {seconds} seconds")
