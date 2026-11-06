"""The server's calls for this computer: taken by long polls, each run through
the node's sidecar the way bcc's calls are, and its answer handed back."""

from __future__ import annotations

lazy import asyncio

from bazaar_contrib_sidecar import Client
from bazaar_contrib_sidecar.wire import Failure, Request
from bazaar_contrib_timer import Timer, wait_for
from pydantic import BaseModel

from .link import REQUEST_MS, Link

# the server holds a listen up to 25 seconds; the node waits 5 more for it
HOLD_MS = 25_000
GRACE_MS = 5_000
# how long the server waits for an answer: a call is answered within it
ANSWER_MS = 60_000
# what one request may carry, as the server takes it
MAX_BODY_BYTES = 1024 * 1024


class _Call(BaseModel):
    number: int
    request: Request


class _Listened(BaseModel):
    calls: list[_Call]


class Calls:
    def __init__(
        self, link: Link, client: Client, timer: Timer, tasks: asyncio.TaskGroup
    ) -> None:
        self._link = link
        self._client = client
        self._timer = timer
        self._tasks = tasks
        # the last call taken: the server hands over those after it
        self._after = 0

    async def listen(self) -> None:
        while True:
            answer = await self._link.send(
                "listen",
                "GET",
                "listen",
                HOLD_MS + GRACE_MS,
                params={"after": str(self._after)},
            )
            if answer is None:
                await self._link.back_off()
                continue
            for call in _Listened.model_validate_json(answer).calls:
                self._after = max(self._after, call.number)
                self._tasks.create_task(self._answer(call))

    async def _answer(self, call: _Call) -> None:
        taken_ms = self._timer.now_ms
        try:
            response = await wait_for(
                self._timer, self._client.send(call.request), ANSWER_MS
            )
        except TimeoutError:
            response = Failure(
                type="TimeoutError",
                message=f"the node did not answer within {ANSWER_MS // 1000} seconds",
            )
        except OSError as error:
            # the node's sidecar could not be reached
            response = Failure(type=type(error).__name__, message=str(error))
        body = response.model_dump_json().encode()
        if len(body) > MAX_BODY_BYTES:
            body = (
                Failure(
                    type="ResultTooLarge",
                    message=f"the answer is {len(body)} bytes, more than the server takes",
                )
                .model_dump_json()
                .encode()
            )
        # the server waits no longer than this for the answer
        while self._timer.now_ms - taken_ms < ANSWER_MS:
            announced = await self._link.send(
                "announce", "PUT", f"announce/{call.number}", REQUEST_MS, body=body
            )
            if announced is not None:
                return
            await self._link.back_off()
