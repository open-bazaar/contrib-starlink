"""The node's audit events on their way to the server: queued as they come,
sent in order a batch at a time, and let go once the server confirms them."""

from __future__ import annotations

lazy import asyncio
lazy import json
lazy import logging
lazy from collections import deque

from bazaar_contrib_audit import AuditEvent
from pydantic import BaseModel, JsonValue, TypeAdapter

from .link import REQUEST_MS, Link

# half the server's request limit (1 MiB): a backlog goes over in requests of
# this size, and no more waits for the server than one of them carries
BATCH_BYTES = 512 * 1024

EVENT = TypeAdapter[AuditEvent](AuditEvent)
PAYLOAD = TypeAdapter[JsonValue](JsonValue)


class _Confirmed(BaseModel):
    run_id: str
    sequence: int


def _envelope(run_id: str) -> bytes:
    return b'{"run_id":' + json.dumps(run_id).encode() + b',"events":[]}'


class Reports:
    def __init__(self, link: Link) -> None:
        self._link = link
        # run, sequence and the event as it is sent, oldest first
        self._waiting: deque[tuple[str, int, bytes]] = deque()
        self._bytes = 0
        # events let go at the door since a report last got through
        self._dropped = 0
        self._ready = asyncio.Event()

    def put(self, event: AuditEvent) -> None:
        """Queue the event, unless the queue is full; an event that cannot fit a
        request goes with its payload left out."""
        envelope = len(_envelope(event.run_id))
        encoded = EVENT.dump_json(event)
        if envelope + len(encoded) > BATCH_BYTES:
            event = event.model_copy(
                update={"payload": {"omitted": len(PAYLOAD.dump_json(event.payload))}}
            )
            encoded = EVENT.dump_json(event)
        if (
            envelope + len(encoded) > BATCH_BYTES
            or self._bytes + len(encoded) > BATCH_BYTES
        ):
            self._dropped += 1
            return
        self._waiting.append((event.run_id, event.sequence, encoded))
        self._bytes += len(encoded)
        self._ready.set()

    async def send(self) -> None:
        while True:
            await self._ready.wait()
            run_id = self._waiting[0][0]
            envelope = _envelope(run_id)
            batch: list[bytes] = []
            size = len(envelope)
            # a report carries the events of one run, in order, a request's worth
            for waiting_run, _, encoded in self._waiting:
                more = len(encoded) + (1 if batch else 0)
                if waiting_run != run_id or size + more > BATCH_BYTES:
                    break
                batch.append(encoded)
                size += more
            answer = await self._link.send(
                "report",
                "POST",
                "report",
                REQUEST_MS,
                body=envelope[:-2] + b",".join(batch) + b"]}",
            )
            if answer is None:
                await self._link.back_off()
                continue
            confirmed = _Confirmed.model_validate_json(answer)
            while (
                self._waiting
                and self._waiting[0][0] == confirmed.run_id
                and self._waiting[0][1] <= confirmed.sequence
            ):
                self._bytes -= len(self._waiting.popleft()[2])
            if not self._waiting:
                self._ready.clear()
            if self._dropped:
                logging.getLogger(__name__).warning(
                    "%d audit events were dropped while the server did not take them",
                    self._dropped,
                )
                self._dropped = 0
