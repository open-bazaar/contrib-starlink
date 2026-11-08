"""starlink between a real server's computers and a node exposing the probe's
members: calls and their answers, reports, the token, a server that goes
away, and a node that leaves."""

from __future__ import annotations

lazy import asyncio
lazy from contextlib import AsyncExitStack

import pytest
import starlink_probe
from bazaar_contrib_audit import AuditContext
from bazaar_core import Failed
from linked import Served, admin, ask, client, enrol, free_port, node, serving, until
from pydantic import JsonValue


def reported(served: Served, name: str) -> list[dict[str, JsonValue]]:
    """The payloads of the events of the name the computers reported."""
    return [
        report.event.payload
        for report in served.reported
        if report.event.event_name == name and isinstance(report.event.payload, dict)
    ]


def requests(served: Served, name: str) -> list[JsonValue]:
    """Which requests the starlink events of the name were about."""
    return [payload.get("request") for payload in reported(served, name)]


async def test_the_server_calls_what_the_node_exposes_and_nothing_else():
    async with serving() as served, client() as boss:
        await admin(served, boss)
        ref, token = await enrol(served, boss, "kana-vm")
        async with node(server=served.url, token=token):
            echoed = await ask(served, boss, ref, "echo", "hello")
            assert echoed.json() == {"value": "hello"}
            # a member the probe keeps to itself is not reached
            kept = await ask(served, boss, ref, "kept")
            assert kept.json() == {"failed": "LookupError"}
            # an answer larger than the server takes comes back as a failure
            sized = await ask(served, boss, ref, "sized", 2 * 1024 * 1024)
            assert sized.json() == {"failed": "ResultTooLarge"}


async def test_events_wait_while_the_server_is_away_and_arrive_once():
    port = free_port()
    async with client() as boss, serving(port) as served:
        await admin(served, boss)
        _, token = await enrol(served, boss, "kana-vm")
    # the node starts and speaks while the server is down
    async with node(server=served.url, token=token) as linked:
        for n in (1, 2, 3):
            await linked.audit.publish(
                "probe.spoke", context=AuditContext(agent_id="kana"), payload={"n": n}
            )
        # the server comes back once the node has found it away
        await until(lambda: linked.failing("listen"))
        async with serving(port) as back:
            try:
                await until(
                    lambda: (
                        len(reported(back, "probe.spoke")) >= 3
                        and "listen" in requests(back, "starlink.recovered")
                    )
                )
            except TimeoutError as error:
                raise AssertionError(
                    [report.event.event_name for report in back.reported]
                ) from error
            # each once, however often the node tried
            spoke = reported(back, "probe.spoke")
            assert len(spoke) == 3
            assert all({"n": n} in spoke for n in (1, 2, 3))
            # the node noted the server being away
            assert "listen" in requests(back, "starlink.failed")


async def test_an_event_too_large_for_a_report_arrives_without_its_payload():
    async with serving() as served, client() as boss:
        await admin(served, boss)
        _, token = await enrol(served, boss, "kana-vm")
        async with node(server=served.url, token=token) as linked:
            await linked.audit.publish(
                "probe.spoke",
                context=AuditContext(agent_id="kana"),
                payload={"text": "x" * 1024 * 1024},
            )
            await linked.audit.publish(
                "probe.spoke", context=AuditContext(agent_id="kana"), payload={"n": 2}
            )
            await until(lambda: len(reported(served, "probe.spoke")) >= 2)
            spoke = reported(served, "probe.spoke")
            # the large one comes with how much was left out, the one after it whole
            assert any(
                isinstance(size := payload.get("omitted"), int) and size > 1024 * 1024
                for payload in spoke
            )
            assert {"n": 2} in spoke


async def test_an_answer_waits_for_the_server_to_come_back():
    port = free_port()
    async with AsyncExitStack() as stack:
        boss = await stack.enter_async_context(client())
        async with serving(port) as served:
            await admin(served, boss)
            ref, token = await enrol(served, boss, "kana-vm")
            linked = await stack.enter_async_context(
                node(server=served.url, token=token)
            )
            asking = asyncio.create_task(ask(served, boss, ref, "slow", 3_000))
            await until(lambda: bool(starlink_probe.under_way))
            asking.cancel()
            await asyncio.gather(asking, return_exceptions=True)
        # the server is gone when the answer is ready, and back in time for it
        await until(lambda: linked.failing("announce"))
        async with serving(port) as back:
            await until(lambda: "announce" in requests(back, "starlink.recovered"))
            assert "announce" in requests(back, "starlink.failed")


async def test_the_token_comes_from_the_environment_unless_given(
    monkeypatch: pytest.MonkeyPatch,
):
    async with serving() as served, client() as boss:
        await admin(served, boss)
        ref, token = await enrol(served, boss, "kana-vm")
        monkeypatch.setenv("STARLINK_TOKEN", token)
        async with node(server=served.url, token_env="STARLINK_TOKEN"):
            answered = await ask(served, boss, ref, "echo", "from the environment")
            assert answered.json() == {"value": "from the environment"}
        monkeypatch.setenv("STARLINK_TOKEN", "not-the-token")
        async with node(server=served.url, token=token, token_env="STARLINK_TOKEN"):
            answered = await ask(served, boss, ref, "echo", "given")
            assert answered.json() == {"value": "given"}


async def test_a_token_the_server_does_not_take_stops_starlink():
    async with serving() as served, node(server=served.url, token="x:y") as linked:
        await until(lambda: isinstance(linked.state(), Failed))
        assert "token" in str(linked.state())


async def test_starlink_without_a_token_does_not_start():
    async with node(server=f"http://127.0.0.1:{free_port()}") as linked:
        await until(lambda: isinstance(linked.state(), Failed))
        assert "token" in str(linked.state())


async def test_leaving_ends_the_calls_under_way():
    async with serving() as served, client() as boss:
        await admin(served, boss)
        ref, token = await enrol(served, boss, "kana-vm")
        async with asyncio.timeout(30):
            async with node(server=served.url, token=token):
                asking = asyncio.create_task(ask(served, boss, ref, "slow", 600_000))
                await until(lambda: bool(starlink_probe.under_way))
            # the node is gone well before the call would have ended
            assert not starlink_probe.under_way
        asking.cancel()
        await asyncio.gather(asking, return_exceptions=True)
