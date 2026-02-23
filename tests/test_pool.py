"""Tests for sessionbus.pool (BusConnectionPool)."""

import asyncio
import json

import pytest
import websockets

from sessionbus.pool import BusConnectionPool
from sessionbus.types import MessageType, Payload

# pylint: disable=protected-access

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _idle_handler(ws):
    """Keep the connection open until the client or server closes it."""
    await ws.wait_closed()


# ---------------------------------------------------------------------------
# Unit: routing (no real network)
# ---------------------------------------------------------------------------


def test_slot_for_same_key_always_same_slot():
    """Same key always maps to the same slot (deterministic hash routing)."""
    pool = BusConnectionPool("ws://localhost:8765", pool_size=4)
    assert pool._slot_for(("u1", "s1")) == pool._slot_for(("u1", "s1"))


def test_slot_for_stays_in_range():
    """Slot index is always in [0, pool_size)."""
    pool = BusConnectionPool("ws://localhost:8765", pool_size=4)
    for i in range(50):
        slot = pool._slot_for((f"user{i}", f"session{i}"))
        assert 0 <= slot < pool.pool_size


# ---------------------------------------------------------------------------
# Integration: real WebSocket server
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pool_connect_all_slots_connected():
    """After connect(), is_connected() returns True (all slots up)."""
    async with websockets.serve(_idle_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(f"ws://127.0.0.1:{port}", pool_size=2)
        await pool.connect()
        assert pool.is_connected() is True
        assert pool.slot_count() == 2
        await pool.close()


@pytest.mark.asyncio
async def test_pool_subscribe_sends_frame_and_tracks_key():
    """subscribe() sends type=send/action=subscribe to the server and registers the key."""
    frames = []

    async def handler(ws):
        try:
            async for msg in ws:
                frames.append(msg)
        except Exception: # pylint: disable=broad-exception-caught
            pass

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(f"ws://127.0.0.1:{port}", pool_size=2)
        await pool.connect()
        await pool.subscribe("user1", "session1")
        await asyncio.sleep(0.05)

        key = ("user1", "session1")
        assert key in pool._key_to_slot

        await pool.close()

    subscribe_msgs = [
        json.loads(f)
        for f in frames
        if isinstance(f, str) and json.loads(f).get("action") == "subscribe"
    ]
    assert any(
        m["user_id"] == "user1" and m["session_id"] == "session1"
        for m in subscribe_msgs
    )


@pytest.mark.asyncio
async def test_pool_same_key_same_slot():
    """Subscribing the same key twice always routes to the same slot."""
    async with websockets.serve(_idle_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(f"ws://127.0.0.1:{port}", pool_size=4)
        await pool.connect()

        await pool.subscribe("user1", "session1")
        slot_first = pool._key_to_slot[("user1", "session1")]

        await pool.subscribe("user1", "session1")
        slot_second = pool._key_to_slot[("user1", "session1")]

        assert slot_first == slot_second
        await pool.close()


@pytest.mark.asyncio
async def test_pool_unsubscribe_removes_key_tracking():
    """unsubscribe() cleans up _key_to_slot and _slot_to_keys."""
    async with websockets.serve(_idle_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(f"ws://127.0.0.1:{port}", pool_size=2)
        await pool.connect()

        await pool.subscribe("user1", "session1")
        key = ("user1", "session1")
        assert key in pool._key_to_slot

        await pool.unsubscribe("user1", "session1")
        assert key not in pool._key_to_slot
        for tracked in pool._slot_to_keys.values():
            assert key not in tracked

        await pool.close()


@pytest.mark.asyncio
async def test_pool_is_connected_for_specific_key():
    """is_connected(user_id, session_id) checks only the slot for that key."""
    async with websockets.serve(_idle_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(f"ws://127.0.0.1:{port}", pool_size=2)
        await pool.connect()
        await pool.subscribe("user1", "session1")
        assert pool.is_connected("user1", "session1") is True
        await pool.close()


@pytest.mark.asyncio
async def test_pool_receive_pushed_payload():
    """Pool queue receives a payload pushed by the server after subscribe."""

    async def handler(ws):
        await ws.recv()  # consume subscribe frame
        pushed = Payload(
            type=MessageType.SEND,
            user_id="user1",
            session_id="session1",
            content="hello from server",
        ).serialize()
        await ws.send(pushed)
        await ws.wait_closed()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(f"ws://127.0.0.1:{port}", pool_size=1)
        await pool.connect()
        await pool.subscribe("user1", "session1")

        q = await pool.get_queue("user1", "session1")
        payload = await asyncio.wait_for(q.get(), timeout=2.0)
        assert payload.content == "hello from server"
        assert payload.user_id == "user1"

        await pool.close()


@pytest.mark.asyncio
async def test_pool_after_connect_hook_called_per_slot():
    """after_connect hook is invoked once per pool slot on connect()."""
    called = []

    async def on_connect(ws):
        called.append(ws)

    async with websockets.serve(_idle_handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        pool = BusConnectionPool(
            f"ws://127.0.0.1:{port}", pool_size=3, after_connect=on_connect
        )
        await pool.connect()
        assert len(called) == 3
        await pool.close()
