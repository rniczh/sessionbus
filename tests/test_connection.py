"""Tests for sessionbus.connection (ConnectionManager and BusConnection)."""

import asyncio
import json
from unittest.mock import AsyncMock

import pytest
import websockets

from sessionbus.connection import BusConnection, ConnectionManager
from sessionbus.types import (
    ATTACHMENT_ID_BYTES,
    Attachment,
    MessageType,
    Payload,
    new_attachment_id,
)


# -----------------------------------------------------------------------------
# ConnectionManager (server-side)
# -----------------------------------------------------------------------------


def test_connection_manager_add_and_remove():
    """add/remove correctly track ws membership per key."""
    cm = ConnectionManager()
    ws1, ws2 = object(), object()
    cm.add(("u1", "s1"), ws1)
    cm.add(("u1", "s1"), ws2)
    assert cm.get_connections("u1", "s1") == {ws1, ws2}

    cm.remove(("u1", "s1"), ws1)
    assert cm.get_connections("u1", "s1") == {ws2}

    cm.remove(("u1", "s1"), ws2)
    assert cm.get_connections("u1", "s1") == set()


def test_connection_manager_remove_ws_clears_all_keys():
    """remove_ws(ws) removes that ws from every key it subscribed to."""
    cm = ConnectionManager()
    ws = object()
    cm.add(("u1", "s1"), ws)
    cm.add(("u2", "s2"), ws)
    cm.remove_ws(ws)
    assert cm.get_connections("u1", "s1") == set()
    assert cm.get_connections("u2", "s2") == set()


def test_connection_manager_remove_key():
    """remove_key clears all ws for that key."""
    cm = ConnectionManager()
    ws1, ws2 = object(), object()
    cm.add(("u1", "s1"), ws1)
    cm.add(("u1", "s1"), ws2)
    cm.remove_key("u1", "s1")
    assert cm.get_connections("u1", "s1") == set()


# -----------------------------------------------------------------------------
# BusConnection (client-side)
# -----------------------------------------------------------------------------

# pylint: disable=protected-access
@pytest.mark.asyncio
async def test_bus_connection_is_connected():
    """is_connected returns True if the connection is open."""
    conn = BusConnection("ws://localhost:8765")
    assert conn.is_connected() is False
    conn._ws = object()
    conn._closed = False
    assert conn.is_connected() is True
    conn._closed = True
    assert conn.is_connected() is False


@pytest.mark.asyncio
async def test_bus_connection_queue_identity():
    """get_queue returns the same queue object for the same key."""
    conn = BusConnection("ws://localhost:8765")
    q1 = await conn.get_queue("u1", "s1")
    q2 = await conn.get_queue("u1", "s1")
    assert q1 is q2


@pytest.mark.asyncio
async def test_bus_connection_dispatch():
    """_dispatch routes a payload to the correct per-key queue."""
    conn = BusConnection("ws://localhost:8765")
    q = await conn.get_queue("u1", "s1")
    p = Payload(type=MessageType.SEND, user_id="u1", session_id="s1", content="hi")
    await conn._dispatch(p)
    got = q.get_nowait()
    assert got.content == "hi"
    assert got.user_id == "u1"


@pytest.mark.asyncio
async def test_bus_connection_unsubscribe_json():
    """unsubscribe() sends JSON with action=unsubscribe."""
    conn = BusConnection("ws://localhost:8765")
    conn._ws = AsyncMock()
    conn._closed = False
    await conn.unsubscribe("user-x", "session-y")
    (arg,) = conn._ws.send.call_args[0]
    data = json.loads(arg)
    assert data["type"] == "send"
    assert data["action"] == "unsubscribe"
    assert data["user_id"] == "user-x"
    assert data["session_id"] == "session-y"


@pytest.mark.asyncio
async def test_bus_connection_receive_loop_dispatches():
    """_receive_loop dispatches incoming SEND payloads to the right queue."""
    conn = BusConnection("ws://localhost:8765")
    q = await conn.get_queue("u1", "s1")
    payload_json = Payload(
        type=MessageType.SEND, user_id="u1", session_id="s1", content="received"
    ).serialize()

    call_count = 0

    async def fake_recv():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return payload_json
        raise asyncio.CancelledError()

    conn._ws = AsyncMock()
    conn._ws.recv = fake_recv
    conn._closed = False

    task = asyncio.create_task(conn._receive_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert q.get_nowait().content == "received"


# -----------------------------------------------------------------------------
# Integration: real WebSocket
# -----------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_subscribe_sends_correct_frame():
    """connect + subscribe() really sends the expected JSON over the wire."""
    received = []

    async def handler(ws):
        received.append(json.loads(await ws.recv()))
        await ws.close()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = BusConnection(f"ws://127.0.0.1:{port}")
        await conn.connect()
        await conn.subscribe("user-abc", "session-xyz")
        await asyncio.sleep(0.05)
        await conn.close()

    assert received[0]["type"] == "send"
    assert received[0]["action"] == "subscribe"
    assert received[0]["user_id"] == "user-abc"
    assert received[0]["session_id"] == "session-xyz"


@pytest.mark.asyncio
async def test_send_attachment_sends_preamble_and_binary():
    """send_payload with attachment sends JSON preamble then id-prefixed binary frame."""
    frames = []

    async def handler(ws):
        while len(frames) < 2:
            try:
                frames.append(await ws.recv())
            except websockets.exceptions.ConnectionClosed:
                break
        await ws.close()

    async with websockets.serve(handler, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        conn = BusConnection(f"ws://127.0.0.1:{port}")
        await conn.connect()

        data = b"binary-attachment-content"
        aid = new_attachment_id()
        att = Attachment(
            data=data,
            filename="file.bin",
            mime="application/octet-stream",
            size=len(data),
            id=aid,
        )
        p = Payload(
            type=MessageType.SEND, user_id="u1", session_id="s1", content="", attachment=att
        )
        await conn.send_payload(p)
        await asyncio.sleep(0.05)
        await conn.close()

    preamble = json.loads(frames[0])
    assert preamble["attachment"]["id"] == aid
    assert preamble["attachment"]["name"] == "file.bin"
    assert preamble["attachment"]["mime"] == "application/octet-stream"
    assert preamble["attachment"]["size"] == len(data)

    binary = frames[1]
    assert isinstance(binary, bytes)
    assert len(binary) == ATTACHMENT_ID_BYTES + len(data)
    assert binary[ATTACHMENT_ID_BYTES:] == data
