"""Integration tests"""

import asyncio
import os
import socket
import subprocess
import sys
import time

import pytest

from sessionbus.client import Client
from sessionbus.connection import BusConnection
from sessionbus.server import Server
from sessionbus.types import Attachment, MessageType, Payload, new_attachment_id

# pylint: disable=missing-function-docstring

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakeOutboxBackend:
    """Simple dict-backed outbox backend for overflow tests."""

    def __init__(self):
        self._store: dict[tuple, list] = {}

    async def append_many(self, user_id: str, session_id: str, payloads: list) -> None:
        key = (user_id, session_id.strip())
        self._store.setdefault(key, []).extend(payloads)

    async def pop_all(self, user_id: str, session_id: str) -> list:
        return self._store.pop((user_id, session_id.strip()), [])


def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return True
        except (OSError, socket.error):
            time.sleep(0.1)
    return False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_client_server_binary_roundtrip():
    """Client sends preamble+binary; server assembles and echoes; client receives full payload."""
    server = Server(host="127.0.0.1", port=0)

    async def on_message(_ws, payload: Payload):
        await server.push(payload.user_id, payload.session_id, payload)

    server.on_message = on_message
    await server.start()
    url = f"ws://127.0.0.1:{server.port}"

    data = b"binary\x00attachment\xff"
    att = Attachment(
        data=data,
        filename="file.bin",
        mime="application/octet-stream",
        size=len(data),
        id=new_attachment_id(),
    )
    client = Client(url)
    await client.connect()
    await client.subscribe("user1", "s1")
    await client.send(content="hello", attachment=att, user_id="user1", session_id="s1")

    received = await client.recv("user1", "s1", timeout=2.0)
    await client.close()
    await server.stop()

    assert received.content == "hello"
    assert received.attachment.data == data
    assert received.attachment.id == att.id


@pytest.mark.asyncio
async def test_outbox_drains_on_reconnect():
    """Payloads pushed while client is offline are delivered in order on reconnect."""
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    url = f"ws://127.0.0.1:{server.port}"
    uid, sid = "user-outbox", "s1"

    client1 = Client(url)
    await client1.connect()
    await client1.subscribe(uid, sid)
    await asyncio.sleep(0.05)
    await client1.close()
    await asyncio.sleep(0.05)

    for i in range(2):
        await server.push(
            uid,
            sid,
            Payload(
                type=MessageType.SEND, user_id=uid, session_id=sid, content=f"msg-{i}"
            ),
        )
    assert len(server._outbox[(uid, sid)]) == 2  # pylint: disable=protected-access

    client2 = Client(url)
    await client2.connect()
    await client2.subscribe(uid, sid)

    received = [await client2.recv(uid, sid, timeout=2.0) for _ in range(2)]
    await client2.close()
    await server.stop()

    assert [r.content for r in received] == ["msg-0", "msg-1"]


@pytest.mark.asyncio
async def test_outbox_overflow_to_backend():
    """When in-memory outbox exceeds max_outbox_per_key, overflow goes to backend;
    All payloads are delivered in order.
    """
    backend = FakeOutboxBackend()
    server = Server(
        host="127.0.0.1", port=0, outbox_backend=backend, max_outbox_per_key=2
    )
    await server.start()
    url = f"ws://127.0.0.1:{server.port}"
    uid, sid = "user-overflow", "s1"

    for i in range(3):
        await server.push(
            uid,
            sid,
            Payload(
                type=MessageType.SEND, user_id=uid, session_id=sid, content=f"msg-{i}"
            ),
        )

    assert len(server._outbox[(uid, sid)]) == 2  # pylint: disable=protected-access

    client = Client(url)
    await client.connect()
    await client.subscribe(uid, sid)

    received = [await client.recv(uid, sid, timeout=2.0) for _ in range(3)]
    await client.close()
    await server.stop()

    assert [r.content for r in received] == ["msg-0", "msg-1", "msg-2"]


@pytest.mark.asyncio
async def test_one_connection_multi_key_routing():
    """One WS subscribed to two keys; each queue receives only its own payload."""
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    url = f"ws://127.0.0.1:{server.port}"

    conn = BusConnection(url)
    await conn.connect()
    q_a = await conn.get_queue("user-A", "s1")
    q_b = await conn.get_queue("user-B", "s1")
    await conn.subscribe("user-A", "s1")
    await conn.subscribe("user-B", "s1")
    await asyncio.sleep(0.05)

    await server.push(
        "user-A",
        "s1",
        Payload(
            type=MessageType.SEND, user_id="user-A", session_id="s1", content="for-A"
        ),
    )
    await server.push(
        "user-B",
        "s1",
        Payload(
            type=MessageType.SEND, user_id="user-B", session_id="s1", content="for-B"
        ),
    )

    p_a = await asyncio.wait_for(q_a.get(), timeout=2.0)
    p_b = await asyncio.wait_for(q_b.get(), timeout=2.0)
    await conn.close()
    await server.stop()

    assert p_a.content == "for-A" and p_a.user_id == "user-A"
    assert p_b.content == "for-B" and p_b.user_id == "user-B"


@pytest.mark.asyncio
async def test_two_connections_four_keys_routing():
    """Two WS connections, 2 keys each; server pushes to all 4;
    Each queue receives only its payload.
    """
    server = Server(host="127.0.0.1", port=0)
    await server.start()
    url = f"ws://127.0.0.1:{server.port}"

    conn1 = BusConnection(url)
    conn2 = BusConnection(url)
    await conn1.connect()
    await conn2.connect()

    q1_a = await conn1.get_queue("client-1", "s1")
    q1_b = await conn1.get_queue("client-2", "s1")
    q2_a = await conn2.get_queue("client-3", "s1")
    q2_b = await conn2.get_queue("client-4", "s1")
    await conn1.subscribe("client-1", "s1")
    await conn1.subscribe("client-2", "s1")
    await conn2.subscribe("client-3", "s1")
    await conn2.subscribe("client-4", "s1")
    await asyncio.sleep(0.05)

    for i in range(1, 5):
        uid = f"client-{i}"
        await server.push(
            uid,
            "s1",
            Payload(
                type=MessageType.SEND,
                user_id=uid,
                session_id="s1",
                content=f"for-{uid}",
            ),
        )
    await asyncio.sleep(0.05)

    received = [
        await asyncio.wait_for(q.get(), timeout=2.0) for q in [q1_a, q1_b, q2_a, q2_b]
    ]
    await conn1.close()
    await conn2.close()
    await server.stop()

    for i, p in enumerate(received, start=1):
        assert p.user_id == f"client-{i}"
        assert p.content == f"for-client-{i}"


@pytest.mark.asyncio
async def test_server_subprocess_client_in_process():
    """Server in subprocess, client in this process;
    Send and receive echo across process boundary.
    """
    port = 18765
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.Popen(
        [
            sys.executable,
            os.path.join(root, "examples", "run_server.py"),
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--echo",
        ],
        cwd=root,
        env={**os.environ, "PYTHONPATH": os.path.join(root, "src")},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        if not _wait_for_port("127.0.0.1", port):
            proc.terminate()
            proc.wait(timeout=5)
            pytest.fail("server did not start in time")

        conn = BusConnection(f"ws://127.0.0.1:{port}")
        await conn.connect()
        q = await conn.get_queue("user1", "s1")
        await conn.subscribe("user1", "s1")
        await asyncio.sleep(0.05)

        await conn.send_payload(
            Payload(
                type=MessageType.SEND, user_id="user1", session_id="s1", content="hello"
            )
        )

        received = await asyncio.wait_for(q.get(), timeout=5.0)
        await conn.close()

        assert received.user_id == "user1"
        assert received.content == "hello"
    finally:
        proc.terminate()
        proc.wait(timeout=5)
