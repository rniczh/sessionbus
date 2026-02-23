"""
Connection layer for the sessionbus.

Provides:
- ConnectionManager (server-side): manages incoming WebSockets and subscriptions
- BusConnection (client-side): holds one (or few) WebSocket(s) to the bus URL,
  dispatches received payloads by (user_id, session_id) to per-session queues
  so multiple Clients can share the same connection.

Attachment protocol:
  Preamble: JSON with attachment { id, name, mime, size, chunks? }
  Binary frame:
    [id:36][chunk_index:4][total_chunks:4][chunk data] for multiple chunks.
    [id:36][raw data] for a single chunk.
  Receiver matches binary to preamble by id
"""

import asyncio
import json
import logging
import ssl
import threading
from collections import defaultdict
from typing import Any, Awaitable, Callable, Dict, Optional, Set, Tuple

import websockets

from sessionbus.types import (
    ATTACHMENT_CHUNK_HEADER_BYTES,
    ATTACHMENT_CHUNK_SIZE,
    ATTACHMENT_ID_BYTES,
    Attachment,
    MessageType,
    Payload,
    SessionId,
    UserId,
    decode_chunk_frame,
    encode_chunk_frame,
    new_attachment_id,
    split_into_chunks,
)

logger = logging.getLogger(__name__)

# Subscription key: (user_id, session_id).
Key = Tuple[UserId, SessionId]

# Generic WebSocket type.
WS = Any


class ConnectionManager:
    """
    Server-side connection manager.

    Each WS calls subscribe(user_id, session_id) to subscribe to a key,
    and ConnectionManager records key -> set(ws).
    Use get_connections(user_id, session_id) to get all WebSockets for a key;
    push to all of them (broadcast).
    """

    def __init__(self) -> None:
        self._connections: Dict[Key, Set[WS]] = defaultdict(set)
        self._ws_to_keys: Dict[WS, Set[Key]] = defaultdict(set)
        self._lock = threading.Lock()

    def add(self, key: Key, ws: WS) -> None:
        """Register that this WebSocket is subscribed to this (user_id, session_id)."""
        with self._lock:
            self._connections[key].add(ws)
            self._ws_to_keys[ws].add(key)

    def remove(self, key: Key, ws: WS) -> None:
        """Unregister this WebSocket from this key only."""
        with self._lock:
            self._connections[key].discard(ws)
            if not self._connections[key]:
                self._connections.pop(key, None)
            self._ws_to_keys[ws].discard(key)
            if not self._ws_to_keys[ws]:
                self._ws_to_keys.pop(ws, None)

    def remove_ws(self, ws: WS) -> None:
        """Remove this WebSocket from all keys (call on disconnect)."""
        with self._lock:
            keys = self._ws_to_keys.pop(ws, set())
            for key in keys:
                self._connections[key].discard(ws)
                if not self._connections[key]:
                    self._connections.pop(key, None)

    def remove_key(self, user_id: UserId, session_id: str = "") -> None:
        """Remove this key entirely; all WS subscribed to it are unsubscribed from this key."""
        key = (user_id, session_id.strip())
        with self._lock:
            ws_set = self._connections.pop(key, set())
            for ws in ws_set:
                self._ws_to_keys[ws].discard(key)
                if not self._ws_to_keys[ws]:
                    self._ws_to_keys.pop(ws, None)

    def get_connections(
        self,
        user_id: UserId,
        session_id: str = "",
    ) -> Set[WS]:
        """Return all WebSockets subscribed to this (user_id, session_id) (broadcast)."""
        key = (user_id, session_id.strip())
        with self._lock:
            ws_set = self._connections.get(key, set())
            return ws_set.copy() if ws_set else set()


class BusConnection:
    """
    Client-side shared connection to a bus server.

    Maintains one WebSocket to the given URL. Multiple logical sessions
    Clients can use this connection for multiple keys (user_id, session_id)
    and gets a dedicated queue for incoming payloads. Outgoing messages are sent
    over the same WebSocket.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self._ws: Optional[WS] = None
        self._receive_task: Optional[asyncio.Task] = None
        self._closed = False
        # key -> queue of Payloads for that key
        self._queues: Dict[Key, asyncio.Queue[Payload]] = {}
        self._queues_lock = asyncio.Lock()
        # Non-chunked: preamble buffered by att_id until binary frame arrives
        self._pending_preambles_by_id: Dict[str, Payload] = {}
        # Chunked: att_id -> {"preamble": Payload, "received": {chunk_index: bytes}, "total": int}
        self._pending_chunks: Dict[str, dict] = {}

    async def connect(
        self,
        *,
        retry_interval: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
        after_connect: Optional[Callable[[Any], Awaitable[None]]] = None,
    ) -> "BusConnection":
        """Connect to the bus server; retries until success or close().
        If after_connect is given, it is awaited after the socket is open and before the receive
        loop starts. It is useful for auth. handshake.
        """
        while not self._closed:
            try:
                # Only pass ssl for wss:// or when caller provides ssl_context
                use_ssl = (
                    ssl_context
                    if ssl_context is not None
                    else (True if self.url.startswith("wss") else None)
                )
                kwargs = {"ping_interval": 20, "ping_timeout": 20}
                if use_ssl is not None:
                    kwargs["ssl"] = use_ssl
                self._ws = await websockets.connect(self.url, **kwargs)
                logger.info("BusConnection connected to %s", self.url)
                if after_connect is not None:
                    await after_connect(self._ws)
                self._receive_task = asyncio.create_task(self._receive_loop())
                return self
            except (OSError, ConnectionError) as e:
                logger.warning(
                    "BusConnection connect failed (will retry in %.1fs): %s",
                    retry_interval,
                    e,
                )
                await asyncio.sleep(retry_interval)
        return self

    async def _receive_loop(self) -> None:
        """Read from WebSocket and dispatch payloads to per-session queues.

        Binary frame routing:
        - Look up the preamble by att_id (first 36 bytes).
        - If preamble.attachment.chunks == 1: non-chunked path.
        - If preamble.attachment.chunks > 1: buffer chunk; dispatch when all arrived.
        """
        try:
            while self._ws and not self._closed:
                raw = await self._ws.recv()

                if isinstance(raw, bytes):
                    if len(raw) < ATTACHMENT_ID_BYTES:
                        logger.warning(
                            "Bus protocol: binary frame shorter than id prefix (%d), skipping",
                            ATTACHMENT_ID_BYTES,
                        )
                        continue

                    att_id = (
                        raw[:ATTACHMENT_ID_BYTES]
                        .decode("utf-8", errors="replace")
                        .strip("\x00 ")
                    )
                    preamble = self._pending_preambles_by_id.get(att_id)
                    if preamble is None:
                        logger.warning(
                            "Bus protocol: binary for unknown attachment id %r, dropping",
                            att_id[:16] if len(att_id) > 16 else att_id,
                        )
                        continue

                    is_chunked = (
                        preamble.attachment is not None
                        and preamble.attachment.chunks > 1
                    )

                    if is_chunked:
                        if len(raw) < ATTACHMENT_CHUNK_HEADER_BYTES:
                            logger.warning(
                                "Bus protocol: chunked frame too short (%d bytes), dropping",
                                len(raw),
                            )
                            continue
                        _, chunk_index, total_chunks, chunk_data = decode_chunk_frame(
                            raw
                        )
                        state = self._pending_chunks.setdefault(
                            att_id,
                            {
                                "preamble": preamble,
                                "received": {},
                                "total": total_chunks,
                            },
                        )
                        state["received"][chunk_index] = chunk_data
                        if len(state["received"]) < state["total"]:
                            continue
                        # All chunks received and assembled
                        self._pending_preambles_by_id.pop(att_id, None)
                        self._pending_chunks.pop(att_id, None)
                        data = b"".join(
                            state["received"][i] for i in range(state["total"])
                        )
                    else:
                        self._pending_preambles_by_id.pop(att_id, None)
                        data = raw[ATTACHMENT_ID_BYTES:]
                        if (
                            preamble.attachment is None
                            or len(data) != preamble.attachment.size
                        ):
                            logger.warning(
                                "Bus protocol: attachment size mismatch (expected %d, got %d), "
                                "dropping",
                                preamble.attachment.size if preamble.attachment else 0,
                                len(data),
                            )
                            continue

                    full = Payload(
                        type=preamble.type,
                        user_id=preamble.user_id,
                        session_id=preamble.session_id,
                        content=preamble.content,
                        error=preamble.error,
                        action=preamble.action,
                        attachment=Attachment(
                            data=data,
                            filename=preamble.attachment.filename,
                            mime=preamble.attachment.mime,
                            size=len(data),
                            id=preamble.attachment.id,
                        ),
                    )
                    await self._dispatch(full)
                    continue

                try:
                    payload = Payload.deserialize(raw)
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.warning("Bus protocol: invalid JSON preamble %s", e)
                    continue

                if payload.attachment and payload.attachment.size > 0:
                    att_id = payload.attachment.id
                    if not att_id or not isinstance(att_id, str):
                        logger.warning(
                            "Bus protocol: preamble attachment missing required id, dropping"
                        )
                        continue
                    self._pending_preambles_by_id[att_id] = payload
                    continue

                if payload.type in (
                    MessageType.SEND,
                    MessageType.ERROR,
                    MessageType.CONNECTION_LOST,
                ):
                    await self._dispatch(payload)

        except asyncio.CancelledError:
            pass
        except Exception as e:  # pylint: disable=broad-exception-caught
            if not self._closed:
                logger.warning("BusConnection receive_loop: %s", e)
                self._closed = True
                self._ws = None
                # Put error into all known queues so waiters can wake
                async with self._queues_lock:
                    err = Payload(
                        type=MessageType.ERROR,
                        user_id=UserId(""),
                        session_id=SessionId(""),
                        error="Connection lost",
                    )
                    for q in self._queues.values():
                        try:
                            q.put_nowait(err)
                        except asyncio.QueueFull:
                            pass

    async def _dispatch(self, payload: Payload) -> None:
        """Send payload to the queue for (user_id, session_id)."""
        key = (payload.user_id, payload.session_id)
        async with self._queues_lock:
            q = self._queues.get(key)
        if q is not None:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                logger.warning("Queue full for %s, dropping payload", key)

    async def get_queue(
        self, user_id: UserId, session_id: str = ""
    ) -> asyncio.Queue[Payload]:
        """Get or create the queue for this (user_id, session_id)."""
        key = (user_id, session_id.strip())
        async with self._queues_lock:
            if key not in self._queues:
                self._queues[key] = asyncio.Queue()
            return self._queues[key]

    async def subscribe(self, user_id: UserId, session_id: str = "") -> None:
        """Send subscribe for this (user_id, session_id) over the shared connection."""
        if not self._ws or self._closed:
            raise RuntimeError("BusConnection not connected")
        await self._ws.send(
            Payload(
                type=MessageType.SEND,
                action="subscribe",
                user_id=user_id,
                session_id=session_id or "",
            ).serialize()
        )

    async def unsubscribe(self, user_id: UserId, session_id: str = "") -> None:
        """Tell the server to stop pushing payloads for this key to this connection."""
        if not self._ws or self._closed:
            raise RuntimeError("BusConnection not connected")
        await self._ws.send(
            Payload(
                type=MessageType.SEND,
                action="unsubscribe",
                user_id=user_id,
                session_id=session_id or "",
            ).serialize()
        )

    async def send_payload(self, payload: Payload) -> None:
        """Send a payload (preamble JSON + binary frame(s)) over the shared connection.

        If the attachment exceeds ATTACHMENT_CHUNK_SIZE (256 KB), it is split into
        multiple binary frames, each carrying [id:36][chunk_index:4][total:4][data].
        The preamble includes `chunks=N` so the receiver knows to reassemble.
        Attachments that fit in one frame use the [id:36][data] format.
        """
        if not self._ws or self._closed:
            raise RuntimeError("BusConnection not connected")
        try:
            if payload.attachment and len(payload.attachment.data) > 0:
                att = payload.attachment
                if att.id is None:
                    att = Attachment(
                        data=att.data,
                        filename=att.filename,
                        mime=att.mime,
                        size=att.size,
                        id=new_attachment_id(),
                    )

                raw_data = att.data
                chunk_list = split_into_chunks(raw_data, ATTACHMENT_CHUNK_SIZE)
                total = len(chunk_list)

                if total > 1:
                    att = Attachment(
                        data=raw_data,
                        filename=att.filename,
                        mime=att.mime,
                        size=len(raw_data),
                        id=att.id,
                        chunks=total,
                    )

                payload = Payload(
                    type=payload.type,
                    user_id=payload.user_id,
                    session_id=payload.session_id,
                    content=payload.content,
                    error=payload.error,
                    action=payload.action,
                    attachment=att,
                )
                await self._ws.send(payload.serialize())

                aid = att.id
                if total == 1:
                    id_bytes = aid.encode("utf-8")[:ATTACHMENT_ID_BYTES].ljust(
                        ATTACHMENT_ID_BYTES
                    )[:ATTACHMENT_ID_BYTES]
                    await self._ws.send(id_bytes + raw_data)
                else:
                    for idx, chunk in enumerate(chunk_list):
                        await self._ws.send(encode_chunk_frame(aid, idx, total, chunk))
            else:
                await self._ws.send(payload.serialize())
        except Exception:  # pylint: disable=broad-exception-caught
            self._closed = True
            self._ws = None
            raise

    async def close(self) -> None:
        """Close the WebSocket and cancel the receive task."""
        self._closed = True
        if self._ws:
            try:
                await self._ws.close()
            except Exception:  # pylint: disable=broad-exception-caught
                pass
            self._ws = None
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
            self._receive_task = None

    def is_connected(self) -> bool:
        """Check if the connection is still open."""
        return not self._closed and self._ws is not None
