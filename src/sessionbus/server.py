"""
Bus server: WebSocket server for session bus.

Listens for client connections; clients send subscribe(user_id, session_id)
to register. Use push() to send payloads to all subscribers for a key (broadcast).
When no connection exists for a key, payloads are queued in an outbox and sent when
the client reconnects and subscribes.
"""

import asyncio
import json
import logging
import ssl
from collections import defaultdict
from typing import Callable, List, Optional, Set

from websockets.asyncio.server import ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from sessionbus.connection import ConnectionManager
from sessionbus.outbox_backend import OutboxBackend
from sessionbus.types import (
    ATTACHMENT_CHUNK_HEADER_BYTES,
    ATTACHMENT_CHUNK_SIZE,
    ATTACHMENT_ID_BYTES,
    Attachment,
    MessageType,
    Payload,
    UserId,
    decode_chunk_frame,
    encode_chunk_frame,
    split_into_chunks,
)

logger = logging.getLogger(__name__)

OnMessage = Callable[[ServerConnection, Payload], asyncio.Future]


# Key for outbox: (user_id, session_id)
_Key = tuple[str, str]


class Server:
    """
    WebSocket bus server.

    - Clients connect and send JSON:
      { "type": "send", "action": "subscribe", "user_id", "session_id" }.
    - Server registers (user_id, session_id) -> ws in ConnectionManager.
    - On disconnect, the ws is removed from all keys.
    - Use push(user_id, session_id, payload) to send to all subscribers for that key.
    - When no connection exists for a key, payloads go to the outbox for that key; when a client
      subscribes again, the server drains the outbox and sends those payloads to the subscriber(s).
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        *,
        on_message: Optional[OnMessage] = None,
        outbox_backend: Optional[OutboxBackend] = None,
        max_outbox_per_key: int = 100,
        ssl_context: Optional[ssl.SSLContext] = None,
    ) -> None:
        self.host = host
        self.port = port
        self.on_message = on_message
        self._ssl_context = ssl_context
        self._manager = ConnectionManager()
        self._server = None
        # Per-ws: pending non-chunked preamble (att_id -> Payload) indexed by ws id
        self._pending: dict[int, dict[str, Payload]] = {}
        # Per-attachment: chunked state
        #  {att_id: {"preamble": Payload, "received": {idx: bytes}, "total": int}}
        self._pending_chunks: dict[str, dict] = {}
        # Outbox: per (user_id, session_id), queue of Payloads (in memory)
        self._outbox: dict[_Key, List[Payload]] = defaultdict(list)
        self._outbox_backend = outbox_backend
        self._max_outbox_per_key = max(1, max_outbox_per_key)

    @property
    def manager(self) -> ConnectionManager:
        """ConnectionManager used by this server (for tests or custom routing)."""
        return self._manager

    async def _handle(self, ws: ServerConnection) -> None:
        ws_id = id(ws)
        try:
            async for message in ws:
                try:
                    if isinstance(message, str):
                        data = json.loads(message)
                        payload = Payload.model_validate(data)
                        if (
                            payload.type == MessageType.SEND
                            and payload.action == "subscribe"
                        ):
                            key = (
                                payload.user_id or "",
                                (payload.session_id or "").strip(),
                            )
                            self._manager.add(key, ws)
                            logger.debug("subscribe %s -> ws", key)
                            await self._drain_outbox(key)
                            continue
                        if (
                            payload.type == MessageType.SEND
                            and payload.action == "unsubscribe"
                        ):
                            key = (
                                payload.user_id or "",
                                (payload.session_id or "").strip(),
                            )
                            self._manager.remove(key, ws)
                            logger.debug("unsubscribe %s -> ws", key)
                            continue
                        if (
                            payload.attachment
                            and payload.attachment.size
                            and payload.attachment.id
                        ):
                            # Buffer preamble, keyed by att_id per ws
                            self._pending.setdefault(ws_id, {})[
                                payload.attachment.id
                            ] = payload
                            continue
                        if self.on_message:
                            await asyncio.ensure_future(self.on_message(ws, payload))
                    else:
                        # Binary frame: non-chunked or one chunk of a chunked transfer
                        raw = message
                        if len(raw) < ATTACHMENT_ID_BYTES:
                            logger.warning("bus server: binary frame too short, drop")
                            continue
                        att_id = (
                            raw[:ATTACHMENT_ID_BYTES]
                            .decode("utf-8", errors="replace")
                            .strip("\x00 ")
                        )
                        preamble = (self._pending.get(ws_id) or {}).get(att_id)
                        if preamble is None or not preamble.attachment:
                            logger.warning(
                                "bus server: binary for unknown att_id %r, drop",
                                att_id[:16],
                            )
                            continue

                        is_chunked = preamble.attachment.chunks > 1

                        if is_chunked:
                            if len(raw) < ATTACHMENT_CHUNK_HEADER_BYTES:
                                logger.warning(
                                    "bus server: chunked frame too short (%d bytes), drop",
                                    len(raw),
                                )
                                continue
                            _, chunk_index, total_chunks, chunk_data = (
                                decode_chunk_frame(raw)
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
                            # All chunks received - assemble
                            self._pending.get(ws_id, {}).pop(att_id, None)
                            self._pending_chunks.pop(att_id, None)
                            body = b"".join(
                                state["received"][i] for i in range(state["total"])
                            )
                        else:
                            self._pending.get(ws_id, {}).pop(att_id, None)
                            body = raw[ATTACHMENT_ID_BYTES:]
                            if preamble.attachment.size != len(body):
                                logger.warning(
                                    "bus server: attachment size mismatch, drop"
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
                                data=body,
                                filename=preamble.attachment.filename,
                                mime=preamble.attachment.mime,
                                size=len(body),
                                id=preamble.attachment.id,
                            ),
                        )
                        if self.on_message:
                            await asyncio.ensure_future(self.on_message(ws, full))
                except (json.JSONDecodeError, ValueError, KeyError) as e:
                    logger.warning("bus server: invalid message %s", e)
        except ConnectionClosed:
            pass
        finally:
            # Clean up any pending state for this ws
            pending_ids = set((self._pending.pop(ws_id, None) or {}).keys())
            for att_id in pending_ids:
                self._pending_chunks.pop(att_id, None)
            self._manager.remove_ws(ws)
            logger.debug("ws disconnected, removed from manager")

    async def _enqueue_outbox(
        self, key: _Key, user_id: UserId, payload: Payload
    ) -> None:
        """Append payload to outbox for this key (memory or backend)."""
        if len(self._outbox[key]) < self._max_outbox_per_key:
            self._outbox[key].append(payload)
            logger.debug("outbox: no connection for %s, queued payload (memory)", key)
        else:
            if self._outbox_backend:
                await self._outbox_backend.append_many(user_id, key[1], [payload])
                logger.debug(
                    "outbox: no connection for %s, queued payload (backend)", key
                )
            else:
                self._outbox[key].append(payload)

    async def _send_to_connections(
        self, conns: Set[ServerConnection], payload: Payload
    ) -> int:
        """Send payload (preamble + binary frame(s)) to each connection. Returns count sent.

        Attachments larger than ATTACHMENT_CHUNK_SIZE are split into multiple frames:
        [id:36][chunk_index:4][total:4][data]. Smaller attachments use the legacy
        single-frame format [id:36][data].
        """
        if payload.attachment and payload.attachment.data:
            att = payload.attachment
            raw_data = att.data
            chunk_list = split_into_chunks(raw_data, ATTACHMENT_CHUNK_SIZE)
            total = len(chunk_list)
            if total > 1 and att.chunks != total:
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
        else:
            chunk_list = []
            total = 0

        sent = 0
        for ws in conns:
            try:
                await ws.send(payload.serialize())
                if payload.attachment and payload.attachment.data:
                    aid = payload.attachment.id or ""
                    if total == 1:
                        id_bytes = aid.encode("utf-8")[:ATTACHMENT_ID_BYTES].ljust(
                            ATTACHMENT_ID_BYTES
                        )[:ATTACHMENT_ID_BYTES]
                        await ws.send(id_bytes + chunk_list[0])
                    else:
                        for idx, chunk in enumerate(chunk_list):
                            await ws.send(encode_chunk_frame(aid, idx, total, chunk))
                sent += 1
            except Exception as e:  # pylint: disable=broad-exception-caught
                logger.warning("_send_to_connections: failed to send to ws: %s", e)
        return sent

    async def _drain_outbox(self, key: _Key) -> None:
        """
        Send outbox payloads for this key (memory first, then backend).
        Send until no conns or send fails, then put the rest back and leave.
        Next subscribe will run drain again.
        """
        user_id, session_id = key
        # Drain memory
        payloads = self._outbox[key]
        if payloads:
            self._outbox[key] = []
            for i, p in enumerate(payloads):
                conns = self._manager.get_connections(user_id, session_id)
                if not conns:
                    self._outbox[key].extend(payloads[i:])
                    return
                sent = await self._send_to_connections(conns, p)
                if sent == 0:
                    self._outbox[key].extend(payloads[i:])
                    return
        # Drain backend
        if not self._outbox_backend:
            return
        payloads_db = await self._outbox_backend.pop_all(user_id, session_id)
        for i, p in enumerate(payloads_db):
            conns = self._manager.get_connections(user_id, session_id)
            if not conns:
                await self._outbox_backend.append_many(
                    user_id, session_id, payloads_db[i:]
                )
                return
            sent = await self._send_to_connections(conns, p)
            if sent == 0:
                await self._outbox_backend.append_many(
                    user_id, session_id, payloads_db[i:]
                )
                return

    async def push(
        self,
        user_id: UserId,
        session_id: str = "",
        payload: Optional[Payload] = None,
    ) -> int:
        """
        Send payload to all WebSockets subscribed to (user_id, session_id).
        If no connection exists (or send to all conns failed), the payload is queued in the outbox
        and will be sent when a client subscribes again. Outbox is drained only on subscribe;
        to deliver backlog without waiting for user action, the client should subscribe on
        connection for all keys it cares about (restore subscriptions).

        Returns number of WebSockets that received the message (0 if queued).
        """
        if payload is None:
            return 0
        key = (user_id, session_id.strip())
        conns = self._manager.get_connections(user_id, session_id.strip())
        if not conns:
            await self._enqueue_outbox(key, user_id, payload)
            return 0
        sent = await self._send_to_connections(conns, payload)
        if sent == 0:
            # All conns failed; queue for drain on next subscribe
            await self._enqueue_outbox(key, user_id, payload)
            logger.debug("outbox: send to %s failed for all conns, queued payload", key)
        return sent

    async def start(self) -> "Server":
        """Start the WebSocket server. Use stop() to shut down. Pass ssl_context for WSS."""
        kwargs = {"ping_interval": 20, "ping_timeout": 20}
        if self._ssl_context is not None:
            kwargs["ssl"] = self._ssl_context
        self._server = await serve(
            self._handle,
            self.host,
            self.port,
            **kwargs,
        )
        # Resolve actual port if port=0
        if self.port == 0 and self._server.sockets:
            self.port = self._server.sockets[0].getsockname()[1]
        logger.info("bus server listening on %s:%s", self.host, self.port)
        return self

    async def run_forever(self) -> None:
        """Run the server until it is closed. Call after start()."""
        if self._server is None:
            raise RuntimeError("Server not started; call start() first")
        await self._server.serve_forever()

    async def stop(self) -> None:
        """Stop the WebSocket server."""
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        logger.info("bus server stopped")
