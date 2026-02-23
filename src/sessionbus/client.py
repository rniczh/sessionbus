"""
Bus client: connect to the bus and exchange messages for one or more (user_id, session_id).

Subscribe to the keys you need, then send and receive payloads per key.

Usage:
    client = Client("ws://localhost:8765", pool_size=4)
    await client.connect()
    await client.subscribe("user-1", "session-123")
    await client.send("Hello", user_id="user-1", session_id="session-123")
    payload = await client.recv("user-1", "session-123")
    await client.unsubscribe("user-1", "session-123")
    await client.close()
"""

import asyncio
import logging
import ssl
from typing import Any, Awaitable, Callable, Optional

from sessionbus.pool import BusConnectionPool
from sessionbus.types import Attachment, MessageType, Payload, UserId

logger = logging.getLogger(__name__)

# pylint: disable=too-many-arguments

class Client:
    """
    Client for the bus: one pool, one or many (user_id, session_id).

    Call subscribe(user_id, session_id) for each key you want to receive from;
    use recv() and send() with explicit user_id and session_id.
    get_queue() is optional, it gives a way for user to get the payload queue for a key directly.
    """

    def __init__(
        self,
        url: str,
        *,
        pool_size: int = 1,
        retry_interval: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
        after_connect: Optional[Callable[[Any], Awaitable[None]]] = None,
    ) -> None:
        self.url = url
        self.retry_interval = retry_interval
        self._ssl_context = ssl_context
        self._pool = BusConnectionPool(
            url,
            pool_size=max(1, pool_size),
            retry_interval=retry_interval,
            ssl_context=ssl_context,
            after_connect=after_connect,
        )

    async def connect(self) -> "Client":
        """Connect the pool to the bus server."""
        await self._pool.connect()
        return self

    async def get_queue(
        self, user_id: UserId, session_id: str = ""
    ) -> asyncio.Queue[Payload]:
        """Get the queue for this (user_id, session_id)"""
        return await self._pool.get_queue(user_id, session_id)

    async def subscribe(self, user_id: UserId, session_id: str = "") -> None:
        """Subscribe to (user_id, session_id) to receive payloads for this key"""
        if not self._pool.is_connected():
            raise RuntimeError("Client not connected")
        await self._pool.subscribe(user_id, session_id)

    async def unsubscribe(self, user_id: UserId, session_id: str = "") -> None:
        """Stop receiving pushes for (user_id, session_id)."""
        if not self._pool.is_connected():
            raise RuntimeError("Client not connected")
        await self._pool.unsubscribe(user_id, session_id)

    async def send(
        self,
        content: str = "",
        *,
        user_id: UserId,
        session_id: str = "",
        action: Optional[str] = None,
        attachment: Optional[Attachment] = None,
    ) -> None:
        """Send a SEND payload for (user_id, session_id)."""
        payload = Payload(
            type=MessageType.SEND,
            user_id=user_id,
            session_id=(session_id or "").strip(),
            content=content,
            action=action,
            attachment=attachment,
        )
        await self._pool.send_payload(payload)

    async def recv(
        self,
        user_id: UserId,
        session_id: str = "",
        timeout: Optional[float] = None,
    ) -> Payload:
        """Receive the next payload for (user_id, session_id)."""
        queue = await self._pool.get_queue(user_id, session_id)
        if timeout is not None:
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        return await queue.get()

    def is_connected(
        self, user_id: Optional[UserId] = None, session_id: str = ""
    ) -> bool:
        """True if connected"""
        return self._pool.is_connected(user_id, session_id)

    async def close(self) -> None:
        """Close the pool and all connections."""
        await self._pool.close()

    def slot_count(self) -> int:
        """Number of pool slots (connections)."""
        return self._pool.slot_count()

    # Example for async with context manager:
    #
    # ```python
    # async with Client("ws://localhost:8765") as client:
    #     await client.subscribe("user-1", "session-1")
    #     await client.send("Hi", user_id="user-1", session_id="session-1")
    #     payload = await client.recv("user-1", "session-1")
    # ```
    #
    # It will connect to the server when entering the context manager
    # and close the connection when exiting.

    async def __aenter__(self) -> "Client":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
