"""
BusConnectionPool: fixed pool of BusConnections with hash routing and auto-reconnect.

When a slot's BusConnection drops, the pool reconnects it and
re-subscribes all keys that were on that slot.
"""

import asyncio
import logging
import ssl
from typing import Any, Awaitable, Callable, Dict, Optional, Set, Tuple

from sessionbus.connection import BusConnection
from sessionbus.types import Payload, UserId

logger = logging.getLogger(__name__)

Key = Tuple[UserId, str]


# pylint: disable=too-many-instance-attributes
class BusConnectionPool:
    """
    Pool of BusConnections with hash-based routing and auto-reconnect.

    Each (user_id, session_id) is always routed to the same slot (hash % pool_size).
    If that slot drops, the pool reconnects it and re-subscribes all its keys.
    """

    def __init__(
        self,
        url: str,
        pool_size: int = 4,
        *,
        retry_interval: float = 5.0,
        ssl_context: Optional[ssl.SSLContext] = None,
        after_connect: Optional[Callable[[Any], Awaitable[None]]] = None,
    ) -> None:
        self.url = url
        self.pool_size = max(1, pool_size)
        self.retry_interval = retry_interval
        self._ssl_context = ssl_context
        self._after_connect = after_connect
        self._pool: list[BusConnection] = []
        # Per-slot lock: prevents concurrent reconnects for the same slot
        self._slot_locks: list[asyncio.Lock] = []
        # key -> slot index
        self._key_to_slot: Dict[Key, int] = {}
        # slot -> set of keys subscribed on it (for re-subscribe after reconnect)
        self._slot_to_keys: Dict[int, Set[Key]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> "BusConnectionPool":
        """Connect all pool slots to the bus server."""
        self._pool = [BusConnection(self.url) for _ in range(self.pool_size)]
        self._slot_locks = [asyncio.Lock() for _ in range(self.pool_size)]
        self._slot_to_keys = {i: set() for i in range(self.pool_size)}
        for i, conn in enumerate(self._pool):
            await conn.connect(
                retry_interval=self.retry_interval,
                ssl_context=self._ssl_context,
                after_connect=self._after_connect,
            )
            logger.info("BusConnectionPool: slot %d connected to %s", i, self.url)
        return self

    async def close(self) -> None:
        """Close all pool connections."""
        for conn in self._pool:
            await conn.close()
        self._pool.clear()
        self._key_to_slot.clear()
        self._slot_to_keys.clear()
        logger.info("BusConnectionPool: closed (url=%s)", self.url)

    async def __aenter__(self) -> "BusConnectionPool":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Routing
    # ------------------------------------------------------------------

    def _slot_for(self, key: Key) -> int:
        return hash(key) % self.pool_size

    # ------------------------------------------------------------------
    # Auto-reconnect
    # ------------------------------------------------------------------

    async def _ensure_slot(self, slot: int) -> BusConnection:
        """Return the BusConnection for this slot, reconnecting if needed."""
        conn = self._pool[slot]
        if conn.is_connected():
            return conn
        async with self._slot_locks[slot]:
            # Double-check after acquiring lock
            conn = self._pool[slot]
            if conn.is_connected():
                return conn
            logger.warning(
                "BusConnectionPool: slot %d disconnected, reconnecting to %s ...",
                slot,
                self.url,
            )
            await conn.close()
            new_conn = BusConnection(self.url)
            await new_conn.connect(
                retry_interval=self.retry_interval,
                ssl_context=self._ssl_context,
                after_connect=self._after_connect,
            )
            self._pool[slot] = new_conn
            # Re-subscribe all keys that were on this slot
            for key in self._slot_to_keys.get(slot, set()):
                user_id, session_id = key
                await new_conn.get_queue(user_id, session_id)
                await new_conn.subscribe(user_id, session_id)
                logger.info(
                    "BusConnectionPool: slot %d re-subscribed (%s, %s)",
                    slot,
                    user_id,
                    session_id,
                )
            logger.info("BusConnectionPool: slot %d reconnected", slot)
            return new_conn

    # ------------------------------------------------------------------
    # Public API (mirrors BusConnection)
    # ------------------------------------------------------------------

    async def get_queue(
        self, user_id: UserId, session_id: str = ""
    ) -> asyncio.Queue[Payload]:
        """Get or create the queue for this (user_id, session_id)."""
        key = (user_id, (session_id or "").strip())
        slot = self._slot_for(key)
        conn = await self._ensure_slot(slot)
        return await conn.get_queue(user_id, session_id)

    async def subscribe(self, user_id: UserId, session_id: str = "") -> None:
        """Subscribe (user_id, session_id) on the appropriate pool slot."""
        key = (user_id, (session_id or "").strip())
        slot = self._slot_for(key)
        conn = await self._ensure_slot(slot)
        await conn.get_queue(user_id, session_id)
        self._key_to_slot[key] = slot
        self._slot_to_keys[slot].add(key)
        await conn.subscribe(user_id, session_id)

    async def unsubscribe(self, user_id: UserId, session_id: str = "") -> None:
        """Stop receiving pushes for this key. Server removes this connection from the key."""
        key = (user_id, (session_id or "").strip())
        slot = self._slot_for(key)
        if slot < len(self._pool) and self._pool[slot].is_connected():
            await self._pool[slot].unsubscribe(user_id, session_id)
        self._key_to_slot.pop(key, None)
        slot_keys = self._slot_to_keys.get(slot)
        if slot_keys is not None:
            slot_keys.discard(key)

    async def send_payload(self, payload: Payload) -> None:
        """Send a payload on the slot for (payload.user_id, payload.session_id)."""
        key = (payload.user_id, (payload.session_id or "").strip())
        slot = self._slot_for(key)
        conn = await self._ensure_slot(slot)
        await conn.send_payload(payload)

    def is_connected(
        self, user_id: Optional[UserId] = None, session_id: str = ""
    ) -> bool:
        """
        If user_id given: True if the slot for that key is connected.
        Otherwise: True if ALL slots are connected.
        """
        if user_id is not None:
            key = (user_id, (session_id or "").strip())
            slot = self._slot_for(key)
            return self._pool[slot].is_connected() if self._pool else False
        return bool(self._pool) and all(c.is_connected() for c in self._pool)

    def slot_count(self) -> int:
        """Return the number of slots in the pool."""
        return len(self._pool)
