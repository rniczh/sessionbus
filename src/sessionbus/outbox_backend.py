"""
Outbox backend: persist outbox when in-memory capacity is exceeded.

When Server's in-memory outbox for a key exceeds max_outbox_per_key, payloads
are stored in a backend (e.g. PostgreSQL). On drain, Server sends from memory
first, then loads from the backend for that key.
"""

from typing import List, Protocol

from sessionbus.types import Payload, UserId


class OutboxBackend(Protocol):
    """Protocol for outbox persistence (e.g. PostgreSQL)."""

    async def append_many(
        self,
        user_id: UserId,
        session_id: str,
        payloads: List[Payload],
    ) -> None:
        """Append payloads to the outbox for this key"""

    async def pop_all(self, user_id: UserId, session_id: str) -> List[Payload]:
        """
        Remove and return all payloads for this key in order (oldest first).
        Used when draining; if send fails mid-way, caller re-appends the rest via append_many.
        """
