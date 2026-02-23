"""
PostgreSQL outbox backend.

Schema (run once to create the table):

  CREATE TABLE sessionbus_outbox (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    type TEXT NOT NULL,
    content TEXT NOT NULL DEFAULT '',
    error TEXT,
    action TEXT,
    att_filename TEXT,
    att_mime TEXT,
    att_size INT NOT NULL DEFAULT 0,
    att_id TEXT,
    att_data BYTEA
  );
  CREATE INDEX sessionbus_outbox_key_created ON sessionbus_outbox (user_id, session_id, created_at);
"""

import logging
from typing import List, Optional

import asyncpg

from sessionbus.types import Attachment, MessageType, Payload, UserId

logger = logging.getLogger(__name__)

TABLE_DEFAULT = "sessionbus_outbox"


async def _ensure_pool(conn_string: str, pool=None):
    """Create a connection pool for PostgreSQL.
    Since each time, when user need to do something with the database,
    they need to create a new connection, it's better to create a pool of connections
    and reuse them.
    """
    if pool is not None:
        return pool
    return await asyncpg.create_pool(conn_string, min_size=1, max_size=10)


def _row_to_payload(row) -> Payload:
    """Build Payload from a DB row (asyncpg Record)."""
    att = None
    if row["att_id"] is not None or row["att_data"] is not None:
        att = Attachment(
            data=row["att_data"] or b"",
            filename=row["att_filename"] or "",
            mime=row["att_mime"] or "application/octet-stream",
            size=row["att_size"] or 0,
            id=row["att_id"],
        )
    return Payload(
        type=MessageType(row["type"]),
        user_id=row["user_id"],
        session_id=row["session_id"],
        content=row["content"] or "",
        error=row["error"],
        action=row["action"],
        attachment=att,
    )


def _payload_to_row(p: Payload) -> tuple:
    """Return a tuple of values to insert into the database."""
    att = p.attachment
    return (
        p.user_id,
        p.session_id,
        p.type.value,
        p.content or "",
        p.error,
        p.action,
        att.filename if att else None,
        att.mime if att else None,
        att.size if att else 0,
        att.id if att else None,
        att.data if att else None,
    )


class PostgresOutbox:
    """
    Outbox backend that stores pending payloads in PostgreSQL.
    """

    def __init__(
        self,
        connection_string: str,
        *,
        table: str = TABLE_DEFAULT,
        pool=None,
    ) -> None:
        """
        connection_string: e.g. "postgresql://user:pass@localhost:5432/dbname"
        table: table name (default sessionbus_outbox).
        pool: optional existing asyncpg pool; if None, a pool is created on first use.
        """
        self._conn_string = connection_string
        self._table = table
        self._pool = pool
        self._pool_created: Optional[object] = None

    async def _get_pool(self):
        if self._pool is not None:
            return self._pool
        if self._pool_created is None:
            self._pool_created = await _ensure_pool(self._conn_string, self._pool)
        return self._pool_created

    async def create_table_if_not_exists(self) -> None:
        """Create the outbox table and index if they do not exist."""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                    id BIGSERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    type TEXT NOT NULL,
                    content TEXT NOT NULL DEFAULT '',
                    error TEXT,
                    action TEXT,
                    att_filename TEXT,
                    att_mime TEXT,
                    att_size INT NOT NULL DEFAULT 0,
                    att_id TEXT,
                    att_data BYTEA
                )
                """
            )
            await conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self._table}_key_created
                ON {self._table} (user_id, session_id, created_at)
                """
            )
        logger.info("postgres outbox table %s ready", self._table)

    async def append_many(
        self,
        user_id: UserId,
        session_id: str,
        payloads: List[Payload],
    ) -> None:
        """Append multiple payloads to the outbox."""
        if not payloads:
            return
        pool = await self._get_pool()
        rows = [_payload_to_row(p) for p in payloads]
        async with pool.acquire() as conn:
            await conn.executemany(
                f"""
                INSERT INTO {self._table}
                (user_id, session_id, type, content, error, action,
                 att_filename, att_mime, att_size, att_id, att_data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """,
                rows,
            )
        logger.debug(
            "postgres outbox: appended %d payloads for %s/%s",
            len(payloads),
            user_id,
            session_id,
        )

    async def pop_all(self, user_id: UserId, session_id: str) -> List[Payload]:
        """Remove and return all payloads for this key, oldest first."""
        pool = await self._get_pool()
        async with pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    f"""
                    SELECT id, user_id, session_id, type, content, error, action,
                           att_filename, att_mime, att_size, att_id, att_data
                    FROM {self._table}
                    WHERE user_id = $1 AND session_id = $2
                    ORDER BY created_at, id
                    FOR UPDATE SKIP LOCKED
                    """,
                    user_id,
                    session_id.strip(),
                )
                if not rows:
                    return []
                ids = [r["id"] for r in rows]
                await conn.execute(
                    f"DELETE FROM {self._table} WHERE id = ANY($1::bigint[])",
                    ids,
                )
        out = [_row_to_payload(r) for r in rows]
        logger.debug(
            "postgres outbox: popped %d payloads for %s/%s",
            len(out),
            user_id,
            session_id,
        )
        return out

    async def close(self) -> None:
        """Close the pool if this instance created it."""
        if self._pool_created is not None:
            await self._pool_created.close()
            self._pool_created = None
