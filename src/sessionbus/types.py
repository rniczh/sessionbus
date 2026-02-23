"""
Types for the session bus.
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Optional
import json
import uuid

# ─── Attachment binary frame layouts ────────────────────────────────────────
#
# Non-chunked (attachment.chunks == 1):
#
#  (prefix) 36 bytes         (data) N bytes
# +------------------+--------------------------------+
# | Attachment ID    | Raw data                       |
# +------------------+--------------------------------+
#
# Chunked (attachment.chunks > 1):
#
#  36 bytes             4 bytes      4 bytes        N bytes
# +------------------+------------+-------------+--------------------------------+
# | Attachment ID    |chunk_index | total_chunks| Chunk data                     |
# +------------------+------------+-------------+--------------------------------+
#
# Receiver determines format from the preamble's `chunks` field.
# Chunks are reassembled in order by chunk_index (0-based).

ATTACHMENT_ID_BYTES = 36

# Attachments larger than this are split into multiple binary frames.
ATTACHMENT_CHUNK_SIZE = 256 * 1024

# Header size for chunked frames: id(36) + chunk_index(4) + total_chunks(4)
ATTACHMENT_CHUNK_HEADER_BYTES = ATTACHMENT_ID_BYTES + 8

UserId = str
SessionId = str


@dataclass(frozen=True)
class SubscriptionKey:
    """
    Key for a subscription: (user_id, session_id).
    """

    user_id: UserId
    session_id: SessionId


class MessageType(StrEnum):
    """
    Top-level message type:
    - SEND: payload with optional "action" (arbitrary string; caller-defined).
    - ERROR: protocol/application error.
    - CONNECTION_LOST: connection lost.
    """

    SEND = "send"
    ERROR = "error"
    CONNECTION_LOST = "connection_lost"


def new_attachment_id() -> str:
    """Return a new attachment UUID."""
    return uuid.uuid4().hex


def split_into_chunks(
    data: bytes, chunk_size: int = ATTACHMENT_CHUNK_SIZE
) -> list[bytes]:
    """Split *data* into fixed-size pieces. Returns a list of one or more byte strings."""
    if len(data) <= chunk_size:
        return [data]
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


def encode_chunk_frame(
    att_id: str, chunk_index: int, total_chunks: int, chunk_data: bytes
) -> bytes:
    """Encode a chunked binary frame: [id:36][chunk_index:4 BE][total_chunks:4 BE][data]."""
    id_bytes = att_id.encode("utf-8")[:ATTACHMENT_ID_BYTES].ljust(ATTACHMENT_ID_BYTES)[
        :ATTACHMENT_ID_BYTES
    ]
    return (
        id_bytes
        + chunk_index.to_bytes(4, "big")
        + total_chunks.to_bytes(4, "big")
        + chunk_data
    )


def decode_chunk_frame(raw: bytes) -> tuple[str, int, int, bytes]:
    """Decode a chunked binary frame. Returns (att_id, chunk_index, total_chunks, chunk_data)."""
    att_id = raw[:ATTACHMENT_ID_BYTES].decode("utf-8", errors="replace").strip("\x00 ")
    chunk_index = int.from_bytes(
        raw[ATTACHMENT_ID_BYTES : ATTACHMENT_ID_BYTES + 4], "big"
    )
    total_chunks = int.from_bytes(
        raw[ATTACHMENT_ID_BYTES + 4 : ATTACHMENT_ID_BYTES + 8], "big"
    )
    chunk_data = raw[ATTACHMENT_CHUNK_HEADER_BYTES:]
    return att_id, chunk_index, total_chunks, chunk_data


@dataclass
class Attachment:
    """
    Attachment for the bus.

    Wire format:
    - Preamble: JSON with attachment { id, name, mime, size, chunks? }.
    - Binary frames:
      - chunks == 1: [id:36][raw data]
      - chunks > 1: N frames of [id:36][chunk_index:4][total_chunks:4][chunk data]
    """

    data: bytes
    filename: str
    mime: str = "application/octet-stream"
    size: int = 0
    id: Optional[str] = None  # required on wire when size > 0
    chunks: int = 1  # total number of binary frames; 1 = not chunked


@dataclass
class Payload:
    """Payload for the bus. type=SEND uses action to distinguish semantics."""

    type: MessageType
    user_id: UserId
    session_id: SessionId
    content: str = ""  # conversation message, text, etc.
    error: Optional[str] = None
    attachment: Optional[Attachment] = None
    action: Optional[str] = (
        None  # arbitrary string; caller-defined (e.g. "subscribe", "message")
    )

    def model_dump(self) -> dict:
        """Dump the payload for sending over the wire."""
        result = {
            "type": self.type.value,
            "user_id": self.user_id,
            "session_id": self.session_id,
            "content": self.content,
            "error": self.error or "",
            "action": self.action or "",
        }
        if self.attachment:
            # id should not be empty string or None
            if not (self.attachment.id and self.attachment.id.strip()):
                raise ValueError("attachment id is required when attachment is present")
            att = {
                # Data is sent as a separate binary frame; only metadata goes in the preamble.
                "id": self.attachment.id,
                "name": self.attachment.filename,
                "mime": self.attachment.mime,
                "size": len(self.attachment.data),
            }
            if self.attachment.chunks > 1:
                att["chunks"] = self.attachment.chunks
            result["attachment"] = att
        return result

    def serialize(self) -> str:
        """Serialize the payload to a JSON string for sending over the wire."""
        return json.dumps(self.model_dump(), ensure_ascii=False)

    @classmethod
    def model_validate(cls, data: dict) -> "Payload":
        """Validate the payload from a dictionary received from the wire."""
        att = data.get("attachment")
        if att is None or not isinstance(att, dict) or "size" not in att:
            attachment = None
        else:
            attachment = Attachment(
                # Data arrives in a separate binary frame; placeholder here.
                data=att.get("data", b""),
                filename=att.get("name", ""),
                mime=att.get("mime", "application/octet-stream"),
                size=att.get("size", 0),
                id=att.get("id"),
                chunks=max(1, int(att.get("chunks", 1))),
            )
        return Payload(
            type=(
                MessageType(data["type"])
                if isinstance(data.get("type"), str)
                else data["type"]
            ),
            user_id=str(data.get("user_id", "") or ""),
            session_id=data.get("session_id", ""),
            content=data.get("content", ""),
            error=data.get("error") or None,
            action=data.get("action") or None,
            attachment=attachment,
        )

    @classmethod
    def deserialize(cls, data: str) -> "Payload":
        """Deserialize the payload from a JSON string received from the wire."""
        return cls.model_validate(json.loads(data))
