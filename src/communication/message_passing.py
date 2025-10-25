from enum import Enum
from dataclasses import dataclass, asdict
from typing import Any
import json
import uuid
from datetime import datetime


class MessageType(Enum):
    """All message types in the distributed system"""

    # Raft consensus messages
    REQUEST_VOTE = "REQUEST_VOTE"
    REQUEST_VOTE_RESPONSE = "REQUEST_VOTE_RESPONSE"
    APPEND_ENTRIES = "APPEND_ENTRIES"
    APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE"

    # Lock manager messages
    LOCK_ACQUIRE = "LOCK_ACQUIRE"
    LOCK_RELEASE = "LOCK_RELEASE"
    LOCK_RESPONSE = "LOCK_RESPONSE"
    DEADLOCK_DETECT = "DEADLOCK_DETECT"

    # Queue messages
    QUEUE_ENQUEUE = "QUEUE_ENQUEUE"
    QUEUE_DEQUEUE = "QUEUE_DEQUEUE"
    QUEUE_ACK = "QUEUE_ACK"
    QUEUE_REPLICATE = "QUEUE_REPLICATE"

    # Cache messages
    CACHE_GET = "CACHE_GET"
    CACHE_PUT = "CACHE_PUT"
    CACHE_INVALIDATE = "CACHE_INVALIDATE"
    CACHE_RESPONSE = "CACHE_RESPONSE"

    # Health and discovery
    HEARTBEAT = "HEARTBEAT"
    PING = "PING"
    PONG = "PONG"


@dataclass
class Message:
    """Base message structure for all node communication"""

    msg_id: str
    msg_type: MessageType
    sender_id: str
    receiver_id: str
    timestamp: float
    payload: dict[str, Any]
    term: int | None = None  # For Raft consensus

    @classmethod
    def create(
        cls,
        msg_type: MessageType,
        sender_id: str,
        receiver_id: str,
        payload: dict[str, Any],
        term: int | None = None,
    ) -> "Message":
        """Factory method to create a new message"""
        return cls(
            msg_id=str(uuid.uuid4()),
            msg_type=msg_type,
            sender_id=sender_id,
            receiver_id=receiver_id,
            timestamp=datetime.now().timestamp(),
            payload=payload,
            term=term,
        )

    def to_json(self) -> str:
        """Serialize message to JSON"""
        data = asdict(self)
        data["msg_type"] = self.msg_type.value
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        """Deserialize message from JSON"""
        data = json.loads(json_str)
        data["msg_type"] = MessageType(data["msg_type"])
        return cls(**data)

    def to_bytes(self) -> bytes:
        """Serialize message to bytes for network transmission"""
        return self.to_json().encode("utf-8")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Deserialize message from bytes"""
        return cls.from_json(data.decode("utf-8"))


@dataclass
class RequestVotePayload:
    """Payload for Raft RequestVote RPC"""

    candidate_id: str
    last_log_index: int
    last_log_term: int


@dataclass
class AppendEntriesPayload:
    """Payload for Raft AppendEntries RPC"""

    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[Any]
    leader_commit: int


@dataclass
class LockPayload:
    """Payload for lock operations"""

    lock_id: str
    lock_type: str  # "shared" or "exclusive"
    resource_id: str
    timeout: float | None = None


@dataclass
class QueuePayload:
    """Payload for queue operations"""

    queue_id: str
    message_id: str
    message_data: Any
    priority: int = 0


@dataclass
class CachePayload:
    """Payload for cache operations"""

    key: str
    value: Any | None = None
    state: str | None = None  # MESI state
    version: int | None = None
