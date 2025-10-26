# pyright: reportUnusedParameter=false
# pyright: reportExplicitAny=false
# pyright: reportAny=false


import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, cast, override

from ..nodes.base_node import BaseNode
from ..utils.config import NodeConfig
from ..utils.consistent_hash import ConsistentHashRing
from ..communication.message_passing import Message, MessageType


@dataclass
class QueueMessage:
    """Message in the queue"""

    message_id: str
    queue_id: str
    data: dict[str, Any]
    timestamp: float
    ack_required: bool = True
    acked: bool = False
    replicated_to: set[str] = field(default_factory=set)

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "queue_id": self.queue_id,
            "data": self.data,
            "timestamp": self.timestamp,
            "ack_required": self.ack_required,
            "acked": self.acked,
            "replicated_to": list(self.replicated_to),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "QueueMessage":
        return cls(
            message_id=str(d["message_id"]),
            queue_id=str(d["queue_id"]),
            data=cast(dict[str, Any], d["data"]),
            timestamp=float(d["timestamp"]),
            ack_required=bool(d.get("ack_required", True)),
            acked=bool(d.get("acked", False)),
            replicated_to=set(cast(list[str], d.get("replicated_to", []))),
        )


class QueueNode(BaseNode):
    """Distributed queue node using consistent hashing"""

    def __init__(self, config: NodeConfig | None = None):
        super().__init__(config)

        # Consistent hashing ring
        self.hash_ring: ConsistentHashRing = ConsistentHashRing(virtual_nodes=150)

        # Local queue storage: queue_id -> list of messages
        self.queues: dict[str, list[QueueMessage]] = {}

        # Persistence
        self.persistence_dir: Path = Path(
            config.message_persistence_path if config else "./data/queue"
        )
        self.persistence_dir.mkdir(parents=True, exist_ok=True)

        # Replication factor
        self.replication_factor: int = config.queue_replication_factor if config else 2

        # Register queue message handlers
        self.register_handler(MessageType.QUEUE_ENQUEUE, self._handle_enqueue)
        self.register_handler(MessageType.QUEUE_DEQUEUE, self._handle_dequeue)
        self.register_handler(MessageType.QUEUE_ACK, self._handle_ack)
        self.register_handler(MessageType.QUEUE_REPLICATE, self._handle_replicate)

        self.logger.info("QueueNode initialized")

    @override
    async def start(self):
        """Start queue node"""
        await super().start()

        # Add self to hash ring
        node_address = f"{self.config.host}:{self.config.port}"
        self.hash_ring.add_node(node_address)

        # Add peers to hash ring with their addresses
        for peer in self.peers:
            # peer is already in "host:port" format
            # Extract a node_id or use the address as id
            self.hash_ring.add_node(peer, peer)

        # Load persisted messages
        await self._load_persisted_messages()

        self.logger.info(
            f"QueueNode started with {len(self.hash_ring.get_all_nodes())} nodes in ring"
        )

    async def _load_persisted_messages(self) -> None:
        """Load messages from disk"""
        for queue_file in self.persistence_dir.glob("*.json"):
            try:
                with open(queue_file, "r") as f:
                    data = json.load(f)
                    queue_id = str(data["queue_id"])
                    messages = [
                        QueueMessage.from_dict(cast(dict[str, Any], m))
                        for m in data["messages"]
                    ]
                    self.queues[queue_id] = messages
                    self.logger.info(
                        f"Loaded {len(messages)} messages for queue {queue_id}"
                    )
            except Exception as e:
                self.logger.error(f"Error loading {queue_file}: {e}")

    async def _persist_queue(self, queue_id: str) -> None:
        """Persist queue to disk"""
        if queue_id not in self.queues:
            return

        file_path = self.persistence_dir / f"{queue_id}_{self.node_id}.json"

        try:
            data = {
                "queue_id": queue_id,
                "node_id": self.node_id,
                "messages": [m.to_dict() for m in self.queues[queue_id]],
            }

            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error persisting queue {queue_id}: {e}")

    async def enqueue(
        self, queue_id: str, message_id: str, data: dict[str, Any]
    ) -> bool:
        """
        Enqueue a message.
        Returns True if successfully enqueued on primary and replicas.
        """
        # Determine which nodes should handle this message
        target_nodes = self.hash_ring.get_nodes(message_id, self.replication_factor)

        if not target_nodes:
            self.logger.error("No nodes available in hash ring")
            return False

        primary_node = target_nodes[0]

        # Create message
        msg = QueueMessage(
            message_id=message_id,
            queue_id=queue_id,
            data=data,
            timestamp=datetime.now().timestamp(),
        )

        # If we're the primary node, store locally
        if primary_node == self.node_id:
            await self._store_message(msg)

            # Replicate to other nodes
            if len(target_nodes) > 1:
                await self._replicate_to_nodes(msg, target_nodes[1:])

            return True
        else:
            # Forward to primary node
            return await self._forward_enqueue(primary_node, msg)

    async def _store_message(self, msg: QueueMessage) -> None:
        """Store message locally"""
        if msg.queue_id not in self.queues:
            self.queues[msg.queue_id] = []

        self.queues[msg.queue_id].append(msg)

        # Persist to disk
        await self._persist_queue(msg.queue_id)

        self.logger.info(f"Stored message {msg.message_id} in queue {msg.queue_id}")

    async def _replicate_to_nodes(self, msg: QueueMessage, nodes: list[str]):
        """Replicate message to replica nodes"""
        self.logger.info(
            f"Attempting to replicate message {msg.message_id} to nodes: {nodes}"
        )
        replicate_msg = Message.create(
            msg_type=MessageType.QUEUE_REPLICATE,
            sender_id=self.node_id,
            receiver_id="broadcast",
            payload={"message": msg.to_dict()},
        )

        for node_id in nodes:
            # Get the actual address for this node
            node_address = self.hash_ring.get_node_address(node_id)
            if not node_address:
                self.logger.warning(f"No address found for node {node_id}")
                continue

            response = await self.send_message(
                node_address, replicate_msg, retry_count=2
            )
            if response and response.payload.get("success"):
                msg.replicated_to.add(node_id)
                self.logger.debug(f"Replicated message {msg.message_id} to {node_id}")

    async def _forward_enqueue(self, target_node: str, msg: QueueMessage) -> bool:
        """Forward enqueue to the primary node"""
        # Get the actual address
        node_address = self.hash_ring.get_node_address(target_node)
        if not node_address:
            self.logger.error(f"No address found for node {target_node}")
            return False

        enqueue_msg = Message.create(
            msg_type=MessageType.QUEUE_ENQUEUE,
            sender_id=self.node_id,
            receiver_id=node_address,
            payload={"message": msg.to_dict()},
        )

        response = await self.send_message(node_address, enqueue_msg)
        return response is not None and bool(response.payload.get("success", False))

    async def dequeue(self, queue_id: str) -> QueueMessage | None:
        """
        Dequeue a message from the queue.
        Returns the message or None if queue is empty.
        """
        # Check if we have this queue
        if queue_id not in self.queues or not self.queues[queue_id]:
            return None

        # Get first unacked message
        for msg in self.queues[queue_id]:
            if not msg.acked:
                self.logger.info(
                    f"Dequeued message {msg.message_id} from queue {queue_id}"
                )
                return msg

        return None

    async def ack_message(self, queue_id: str, message_id: str) -> bool:
        """Acknowledge a message (consumer finished processing)"""
        if queue_id not in self.queues:
            return False

        # Find and mark as acked
        for msg in self.queues[queue_id]:
            if msg.message_id == message_id:
                msg.acked = True

                # Persist
                await self._persist_queue(queue_id)

                self.logger.info(f"Acked message {message_id}")

                # Notify replicas
                await self._replicate_ack(queue_id, message_id, msg.replicated_to)

                return True

        return False

    async def _replicate_ack(self, queue_id: str, message_id: str, replicas: set[str]):
        """Replicate ACK to replica nodes"""
        ack_msg = Message.create(
            msg_type=MessageType.QUEUE_ACK,
            sender_id=self.node_id,
            receiver_id="broadcast",
            payload={"queue_id": queue_id, "message_id": message_id},
        )

        for replica_id in replicas:
            replica_address = self.hash_ring.get_node_address(replica_id)
            if replica_address:
                _ = await self.send_message(replica_address, ack_msg, retry_count=1)

    async def _handle_enqueue(self, msg: Message) -> dict[str, Any]:
        """Handle enqueue request"""
        try:
            message_dict = cast(dict[str, Any], msg.payload["message"])
            queue_msg = QueueMessage.from_dict(message_dict)

            await self._store_message(queue_msg)

            # Replicate if needed
            target_nodes = self.hash_ring.get_nodes(
                queue_msg.message_id, self.replication_factor
            )
            if len(target_nodes) > 1:
                await self._replicate_to_nodes(queue_msg, target_nodes[1:])

            return {"success": True}
        except Exception as e:
            self.logger.error(f"Error handling enqueue: {e}")
            return {"success": False, "error": str(e)}

    async def _handle_dequeue(self, msg: Message) -> dict[str, Any]:
        """Handle dequeue request"""
        queue_id = str(msg.payload.get("queue_id", ""))

        message = await self.dequeue(queue_id)

        if message:
            return {"success": True, "message": message.to_dict()}
        else:
            return {"success": False, "message": None}

    async def _handle_ack(self, msg: Message) -> dict[str, Any]:
        """Handle ACK from replica"""
        queue_id = str(msg.payload["queue_id"])
        message_id = str(msg.payload["message_id"])

        success = await self.ack_message(queue_id, message_id)
        return {"success": success}

    async def _handle_replicate(self, msg: Message) -> dict[str, Any]:
        """Handle replication request"""
        try:
            message_dict = cast(dict[str, Any], msg.payload["message"])
            queue_msg = QueueMessage.from_dict(message_dict)

            await self._store_message(queue_msg)

            return {"success": True}
        except Exception as e:
            self.logger.error(f"Error handling replication: {e}")
            return {"success": False, "error": str(e)}

    def get_queue_size(self, queue_id: str) -> int:
        """Get number of messages in queue"""
        if queue_id not in self.queues:
            return 0
        return len([m for m in self.queues[queue_id] if not m.acked])
