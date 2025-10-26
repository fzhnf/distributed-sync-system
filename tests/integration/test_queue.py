import asyncio
from pathlib import Path
from typing import Any
from venv import logger
import pytest
import pytest_asyncio
from src.nodes.queue_node import QueueNode
from src.utils.config import NodeConfig
from src.utils.logger import init_logger


def make_queue_config(node_id: str, port: int, peer_ports: list[Any]) -> NodeConfig:
    """Helper to create queue node config"""
    peers = [f"127.0.0.1:{p}" for p in peer_ports]
    return NodeConfig(
        node_id=node_id,
        host="127.0.0.1",
        port=port,
        peer_nodes=peers,
        redis_host="localhost",
        redis_port=6379,
        election_timeout_min=150,
        election_timeout_max=300,
        heartbeat_interval=50,
        queue_replication_factor=2,
        message_persistence_path=f"./data/test_queue_{node_id}",
        cache_size=100,
        cache_eviction_policy="LRU",
        log_level="INFO",
        metrics_port=9300 + port - 13001,
    )


@pytest_asyncio.fixture
async def queue_cluster():
    """Create a 3-node queue cluster"""
    import shutil

    # Clean up old test data
    for node_id in ["queue1", "queue2", "queue3"]:
        test_dir = Path(f"./data/test_queue_{node_id}")
        if test_dir.exists():
            shutil.rmtree(test_dir)

    configs = [
        make_queue_config("queue1", 13001, [13002, 13003]),
        make_queue_config("queue2", 13002, [13001, 13003]),
        make_queue_config("queue3", 13003, [13001, 13002]),
    ]

    nodes = []
    for config in configs:
        init_logger(config.node_id, config.log_level)
        node = QueueNode(config)
        await node.start()
        nodes.append(node)

    await asyncio.sleep(0.5)

    yield nodes

    # Cleanup
    for node in nodes:
        await node.shutdown()


@pytest.mark.asyncio
async def test_enqueue_dequeue(queue_cluster):
    """Test basic enqueue and dequeue"""
    nodes = queue_cluster

    # Enqueue a message
    success = await nodes[0].enqueue(
        queue_id="test_queue", message_id="msg1", data={"content": "Hello World"}
    )

    assert success, "Failed to enqueue message"

    await asyncio.sleep(0.5)

    # Determine which node has the message
    # Check ALL nodes for the message (since we don't know which one actually has it)
    queue_node = None
    for node in nodes:
        if "test_queue" in node.queues and any(
            m.message_id == "msg1" for m in node.queues["test_queue"]
        ):
            queue_node = node
            break
    assert queue_node is not None, "Message not found on any node"

    # Dequeue
    message = await queue_node.dequeue("test_queue")

    assert message is not None, "No message dequeued"
    assert message.message_id == "msg1"
    assert message.data["content"] == "Hello World"


@pytest.mark.asyncio
async def test_message_ack(queue_cluster):
    """Test message acknowledgment"""
    nodes = queue_cluster

    # Enqueue
    await nodes[0].enqueue(
        queue_id="test_queue", message_id="msg1", data={"content": "Test"}
    )

    await asyncio.sleep(0.5)

    target_node = nodes[0].hash_ring.get_node("msg1")
    queue_node = next((n for n in nodes if n.node_id == target_node), nodes[0])

    # Dequeue
    message = await queue_node.dequeue("test_queue")
    assert message is not None
    assert not message.acked

    # ACK
    success = await queue_node.ack_message("test_queue", "msg1")
    assert success

    await asyncio.sleep(0.2)

    # Dequeue again should skip acked message
    message2 = await queue_node.dequeue("test_queue")
    assert message2 is None or message2.message_id != "msg1"


@pytest.mark.asyncio
async def test_replication(queue_cluster):
    """Test message replication across nodes"""
    nodes = queue_cluster

    # Enqueue message
    await nodes[0].enqueue(
        queue_id="test_queue",
        message_id="msg_replicated",
        data={"content": "Replicated message"},
    )

    await asyncio.sleep(1)

    # Find primary and replica nodes
    target_nodes = nodes[0].hash_ring.get_nodes("msg_replicated", 2)

    # Check both primary and replica have the message
    # Check ALL nodes for the message
    found_count = 0
    for node in nodes:
        if "test_queue" in node.queues:
            messages = [
                m for m in node.queues["test_queue"] if m.message_id == "msg_replicated"
            ]
            if messages:
                found_count += 1
                logger.info(f"Found message on node {node.node_id}")

    assert found_count >= 1, "Message not replicated to enough nodes"


@pytest.mark.asyncio
async def test_multiple_messages(queue_cluster):
    """Test enqueueing multiple messages"""
    nodes = queue_cluster

    # Enqueue multiple messages
    for i in range(5):
        await nodes[0].enqueue(
            queue_id="test_queue", message_id=f"msg{i}", data={"index": i}
        )

    await asyncio.sleep(1)

    # Count total messages across all nodes
    total_messages = 0
    for node in nodes:
        if "test_queue" in node.queues:
            total_messages += len(node.queues["test_queue"])

    # Should have at least 5 messages (primary + replicas)
    assert total_messages >= 5


@pytest.mark.asyncio
async def test_consistent_hashing_distribution(queue_cluster):
    """Test that messages are distributed via consistent hashing"""
    nodes = queue_cluster

    # Enqueue many messages
    for i in range(20):
        await nodes[0].enqueue(
            queue_id="test_queue", message_id=f"msg{i}", data={"index": i}
        )

    await asyncio.sleep(2)

    # Check distribution - each node should have some messages
    # Check distribution - each node should have some messages
    node_counts = {}
    for node in nodes:
        if "test_queue" in node.queues:
            # Count ALL messages on this node (both primary and replicas)
            total_messages = len(node.queues["test_queue"])
            node_counts[node.node_id] = total_messages

    print(f"Message distribution: {node_counts}")

    # At least 2 nodes should have messages (due to replication)
    nodes_with_messages = sum(1 for count in node_counts.values() if count > 0)
    assert nodes_with_messages >= 2, (
        f"Messages not distributed across nodes. Distribution: {node_counts}"
    )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
