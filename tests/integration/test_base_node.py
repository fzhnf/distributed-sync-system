import asyncio
import pytest
import pytest_asyncio
from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig, set_config
from src.communication.message_passing import Message, MessageType
from src.utils.logger import init_logger


@pytest_asyncio.fixture
async def node1():
    """Create first test node"""
    config = NodeConfig(
        node_id="test_node1",
        host="127.0.0.1",
        port=9001,
        peer_nodes=["127.0.0.1:9002"],
        redis_host="localhost",
        redis_port=6379,
        election_timeout_min=150,
        election_timeout_max=300,
        heartbeat_interval=50,
        queue_replication_factor=2,
        message_persistence_path="./data/test_queue",
        cache_size=100,
        cache_eviction_policy="LRU",
        log_level="DEBUG",
        metrics_port=9090,
    )

    # Initialize logger for this node
    init_logger(config.node_id, config.log_level)

    node = BaseNode(config)
    await node.start()

    yield node

    await node.shutdown()


@pytest_asyncio.fixture
async def node2():
    """Create second test node"""
    config = NodeConfig(
        node_id="test_node2",
        host="127.0.0.1",
        port=9002,
        peer_nodes=["127.0.0.1:9001"],
        redis_host="localhost",
        redis_port=6379,
        election_timeout_min=150,
        election_timeout_max=300,
        heartbeat_interval=50,
        queue_replication_factor=2,
        message_persistence_path="./data/test_queue",
        cache_size=100,
        cache_eviction_policy="LRU",
        log_level="DEBUG",
        metrics_port=9091,
    )

    # Initialize logger for this node
    init_logger(config.node_id, config.log_level)

    node = BaseNode(config)
    await node.start()

    yield node

    await node.shutdown()


@pytest.mark.asyncio
async def test_node_startup(node1):
    """Test that a node starts successfully"""
    assert node1.running
    assert node1.node_id == "test_node1"
    assert len(node1.peers) == 1


@pytest.mark.asyncio
async def test_ping_pong(node1, node2):
    """Test basic message exchange between two nodes"""

    # Register a PING handler on node2
    async def handle_ping(msg: Message):
        return {"pong": True, "echo": msg.payload}

    node2.register_handler(MessageType.PING, handle_ping)

    # Wait for nodes to discover each other and health checks
    await asyncio.sleep(2)

    # Send PING from node1 to node2
    ping_msg = Message.create(
        msg_type=MessageType.PING,
        sender_id=node1.node_id,
        receiver_id=node2.node_id,
        payload={"test": "data"},
    )

    response = await node1.send_message("127.0.0.1:9002", ping_msg)

    assert response is not None
    assert response.payload["pong"] is True
    assert response.payload["echo"]["test"] == "data"


@pytest.mark.asyncio
async def test_broadcast(node1, node2):
    """Test broadcasting to multiple nodes"""

    # Register handler on node2
    async def handle_broadcast(msg: Message):
        return {"received": True, "node": node2.node_id}

    node2.register_handler(MessageType.HEARTBEAT, handle_broadcast)

    await asyncio.sleep(2)

    # Broadcast from node1
    broadcast_msg = Message.create(
        msg_type=MessageType.HEARTBEAT,
        sender_id=node1.node_id,
        receiver_id="broadcast",
        payload={"timestamp": 123456},
    )

    responses = await node1.broadcast_message(broadcast_msg)

    assert len(responses) == 1
    assert responses["127.0.0.1:9002"] is not None
    assert responses["127.0.0.1:9002"].payload["received"] is True


@pytest.mark.asyncio
async def test_health_endpoint(node1):
    """Test health endpoint returns correct data"""
    import aiohttp

    await asyncio.sleep(1)

    async with aiohttp.ClientSession() as session:
        async with session.get("http://127.0.0.1:9001/health") as resp:
            assert resp.status == 200
            data = await resp.json()
            assert data["node_id"] == "test_node1"
            assert data["status"] == "healthy"


@pytest.mark.asyncio
async def test_peer_health_tracking(node1, node2):
    """Test that nodes track peer health"""
    # Wait for health checks to run
    await asyncio.sleep(6)

    # Both nodes should mark each other as healthy
    assert node1.peer_health.get("127.0.0.1:9002") is True
    assert node2.peer_health.get("127.0.0.1:9001") is True


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
