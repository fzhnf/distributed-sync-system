import asyncio
from typing import Any
import pytest
import pytest_asyncio
from src.consensus.raft import RaftNode
from src.utils.config import NodeConfig
from src.utils.logger import init_logger


def make_raft_config(node_id: str, port: int, peer_ports: list[Any]) -> NodeConfig:
    """Helper to create Raft node config"""
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
        message_persistence_path="./data/test_queue",
        cache_size=100,
        cache_eviction_policy="LRU",
        log_level="INFO",
        metrics_port=9090 + port - 10001,
    )


@pytest_asyncio.fixture
async def three_node_cluster():
    """Create a 3-node Raft cluster"""
    configs = [
        make_raft_config("raft1", 10001, [10002, 10003]),
        make_raft_config("raft2", 10002, [10001, 10003]),
        make_raft_config("raft3", 10003, [10001, 10002]),
    ]

    nodes = []
    for config in configs:
        init_logger(config.node_id, config.log_level)
        node = RaftNode(config)
        await node.start()
        nodes.append(node)

    # Wait for cluster to stabilize
    await asyncio.sleep(2)

    yield nodes

    # Cleanup
    for node in nodes:
        await node.shutdown()


@pytest.mark.asyncio
async def test_leader_election(three_node_cluster):
    """Test that a leader is elected"""
    nodes = three_node_cluster

    # Wait for election
    await asyncio.sleep(1)

    # Check that exactly one leader exists
    leaders = [n for n in nodes if n.is_leader()]

    assert len(leaders) == 1, f"Expected 1 leader, found {len(leaders)}"

    leader = leaders[0]
    print(f"Leader elected: {leader.node_id} in term {leader.current_term}")

    # All followers should recognize the leader
    for node in nodes:
        if not node.is_leader():
            assert node.get_leader_id() == leader.node_id


@pytest.mark.asyncio
async def test_log_replication(three_node_cluster):
    """Test that logs are replicated across the cluster"""
    nodes = three_node_cluster

    # Wait for leader election
    await asyncio.sleep(1)

    # Find leader
    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None, "No leader found"

    # Replicate a command
    command = {"type": "set", "key": "test_key", "value": "test_value"}
    success = await leader.replicate_command(command)

    assert success, "Command replication failed"

    # Wait for replication
    await asyncio.sleep(1)

    # Check that all nodes have the entry
    for node in nodes:
        assert len(node.log) > 0, f"Node {node.node_id} has empty log"
        assert node.log[0].command == command, f"Node {node.node_id} has wrong command"
        assert node.state_machine.get("test_key") == "test_value"


@pytest.mark.asyncio
async def test_multiple_commands(three_node_cluster):
    """Test multiple commands are replicated in order"""
    nodes = three_node_cluster

    await asyncio.sleep(1)

    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Replicate multiple commands
    commands = [
        {"type": "set", "key": f"key{i}", "value": f"value{i}"} for i in range(5)
    ]

    for cmd in commands:
        success = await leader.replicate_command(cmd)
        assert success

    await asyncio.sleep(2)

    # Verify all nodes have all commands
    for node in nodes:
        assert len(node.log) == 5
        for i in range(5):
            assert node.state_machine.get(f"key{i}") == f"value{i}"


@pytest.mark.asyncio
async def test_leader_failure_reelection(three_node_cluster):
    """Test that a new leader is elected when the current leader fails"""
    nodes = three_node_cluster

    await asyncio.sleep(1)

    # Find initial leader
    initial_leader = next((n for n in nodes if n.is_leader()), None)
    assert initial_leader is not None

    print(f"Initial leader: {initial_leader.node_id}")

    # Simulate leader failure by shutting it down
    await initial_leader.shutdown()

    # Wait for new election
    await asyncio.sleep(2)

    # Check that a new leader was elected from remaining nodes
    remaining_nodes = [n for n in nodes if n.running]
    leaders = [n for n in remaining_nodes if n.is_leader()]

    assert len(leaders) == 1, f"Expected 1 new leader, found {len(leaders)}"

    new_leader = leaders[0]
    assert new_leader.node_id != initial_leader.node_id
    print(f"New leader elected: {new_leader.node_id}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
