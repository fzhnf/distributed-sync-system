import asyncio
from typing import Any
import pytest
import pytest_asyncio
from src.nodes.lock_manager import LockManager, LockType
from src.utils.config import NodeConfig
from src.utils.logger import init_logger


def make_lock_config(node_id: str, port: int, peer_ports: list[Any]) -> NodeConfig:
    """Helper to create lock manager config"""
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
        metrics_port=9200 + port - 12001,
    )


@pytest_asyncio.fixture
async def lock_cluster():
    """Create a 3-node lock manager cluster"""
    configs = [
        make_lock_config("lock1", 12001, [12002, 12003]),
        make_lock_config("lock2", 12002, [12001, 12003]),
        make_lock_config("lock3", 12003, [12001, 12002]),
    ]

    nodes = []
    for config in configs:
        init_logger(config.node_id, config.log_level)
        node = LockManager(config)
        await node.start()
        nodes.append(node)

    # Wait for leader election
    await asyncio.sleep(2)

    yield nodes

    # Cleanup
    for node in nodes:
        await node.shutdown()


@pytest.mark.asyncio
async def test_exclusive_lock(lock_cluster):
    """Test exclusive lock acquisition"""
    nodes = lock_cluster

    # Find leader
    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Acquire exclusive lock
    success = await leader.acquire_lock(
        lock_id="lock1",
        resource_id="resource1",
        lock_type=LockType.EXCLUSIVE,
        holder_id="client1",
    )

    assert success, "Failed to acquire exclusive lock"

    # Wait for replication
    await asyncio.sleep(0.5)

    # Verify lock exists on all nodes
    for node in nodes:
        locks = node.get_locks("resource1")
        assert len(locks) == 1
        assert locks[0].lock_id == "lock1"
        assert locks[0].lock_type == LockType.EXCLUSIVE


@pytest.mark.asyncio
async def test_shared_locks(lock_cluster):
    """Test multiple shared locks"""
    nodes = lock_cluster

    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Acquire first shared lock
    success1 = await leader.acquire_lock(
        lock_id="lock1",
        resource_id="resource1",
        lock_type=LockType.SHARED,
        holder_id="client1",
    )

    # Acquire second shared lock on same resource
    success2 = await leader.acquire_lock(
        lock_id="lock2",
        resource_id="resource1",
        lock_type=LockType.SHARED,
        holder_id="client2",
    )

    assert success1 and success2, "Failed to acquire shared locks"

    await asyncio.sleep(0.5)

    # Both locks should exist
    for node in nodes:
        locks = node.get_locks("resource1")
        assert len(locks) == 2
        assert all(lock.lock_type == LockType.SHARED for lock in locks)


@pytest.mark.asyncio
async def test_exclusive_blocks_shared(lock_cluster):
    """Test that exclusive lock blocks shared lock"""
    nodes = lock_cluster

    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Acquire exclusive lock
    success1 = await leader.acquire_lock(
        lock_id="lock1",
        resource_id="resource1",
        lock_type=LockType.EXCLUSIVE,
        holder_id="client1",
    )

    assert success1
    await asyncio.sleep(0.5)

    # Try to acquire shared lock - should fail
    success2 = await leader.acquire_lock(
        lock_id="lock2",
        resource_id="resource1",
        lock_type=LockType.SHARED,
        holder_id="client2",
    )

    assert not success2, "Shared lock should be blocked by exclusive lock"


@pytest.mark.asyncio
async def test_lock_release(lock_cluster):
    """Test lock release"""
    nodes = lock_cluster

    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Acquire lock
    await leader.acquire_lock(
        lock_id="lock1",
        resource_id="resource1",
        lock_type=LockType.EXCLUSIVE,
        holder_id="client1",
    )

    await asyncio.sleep(0.5)
    assert leader.has_lock("lock1", "resource1")

    # Release lock
    success = await leader.release_lock("lock1", "resource1")
    assert success

    await asyncio.sleep(0.5)

    # Lock should be gone
    for node in nodes:
        assert not node.has_lock("lock1", "resource1")


@pytest.mark.asyncio
async def test_lock_timeout(lock_cluster):
    """Test lock expiration"""
    nodes = lock_cluster

    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Acquire lock with 1 second timeout
    await leader.acquire_lock(
        lock_id="lock1",
        resource_id="resource1",
        lock_type=LockType.EXCLUSIVE,
        holder_id="client1",
        timeout=1.0,
    )

    await asyncio.sleep(0.5)
    assert leader.has_lock("lock1", "resource1")

    # Wait for expiration
    await asyncio.sleep(1.0)

    # Trigger cleanup
    await leader.cleanup_expired_locks()
    await asyncio.sleep(0.5)

    # Lock should be expired and cleaned
    locks = leader.get_locks("resource1")
    assert len(locks) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])


@pytest.mark.asyncio
async def test_deadlock_detection(lock_cluster):
    """Test deadlock detection and resolution"""
    nodes = lock_cluster

    leader = next((n for n in nodes if n.is_leader()), None)
    assert leader is not None

    # Client A acquires lock on resource1
    await leader.acquire_lock(
        lock_id="lockA1",
        resource_id="resource1",
        lock_type=LockType.EXCLUSIVE,
        holder_id="clientA",
    )

    # Client B acquires lock on resource2
    await leader.acquire_lock(
        lock_id="lockB2",
        resource_id="resource2",
        lock_type=LockType.EXCLUSIVE,
        holder_id="clientB",
    )

    await asyncio.sleep(0.5)

    # Simulate: Client A wants resource2 (will wait - blocked by B)
    leader.deadlock_detector.add_wait("clientA", "resource2", {"clientB"})

    # Simulate: Client B wants resource1 (will wait - blocked by A)
    leader.deadlock_detector.add_wait("clientB", "resource1", {"clientA"})

    # Update resource holders
    leader.deadlock_detector.update_resource_holders("resource1", {"clientA"})
    leader.deadlock_detector.update_resource_holders("resource2", {"clientB"})

    # Detect deadlock
    cycle = leader.deadlock_detector.detect_deadlock()

    assert cycle is not None, "Deadlock should be detected"
    assert "clientA" in cycle and "clientB" in cycle

    print(f"Detected deadlock cycle: {cycle}")

    # Test victim selection
    victim = leader.deadlock_detector.get_victim(cycle)
    assert victim in ["clientA", "clientB"]

    print(f"Selected victim: {victim}")
