import asyncio
import pytest
import pytest_asyncio
from src.consensus.raft import RaftNode
from src.utils.config import NodeConfig
from src.communication.message_passing import Message, MessageType


@pytest_asyncio.fixture
async def two_nodes():
    """Create 2 simple Raft nodes"""
    config1 = NodeConfig(
        node_id="node1",
        host="127.0.0.1",
        port=11001,
        peer_nodes=["127.0.0.1:11002"],
        redis_host="localhost",
        redis_port=6379,
        election_timeout_min=5000,  # Very long to prevent elections
        election_timeout_max=6000,
        heartbeat_interval=50,
        queue_replication_factor=2,
        message_persistence_path="./data/test",
        cache_size=100,
        cache_eviction_policy="LRU",
        log_level="INFO",
        metrics_port=9001,
    )

    config2 = NodeConfig(
        node_id="node2",
        host="127.0.0.1",
        port=11002,
        peer_nodes=["127.0.0.1:11001"],
        redis_host="localhost",
        redis_port=6379,
        election_timeout_min=5000,
        election_timeout_max=6000,
        heartbeat_interval=50,
        queue_replication_factor=2,
        message_persistence_path="./data/test",
        cache_size=100,
        cache_eviction_policy="LRU",
        log_level="INFO",
        metrics_port=9002,
    )

    node1 = RaftNode(config1)
    node2 = RaftNode(config2)

    await node1.start()
    await node2.start()

    await asyncio.sleep(0.5)

    yield node1, node2

    await node1.shutdown()
    await node2.shutdown()


@pytest.mark.asyncio
async def test_direct_request_vote(two_nodes):
    """Test that RequestVote messages work"""
    node1, node2 = two_nodes

    # Manually create and send a RequestVote message
    vote_request = Message.create(
        msg_type=MessageType.REQUEST_VOTE,
        sender_id="node1",
        receiver_id="node2",
        payload={
            "candidate_id": "node1",
            "last_log_index": -1,
            "last_log_term": 0,
        },
        term=1,
    )

    print(f"\nSending vote request from node1 to node2...")
    response = await node1.send_message("127.0.0.1:11002", vote_request)

    print(f"Response: {response}")
    print(f"Response type: {response.msg_type if response else None}")
    print(f"Response payload: {response.payload if response else None}")

    assert response is not None, "No response received"
    assert response.msg_type == MessageType.REQUEST_VOTE_RESPONSE, (
        f"Wrong response type: {response.msg_type}"
    )
    assert "vote_granted" in response.payload, "vote_granted not in payload"

    print(f"Vote granted: {response.payload['vote_granted']}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
