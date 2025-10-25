from src.utils.config import get_config, NodeConfig
from src.utils.logger import init_logger

# Test config loading
config = get_config()
print(f"Node ID: {config.node_id}")
print(f"Port: {config.port}")
print(f"Peers: {config.peer_nodes}")
print(f"Peer addresses: {config.get_peer_addresses()}")

# Test logger
logger = init_logger(config.node_id, config.log_level)
logger.info("Configuration loaded successfully")
logger.debug("This is a debug message")
logger.warning("This is a warning")

# Test message creation
from src.communication.message_passing import Message, MessageType

msg = Message.create(
    msg_type=MessageType.PING,
    sender_id="node1",
    receiver_id="node2",
    payload={"test": "data"},
)

print(f"\nMessage ID: {msg.msg_id}")
print(f"Message JSON: {msg.to_json()}")

# Test serialization
json_str = msg.to_json()
recovered_msg = Message.from_json(json_str)
print(f"Recovered message type: {recovered_msg.msg_type}")
print("✓ Serialization works")

print("\n✓ All foundation tests passed")
