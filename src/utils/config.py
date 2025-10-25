import os
from dataclasses import dataclass
import yaml


@dataclass
class NodeConfig:
    """Configuration for a single node"""

    node_id: str
    host: str
    port: int
    peer_nodes: list[str]  # Format: ["node2:8002", "node3:8003"]

    # Redis configuration
    redis_host: str
    redis_port: int

    # Raft configuration
    election_timeout_min: int  # milliseconds
    election_timeout_max: int  # milliseconds
    heartbeat_interval: int  # milliseconds

    # Queue configuration
    queue_replication_factor: int
    message_persistence_path: str

    # Cache configuration
    cache_size: int  # number of entries
    cache_eviction_policy: str  # LRU or LFU

    # General settings
    log_level: str
    metrics_port: int

    @classmethod
    def from_env(cls) -> "NodeConfig":
        """Load configuration from environment variables"""
        peer_nodes_str = os.getenv("PEER_NODES", "")
        peer_nodes = [p.strip() for p in peer_nodes_str.split(",") if p.strip()]

        return cls(
            node_id=os.getenv("NODE_ID", "node1"),
            host=os.getenv("NODE_HOST", "0.0.0.0"),
            port=int(os.getenv("NODE_PORT", "8001")),
            peer_nodes=peer_nodes,
            redis_host=os.getenv("REDIS_HOST", "localhost"),
            redis_port=int(os.getenv("REDIS_PORT", "6379")),
            election_timeout_min=int(os.getenv("RAFT_ELECTION_TIMEOUT_MIN", "150")),
            election_timeout_max=int(os.getenv("RAFT_ELECTION_TIMEOUT_MAX", "300")),
            heartbeat_interval=int(os.getenv("RAFT_HEARTBEAT_INTERVAL", "50")),
            queue_replication_factor=int(os.getenv("QUEUE_REPLICATION_FACTOR", "2")),
            message_persistence_path=os.getenv(
                "MESSAGE_PERSISTENCE_PATH", "./data/queue"
            ),
            cache_size=int(os.getenv("CACHE_SIZE", "1000")),
            cache_eviction_policy=os.getenv("CACHE_EVICTION_POLICY", "LRU"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            metrics_port=int(os.getenv("METRICS_PORT", "9090")),
        )

    @classmethod
    def from_file(cls, filepath: str) -> "NodeConfig":
        """Load configuration from YAML file"""
        with open(filepath, "r") as f:
            data = yaml.safe_load(f)

        return cls(**data)

    def get_peer_addresses(self) -> list[tuple[str, int]]:
        """Parse peer nodes into (host, port) tuples"""
        addresses = []
        for peer in self.peer_nodes:
            if ":" in peer:
                host, port = peer.split(":")
                addresses.append((host, int(port)))
        return addresses

    def validate(self) -> None:
        """Validate configuration parameters"""
        if self.port < 1024 or self.port > 65535:
            raise ValueError(f"Invalid port: {self.port}")

        if self.election_timeout_min >= self.election_timeout_max:
            raise ValueError(
                "election_timeout_min must be less than election_timeout_max"
            )

        if self.heartbeat_interval >= self.election_timeout_min:
            raise ValueError(
                "heartbeat_interval must be less than election_timeout_min"
            )

        if self.queue_replication_factor < 1:
            raise ValueError("queue_replication_factor must be at least 1")

        if self.cache_eviction_policy not in ["LRU", "LFU"]:
            raise ValueError(
                f"Invalid cache eviction policy: {self.cache_eviction_policy}"
            )


# Global config instance
_config: NodeConfig | None = None


def get_config() -> NodeConfig:
    """Get the global configuration instance"""
    global _config
    if _config is None:
        _config = NodeConfig.from_env()
        _config.validate()
    return _config


def set_config(config: NodeConfig) -> None:
    """Set the global configuration instance (useful for testing)"""
    global _config
    config.validate()
    _config = config
