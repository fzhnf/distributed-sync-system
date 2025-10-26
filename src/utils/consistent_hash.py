import hashlib
from bisect import bisect_right


class ConsistentHashRing:
    """Consistent hashing ring for distributed systems"""

    def __init__(self, virtual_nodes: int = 150):
        self.virtual_nodes: int = virtual_nodes
        self.ring: dict[int, str] = {}  # hash -> node_id
        self.sorted_keys: list[int] = []
        self.nodes: set[str] = set()
        self.node_addresses: dict[str, str] = {}  # node_id -> address

    def _hash(self, key: str) -> int:
        """Hash a key to a position on the ring"""
        return int(hashlib.sha256(key.encode()).hexdigest(), 16)

    def add_node(self, node_id: str, address: str | None = None):
        """Add a node to the ring"""
        if node_id in self.nodes:
            return

        self.nodes.add(node_id)

        # Store address mapping
        if address:
            self.node_addresses[node_id] = address
        else:
            self.node_addresses[node_id] = (
                node_id  # Use node_id as address if none provided
            )

        # Add virtual nodes
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:{i}"
            hash_value = self._hash(virtual_key)
            self.ring[hash_value] = node_id
            self.sorted_keys.append(hash_value)

        self.sorted_keys.sort()

    def remove_node(self, node_id: str):
        """Remove a node from the ring"""
        if node_id not in self.nodes:
            return

        self.nodes.remove(node_id)
        _ = self.node_addresses.pop(node_id, None)

        # Remove virtual nodes
        keys_to_remove: list[int] = []
        for hash_value, node in self.ring.items():
            if node == node_id:
                keys_to_remove.append(hash_value)

        for key in keys_to_remove:
            del self.ring[key]
            self.sorted_keys.remove(key)

    def get_node(self, key: str) -> str | None:
        """Get the node responsible for a key"""
        if not self.ring:
            return None

        hash_value = self._hash(key)

        # Find the first node clockwise from hash_value
        idx = bisect_right(self.sorted_keys, hash_value)

        # Wrap around if necessary
        if idx == len(self.sorted_keys):
            idx = 0

        return self.ring[self.sorted_keys[idx]]

    def get_node_address(self, node_id: str) -> str | None:
        """Get the address for a node_id"""
        return self.node_addresses.get(node_id)

    def get_nodes(self, key: str, count: int) -> list[str]:
        """Get multiple nodes for replication"""
        if not self.ring or count <= 0:
            return []

        hash_value = self._hash(key)
        idx = bisect_right(self.sorted_keys, hash_value)

        result: list[str] = []
        seen: set[str] = set()

        # Collect unique nodes clockwise
        for _ in range(len(self.sorted_keys)):
            if idx >= len(self.sorted_keys):
                idx = 0

            node = self.ring[self.sorted_keys[idx]]
            if node not in seen:
                result.append(node)
                seen.add(node)

                if len(result) == count:
                    break

            idx += 1

        return result

    def get_all_nodes(self) -> list[str]:
        """Get all nodes in the ring"""
        return list(self.nodes)
