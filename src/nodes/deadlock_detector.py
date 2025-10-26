from dataclasses import dataclass
from datetime import datetime

from src.utils.logger import NodeLogger


@dataclass
class WaitFor:
    """Represents a wait-for relationship"""

    waiting_client: str
    waiting_for_resource: str
    blocking_clients: set[str]
    timestamp: float


class DeadlockDetector:
    """Detects deadlocks using wait-for graph cycle detection"""

    def __init__(self):
        # Track who is waiting for what
        # client_id -> WaitFor
        self.wait_map: dict[str, WaitFor] = {}

        # Track resource holders
        # resource_id -> set of client_ids
        self.resource_holders: dict[str, set[str]] = {}

        self.logger: NodeLogger | None = None  # Will be set by LockManager

    def add_wait(self, client_id: str, resource_id: str, blocking_clients: set[str]):
        """Record that a client is waiting for a resource"""
        self.wait_map[client_id] = WaitFor(
            waiting_client=client_id,
            waiting_for_resource=resource_id,
            blocking_clients=blocking_clients,
            timestamp=datetime.now().timestamp(),
        )

        if self.logger:
            self.logger.debug(
                f"Client {client_id} waiting for {resource_id}, blocked by {blocking_clients}"
            )

    def remove_wait(self, client_id: str):
        """Remove a wait entry when lock is acquired or request abandoned"""
        if client_id in self.wait_map:
            del self.wait_map[client_id]

    def update_resource_holders(self, resource_id: str, holders: set[str]):
        """Update who currently holds locks on a resource"""
        if holders:
            self.resource_holders[resource_id] = holders
        else:
            _ = self.resource_holders.pop(resource_id, None)

    def detect_deadlock(self) -> list[str] | None:
        """
        Detect deadlock by finding cycles in the wait-for graph.
        Returns the cycle (list of client IDs) if found, None otherwise.
        """
        # Build adjacency list for wait-for graph
        # client -> set of clients it's waiting for
        graph: dict[str, set[str]] = {}

        for client_id, wait_info in self.wait_map.items():
            if client_id not in graph:
                graph[client_id] = set()

            # This client is waiting for whoever holds the resource
            graph[client_id].update(wait_info.blocking_clients)

        # DFS-based cycle detection
        visited: set[str] = set()
        rec_stack: set[str] = set()
        parent: dict[str, str | None] = {}

        def dfs(node: str, path: list[str]) -> list[str] | None:
            """DFS with cycle detection"""
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            # Check all nodes this one is waiting for
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    parent[neighbor] = node
                    cycle = dfs(neighbor, path.copy())
                    if cycle:
                        return cycle
                elif neighbor in rec_stack:
                    # Found a cycle
                    # Extract the cycle from path
                    cycle_start_idx = path.index(neighbor)
                    cycle = path[cycle_start_idx:] + [neighbor]
                    return cycle

            rec_stack.remove(node)
            return None

        # Check each unvisited node
        for node in graph:
            if node not in visited:
                cycle = dfs(node, [])
                if cycle:
                    if self.logger:
                        cycle_str = " -> ".join([str(c) for c in cycle])
                        self.logger.warning(f"Deadlock detected: {cycle_str}")
                    return cycle

        return None

    def get_victim(self, cycle: list[str]) -> str:
        """
        Choose a victim to abort in the deadlock cycle.
        Strategy: abort the youngest transaction (most recent wait).
        """
        if not cycle:
            return ""

        # Find the client with the most recent wait timestamp
        youngest = None
        youngest_time = 0.0

        for client_id in cycle:
            if client_id in self.wait_map:
                wait_time = self.wait_map[client_id].timestamp
                if youngest is None or wait_time > youngest_time:
                    youngest = client_id
                    youngest_time = wait_time

        return youngest or cycle[0]

    def clear(self):
        """Clear all deadlock detection state"""
        self.wait_map.clear()
        self.resource_holders.clear()
