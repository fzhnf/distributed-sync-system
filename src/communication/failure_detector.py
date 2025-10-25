import asyncio
from typing import Any, Callable
from datetime import datetime, timedelta


class FailureDetector:
    """Adaptive failure detector using phi-accrual algorithm"""

    def __init__(self, threshold: float = 8.0, window_size: int = 100):
        self.threshold = threshold  # Suspicion threshold
        self.window_size = window_size

        # Track heartbeat arrival times for each node
        self.heartbeat_history: dict[str, list[Any]] = {}
        self.last_heartbeat: dict[str, datetime] = {}
        self.suspected_nodes: set[str] = set()

        # Callbacks
        self.on_suspected: Callable[[str], None] = lambda _: None
        self.on_recovered: Callable[[str], None] = lambda _: None

    def heartbeat_received(self, node_id: str):
        """Record a heartbeat from a node"""
        now = datetime.now()

        if node_id not in self.heartbeat_history:
            self.heartbeat_history[node_id] = []

        # Calculate interval since last heartbeat
        if node_id in self.last_heartbeat:
            interval = (now - self.last_heartbeat[node_id]).total_seconds() * 1000  # ms
            self.heartbeat_history[node_id].append(interval)

            # Keep only recent history
            if len(self.heartbeat_history[node_id]) > self.window_size:
                self.heartbeat_history[node_id].pop(0)

        self.last_heartbeat[node_id] = now

        # Check if node was suspected but has recovered
        if node_id in self.suspected_nodes:
            self.suspected_nodes.remove(node_id)
            self.on_recovered(node_id)

    def phi_value(self, node_id: str) -> float:
        """Calculate phi accrual suspicion level for a node"""
        if node_id not in self.last_heartbeat:
            return 0.0

        if (
            node_id not in self.heartbeat_history
            or len(self.heartbeat_history[node_id]) < 2
        ):
            return 0.0

        # Calculate mean and variance of intervals
        intervals = self.heartbeat_history[node_id]
        mean = sum(intervals) / len(intervals)
        variance = sum((x - mean) ** 2 for x in intervals) / len(intervals)
        std_dev = variance**0.5

        if std_dev == 0:
            std_dev = 1.0

        # Time since last heartbeat
        now = datetime.now()
        time_since_last = (
            now - self.last_heartbeat[node_id]
        ).total_seconds() * 1000  # ms

        # Calculate phi value (simplified version)
        import math

        phi = -math.log10(max(1e-10, 1 - ((time_since_last - mean) / std_dev)))

        return max(0.0, phi)

    def is_suspected(self, node_id: str) -> bool:
        """Check if a node is suspected to have failed"""
        phi = self.phi_value(node_id)

        suspected = phi > self.threshold

        # Update suspected set and trigger callbacks
        if suspected and node_id not in self.suspected_nodes:
            self.suspected_nodes.add(node_id)
            self.on_suspected(node_id)

        return suspected

    def get_suspected_nodes(self) -> set[str]:
        """Get all currently suspected nodes"""
        return self.suspected_nodes.copy()
