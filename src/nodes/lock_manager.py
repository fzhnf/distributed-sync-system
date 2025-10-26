import asyncio
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, override

from src.nodes.deadlock_detector import DeadlockDetector

from ..consensus.raft import RaftNode
from ..utils.config import NodeConfig


class LockType(Enum):
    SHARED = "SHARED"
    EXCLUSIVE = "EXCLUSIVE"


@dataclass
class Lock:
    """Represents a lock held by a client"""

    lock_id: str
    resource_id: str
    lock_type: LockType
    holder_id: str
    acquired_at: float
    timeout: float | None = None  # seconds

    def is_expired(self) -> bool:
        """Check if lock has expired"""
        if self.timeout is None:
            return False
        return (datetime.now().timestamp() - self.acquired_at) > self.timeout


class LockManager(RaftNode):
    """Distributed lock manager using Raft consensus"""

    @override
    async def start(self):
        """Start lock manager"""
        await super().start()

        # Start deadlock detection loop
        _ = asyncio.create_task(self._deadlock_detection_loop())

    def __init__(self, config: NodeConfig | None = None):
        super().__init__(config)

        # Lock state (replicated via Raft)
        self.locks: dict[str, list[Lock]] = {}

        # Track lock requests waiting for responses
        self.pending_requests: dict[str, asyncio.Future[Any]] = {}

        # Deadlock detection
        self.deadlock_detector: DeadlockDetector = DeadlockDetector()
        self.deadlock_detector.logger = self.logger

        # Start deadlock detection loop
        self.deadlock_check_interval: float = 5.0  # Check every 5 seconds

        self.logger.info("LockManager initialized")

    @override
    async def _apply_to_state_machine(self, command: dict[str, object]) -> None:
        """Override to handle lock commands"""
        cmd_type = command.get("type")

        if cmd_type == "acquire_lock":
            await self._apply_acquire_lock(command)
        elif cmd_type == "release_lock":
            await self._apply_release_lock(command)
        elif cmd_type == "cleanup_expired":
            await self._apply_cleanup_expired(command)
        else:
            # Fall back to parent implementation
            await super()._apply_to_state_machine(command)

    async def _apply_acquire_lock(self, command: dict[str, object]):
        """Apply lock acquisition to state machine"""
        lock_id = str(command["lock_id"])
        resource_id = str(command["resource_id"])
        lock_type_str = str(command["lock_type"])
        holder_id = str(command["holder_id"])
        timeout = command.get("timeout")

        lock_type = LockType.SHARED if lock_type_str == "SHARED" else LockType.EXCLUSIVE

        # Check if lock can be granted
        can_grant = self._can_grant_lock(resource_id, lock_type)

        if can_grant:
            lock = Lock(
                lock_id=lock_id,
                resource_id=resource_id,
                lock_type=lock_type,
                holder_id=holder_id,
                acquired_at=datetime.now().timestamp(),
                timeout=float(str(timeout)) if timeout else None,
            )

            if resource_id not in self.locks:
                self.locks[resource_id] = []

            self.locks[resource_id].append(lock)
            self.logger.info(
                f"Granted {lock_type.value} lock {lock_id} on {resource_id} to {holder_id}"
            )

            # Resolve pending request if exists
            if lock_id in self.pending_requests:
                self.pending_requests[lock_id].set_result(True)
        else:
            self.logger.warning(
                f"Cannot grant {lock_type.value} lock {lock_id} on {resource_id}"
            )
            if lock_id in self.pending_requests:
                self.pending_requests[lock_id].set_result(False)

    async def _apply_release_lock(self, command: dict[str, object]):
        """Apply lock release to state machine"""
        lock_id = str(command["lock_id"])
        resource_id = str(command["resource_id"])

        if resource_id in self.locks:
            self.locks[resource_id] = [
                lock for lock in self.locks[resource_id] if lock.lock_id != lock_id
            ]

            if not self.locks[resource_id]:
                del self.locks[resource_id]

            self.logger.info(f"Released lock {lock_id} on {resource_id}")

    async def _apply_cleanup_expired(self, command: dict[str, object]):
        """Remove expired locks"""
        cleaned = 0
        for resource_id in list(self.locks.keys()):
            original_count = len(self.locks[resource_id])
            self.locks[resource_id] = [
                lock for lock in self.locks[resource_id] if not lock.is_expired()
            ]
            cleaned += original_count - len(self.locks[resource_id])

            if not self.locks[resource_id]:
                del self.locks[resource_id]

        if cleaned > 0:
            self.logger.info(f"Cleaned up {cleaned} expired locks")

    def _can_grant_lock(self, resource_id: str, lock_type: LockType) -> bool:
        """Check if a lock can be granted on a resource"""
        if resource_id not in self.locks or not self.locks[resource_id]:
            return True

        existing_locks = self.locks[resource_id]

        # Clean up expired locks first
        existing_locks = [lock for lock in existing_locks if not lock.is_expired()]

        if not existing_locks:
            return True

        # Exclusive lock cannot coexist with any other lock
        if lock_type == LockType.EXCLUSIVE:
            return False

        # Shared lock can coexist with other shared locks only
        return all(lock.lock_type == LockType.SHARED for lock in existing_locks)

    async def acquire_lock(
        self,
        lock_id: str,
        resource_id: str,
        lock_type: LockType,
        holder_id: str,
        timeout: float | None = None,
    ) -> bool:
        """Acquire a lock on a resource"""
        if not self.is_leader():
            leader_id = self.get_leader_id()
            self.logger.warning(
                f"Not leader, cannot acquire lock. Leader is {leader_id}"
            )
            return False

        # Check if lock can be granted immediately
        can_grant = self._can_grant_lock(resource_id, lock_type)

        if not can_grant:
            # Record wait for deadlock detection
            blocking_clients = {
                lock.holder_id
                for lock in self.locks.get(resource_id, [])
                if not lock.is_expired()
            }
            self.deadlock_detector.add_wait(holder_id, resource_id, blocking_clients)

        # Create command
        command = {
            "type": "acquire_lock",
            "lock_id": lock_id,
            "resource_id": resource_id,
            "lock_type": lock_type.value,
            "holder_id": holder_id,
            "timeout": timeout,
        }

        # Create future for this request
        future = asyncio.Future()
        self.pending_requests[lock_id] = future

        # Replicate through Raft
        success = await self.replicate_command(command)

        if not success:
            _ = self.pending_requests.pop(lock_id, None)
            self.deadlock_detector.remove_wait(holder_id)
            return False

        # Wait for state machine to apply
        try:
            result = await asyncio.wait_for(future, timeout=5.0)

            # Remove from wait map if lock acquired
            if result:
                self.deadlock_detector.remove_wait(holder_id)

            return result
        except asyncio.TimeoutError:
            self.logger.error(f"Timeout waiting for lock {lock_id}")
            self.deadlock_detector.remove_wait(holder_id)
            return False
        finally:
            _ = self.pending_requests.pop(lock_id, None)

    async def release_lock(self, lock_id: str, resource_id: str) -> bool:
        """Release a lock"""
        if not self.is_leader():
            leader_id = self.get_leader_id()
            self.logger.warning(
                f"Not leader, cannot release lock. Leader is {leader_id}"
            )
            return False

        command = {
            "type": "release_lock",
            "lock_id": lock_id,
            "resource_id": resource_id,
        }

        return await self.replicate_command(command)

    def get_locks(self, resource_id: str) -> list[Lock]:
        """Get all locks for a resource"""
        return self.locks.get(resource_id, []).copy()

    def has_lock(self, lock_id: str, resource_id: str) -> bool:
        """Check if a specific lock is held"""
        if resource_id not in self.locks:
            return False
        return any(lock.lock_id == lock_id for lock in self.locks[resource_id])

    async def cleanup_expired_locks(self):
        """Periodic cleanup of expired locks"""
        if not self.is_leader():
            return

        command = {"type": "cleanup_expired"}
        _ = await self.replicate_command(command)

    async def _deadlock_detection_loop(self):
        """Periodically check for deadlocks"""
        while self.running:
            await asyncio.sleep(self.deadlock_check_interval)

            if self.is_leader():
                await self._check_and_resolve_deadlocks()

    async def _check_and_resolve_deadlocks(self):
        """Check for deadlocks and resolve if found"""
        # Update deadlock detector with current lock state
        for resource_id, locks in self.locks.items():
            holders = {lock.holder_id for lock in locks if not lock.is_expired()}
            self.deadlock_detector.update_resource_holders(resource_id, holders)

        # Detect deadlock
        cycle = self.deadlock_detector.detect_deadlock()

        if cycle:
            # Found a deadlock, choose victim and abort
            victim = self.deadlock_detector.get_victim(cycle)
            self.logger.warning(f"Resolving deadlock by aborting client {victim}")

            # Abort victim's locks
            await self._abort_client_locks(victim)

            # Remove from wait map
            self.deadlock_detector.remove_wait(victim)

    async def _abort_client_locks(self, client_id: str):
        """Release all locks held by a client (for deadlock resolution)"""
        locks_to_release = []

        for resource_id, locks in self.locks.items():
            for lock in locks:
                if lock.holder_id == client_id:
                    locks_to_release.append((lock.lock_id, resource_id))

        for lock_id, resource_id in locks_to_release:
            _ = await self.release_lock(lock_id, resource_id)
