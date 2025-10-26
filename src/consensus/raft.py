# pyright: reportUnusedParameter=false


import asyncio
import random
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, TypedDict, cast, override

from ..communication.message_passing import Message, MessageType
from ..nodes.base_node import BaseNode
from ..utils.config import NodeConfig


class NodeState(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class _RequestVotePayload(TypedDict):
    candidate_id: str
    last_log_index: int
    last_log_term: int


class _AppendEntriesPayload(TypedDict):
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict[str, object]]
    leader_commit: int


def _validate_request_vote(payload: dict[str, object]) -> _RequestVotePayload:
    return {
        "candidate_id": _ensure_str(payload["candidate_id"]),
        "last_log_index": _ensure_int(payload["last_log_index"]),
        "last_log_term": _ensure_int(payload["last_log_term"]),
    }


def _validate_append_entries(payload: dict[str, object]) -> _AppendEntriesPayload:
    return {
        "leader_id": _ensure_str(payload["leader_id"]),
        "prev_log_index": _ensure_int(payload["prev_log_index"]),
        "prev_log_term": _ensure_int(payload["prev_log_term"]),
        "entries": [
            _ensure_dict_str_object(e) for e in _ensure_list(payload["entries"])
        ],
        "leader_commit": _ensure_int(payload["leader_commit"]),
    }


def _ensure_str(obj: object) -> str:
    if isinstance(obj, str):
        return obj
    raise TypeError(f"expected str, got {type(obj)}")


def _ensure_int(obj: object) -> int:
    if isinstance(obj, int):
        return obj
    raise TypeError(f"expected int, got {type(obj)}")


def _ensure_list(obj: object) -> list[dict[str, object]]:
    if isinstance(obj, list) and all(isinstance(x, dict) for x in obj):
        return cast(list[dict[str, object]], obj)
    raise TypeError(f"expected list[dict[str, object]], got {type(obj)}")


def _ensure_dict_str_object(obj: object) -> dict[str, object]:
    if isinstance(obj, dict) and all(isinstance(k, str) for k in obj):
        return cast(dict[str, object], obj)
    raise TypeError("expected dict[str, object]")


@dataclass
class LogEntry:
    """Raft log entry"""

    term: int
    index: int
    command: dict[str, object]
    timestamp: float = field(default_factory=lambda: datetime.now().timestamp())


class RaftNode(BaseNode):
    """Raft consensus node implementation"""

    def __init__(self, config: NodeConfig | None = None):
        super().__init__(config)

        # Persistent state (should be saved to disk in production)
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        # Volatile state on all servers
        self.commit_index: int = -1
        self.last_applied: int = -1

        # Volatile state on leaders (reinitialized after election)
        self.next_index: dict[str, int] = {}  # peer -> next log index to send
        self.match_index: dict[str, int] = {}  # peer -> highest log index replicated

        # Raft specific state
        self.state: NodeState = NodeState.FOLLOWER
        self.leader_id: str | None = None
        self.election_timer: asyncio.Task[None] | None = None  # << None
        self.heartbeat_timer: asyncio.Task[None] | None = None  # << None

        # Vote tracking
        self.votes_received: set[str] = set()

        # State machine (for lock manager, will be overridden)
        self.state_machine: dict[str, str] = {}

        # Register Raft message handlers
        self._register_raft_handlers()

        self.logger.info(f"RaftNode initialized in {self.state.value} state")

    def _register_raft_handlers(self):
        """Register handlers for Raft RPC messages"""
        self.register_handler(MessageType.REQUEST_VOTE, self._handle_request_vote)
        self.register_handler(MessageType.APPEND_ENTRIES, self._handle_append_entries)

    @override
    async def start(self):
        """Start Raft node"""
        await super().start()

        # Start election timer
        self._reset_election_timer()

        self.logger.info(f"Raft node started, peers: {self.peers}")

    def _reset_election_timer(self):
        """Reset election timeout with random duration"""
        if self.election_timer and not self.election_timer.done():
            _ = self.election_timer.cancel()

        # Random timeout between min and max
        timeout = (
            random.randint(
                self.config.election_timeout_min, self.config.election_timeout_max
            )
            / 1000.0
        )  # Convert to seconds

        self.election_timer = asyncio.create_task(self._election_timeout(timeout))

    async def _election_timeout(self, timeout: float):
        """Election timeout handler"""
        try:
            await asyncio.sleep(timeout)

            # Timeout elapsed, start election if not leader
            if self.state != NodeState.LEADER:
                await self._start_election()
        except asyncio.CancelledError:
            pass

    async def _start_election(self):
        """Start leader election - non-blocking version"""
        self.logger.info(f"Starting election for term {self.current_term + 1}")

        # Transition to candidate
        self.state = NodeState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}

        # Reset election timer
        self._reset_election_timer()

        # Send RequestVote RPCs to all peers (fire and forget, collect responses async)
        last_log_index = len(self.log) - 1 if self.log else -1
        last_log_term = self.log[-1].term if self.log else 0

        election_term = self.current_term

        async def send_and_count_vote(peer_addr: str):
            """Send vote request and count if granted"""
            vote_request = Message.create(
                msg_type=MessageType.REQUEST_VOTE,
                sender_id=self.node_id,
                receiver_id=peer_addr,
                payload={
                    "candidate_id": self.node_id,
                    "last_log_index": last_log_index,
                    "last_log_term": last_log_term,
                },
                term=election_term,
            )

            response = await self.send_message(peer_addr, vote_request, retry_count=1)

            # Only count if we're still a candidate in the same term
            if (
                self.state == NodeState.CANDIDATE
                and self.current_term == election_term
                and response
                and response.msg_type == MessageType.REQUEST_VOTE_RESPONSE
                and response.payload.get("vote_granted")
            ):
                self.votes_received.add(response.sender_id)
                self.logger.info(f"Received vote from {response.sender_id}")

                # Check if we won
                majority = (len(self.peers) + 1) // 2 + 1
                if (
                    len(self.votes_received) >= majority
                    and self.state == NodeState.CANDIDATE
                ):
                    self.logger.info(
                        f"Won election with {len(self.votes_received)} votes!"
                    )
                    await self._become_leader()

        # Fire off all vote requests concurrently (don't wait)
        for peer_addr in list(self.peers):
            _ = asyncio.create_task(send_and_count_vote(peer_addr))

        self.logger.info(f"Vote requests sent to {len(self.peers)} peers")

    async def _become_leader(self):
        """Transition to leader state"""
        self.logger.info(f"Became leader for term {self.current_term}")

        self.state = NodeState.LEADER
        self.leader_id = self.node_id

        # Cancel election timer
        if self.election_timer and not self.election_timer.done():
            _ = self.election_timer.cancel()

        # Initialize leader state
        last_log_index = len(self.log)
        for peer in self.peers:
            self.next_index[peer] = last_log_index
            self.match_index[peer] = 0

        # Start sending heartbeats
        self._start_heartbeat_timer()

        # Send initial empty AppendEntries (heartbeat)
        await self._send_append_entries()

    def _start_heartbeat_timer(self):
        """Start periodic heartbeat sending"""
        if self.heartbeat_timer and not self.heartbeat_timer.done():
            _ = self.heartbeat_timer.cancel()

        self.heartbeat_timer = asyncio.create_task(self._heartbeat_loop())

    @override
    async def _heartbeat_loop(self):
        """Send periodic heartbeats as leader"""
        while self.running and self.state == NodeState.LEADER:
            await self._send_append_entries()
            await asyncio.sleep(self.config.heartbeat_interval / 1000.0)

    async def _send_append_entries(self):
        """Send AppendEntries RPCs to all peers"""
        if self.state != NodeState.LEADER:
            return

        for peer_addr in list(self.peers):
            # Determine which entries to send
            next_idx = self.next_index.get(peer_addr, len(self.log))

            prev_log_index = next_idx - 1
            prev_log_term = (
                self.log[prev_log_index].term
                if prev_log_index >= 0 and prev_log_index < len(self.log)
                else 0
            )

            # Get entries to send
            entries = []
            if next_idx < len(self.log):
                entries = [
                    {"term": entry.term, "index": entry.index, "command": entry.command}
                    for entry in self.log[next_idx:]
                ]

            append_msg = Message.create(
                msg_type=MessageType.APPEND_ENTRIES,
                sender_id=self.node_id,
                receiver_id=peer_addr,
                payload={
                    "leader_id": self.node_id,
                    "prev_log_index": prev_log_index,
                    "prev_log_term": prev_log_term,
                    "entries": entries,
                    "leader_commit": self.commit_index,
                },
                term=self.current_term,
            )

            # Send asynchronously
            _ = asyncio.create_task(
                self._send_append_entries_to_peer(peer_addr, append_msg, next_idx)
            )

    async def _send_append_entries_to_peer(
        self, peer_addr: str, message: Message, next_idx: int
    ):
        """Send AppendEntries to a specific peer and handle response"""
        response = await self.send_message(peer_addr, message, retry_count=1)

        if not response:
            return

        # Check if we're still leader
        if self.state != NodeState.LEADER:
            return

        # Handle response - check message type
        if (
            response.msg_type == MessageType.APPEND_ENTRIES_RESPONSE
            and response.term
            and response.term > self.current_term
        ):
            # Step down if we see higher term
            await self._step_down(response.term)
            return

        if (
            response.msg_type == MessageType.APPEND_ENTRIES_RESPONSE
            and response.payload.get("success")
        ):
            # Update next_index and match_index
            if message.payload["entries"]:
                entries = _ensure_list(message.payload["entries"])
                last_entry = _ensure_dict_str_object(entries[-1])
                self.match_index[peer_addr] = _ensure_int(last_entry["index"])
                self.next_index[peer_addr] = self.match_index[peer_addr] + 1

            # Update commit index if majority has replicated
            await self._update_commit_index()
        elif response.msg_type == MessageType.APPEND_ENTRIES_RESPONSE:
            # Log inconsistency, decrement next_index
            self.next_index[peer_addr] = max(0, self.next_index.get(peer_addr, 0) - 1)

    async def _update_commit_index(self):
        """Update commit index based on majority replication"""
        if self.state != NodeState.LEADER:
            return

        # Find highest index replicated on majority
        for n in range(self.commit_index + 1, len(self.log)):
            if self.log[n].term != self.current_term:
                continue

            # Count replicas (including self)
            replicas = 1  # Leader has it
            for peer_addr in self.peers:
                if self.match_index.get(peer_addr, 0) >= n:
                    replicas += 1

            majority = (len(self.peers) + 1) // 2 + 1
            if replicas >= majority:
                self.commit_index = n
                await self._apply_committed_entries()

    async def _apply_committed_entries(self):
        """Apply committed entries to state machine"""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied]

            # Apply to state machine
            await self._apply_to_state_machine(entry.command)

            self.logger.debug(f"Applied entry {self.last_applied}: {entry.command}")

    async def _apply_to_state_machine(self, command: dict[str, object]) -> None:
        if command.get("type") == "set" and isinstance(command["key"], str):
            self.state_machine[command["key"]] = cast(str, command["value"])
        elif command.get("type") == "delete" and isinstance(command["key"], str):
            _ = self.state_machine.pop(command["key"])

    async def _step_down(self, new_term: int):
        """Step down to follower"""
        self.logger.info(f"Stepping down to FOLLOWER, new term: {new_term}")

        self.state = NodeState.FOLLOWER
        self.current_term = new_term
        self.voted_for = None
        self.leader_id = None

        # Cancel heartbeat timer if running
        if self.heartbeat_timer and not self.heartbeat_timer.done():
            _ = self.heartbeat_timer.cancel()

        # Reset election timer
        self._reset_election_timer()

    async def _handle_request_vote(self, msg: Message) -> dict[str, object]:
        """Handle RequestVote RPC"""
        self.logger.info(
            f"=== HANDLING RequestVote from {msg.sender_id} for term {msg.term} ==="
        )

        assert msg.term is not None

        payload = _validate_request_vote(msg.payload)
        candidate_id = payload["candidate_id"]
        last_log_index = payload["last_log_index"]
        last_log_term = payload["last_log_term"]

        self.logger.debug(
            f"Received RequestVote from {candidate_id} for term {msg.term}"
        )

        # If term is old, reject
        if msg.term < self.current_term:
            self.logger.debug(
                f"Rejecting vote: term {msg.term} < current term {self.current_term}"
            )
            return {"vote_granted": False, "term": self.current_term}

        # If term is newer OR EQUAL and we're a candidate, step down
        if msg.term >= self.current_term:
            self.logger.info(
                f"Stepping down due to term: {msg.term} >= {self.current_term}, current state: {self.state}"
            )
            await self._step_down(msg.term)

        # Check if we can vote for this candidate
        vote_granted = False

        if self.voted_for is None or self.voted_for == candidate_id:
            # Check if candidate's log is at least as up-to-date as ours
            our_last_log_index = len(self.log) - 1
            our_last_log_term = self.log[-1].term if self.log else 0

            log_ok = last_log_term > our_last_log_term or (
                last_log_term == our_last_log_term
                and last_log_index >= our_last_log_index
            )

            if log_ok:
                vote_granted = True
                self.voted_for = candidate_id
                self._reset_election_timer()

                self.logger.info(f"Granted vote to {candidate_id} in term {msg.term}")
            else:
                self.logger.debug(f"Denied vote to {candidate_id}: log not up-to-date")
        else:
            self.logger.debug(
                f"Denied vote to {candidate_id}: already voted for {self.voted_for}"
            )

        return {"vote_granted": vote_granted, "term": self.current_term}

    async def _handle_append_entries(self, msg: Message) -> dict[str, object]:
        """Handle AppendEntries RPC"""

        assert msg.term is not None

        payload = _validate_append_entries(msg.payload)
        leader_id = payload["leader_id"]
        prev_log_index = payload["prev_log_index"]
        prev_log_term = payload["prev_log_term"]
        entries = payload["entries"]
        leader_commit = payload["leader_commit"]

        self.logger.debug(
            f"Received AppendEntries from {leader_id}, term {msg.term}, entries: {len(entries)}"
        )

        # If term is old, reject
        if msg.term < self.current_term:
            return {"success": False, "term": self.current_term}

        # Valid leader, reset election timer
        self._reset_election_timer()

        # If term is newer, step down
        if msg.term > self.current_term:
            await self._step_down(msg.term)

        # Recognize leader
        if self.state == NodeState.CANDIDATE:
            self.state = NodeState.FOLLOWER
            self.logger.info(
                f"Recognized leader {leader_id}, stepping down to FOLLOWER"
            )

        self.leader_id = leader_id

        # Check log consistency
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                self.logger.debug(
                    f"Log inconsistency: prev_log_index {prev_log_index} >= log length {len(self.log)}"
                )
                return {"success": False, "term": self.current_term}
            if (
                prev_log_index < len(self.log)
                and self.log[prev_log_index].term != prev_log_term
            ):
                self.logger.debug(
                    f"Log inconsistency: term mismatch at {prev_log_index}"
                )
                return {"success": False, "term": self.current_term}

        # Append new entries
        if entries:
            # Delete conflicting entries and append new ones
            insert_index = prev_log_index + 1

            for i, entry_data in enumerate(entries):
                entry_index = insert_index + i

                if entry_index < len(self.log):
                    if self.log[entry_index].term != entry_data["term"]:
                        # Delete conflicting entry and all that follow
                        self.log = self.log[:entry_index]

                # Append entry if not already present
                if entry_index >= len(self.log):
                    entry = LogEntry(
                        term=_ensure_int(entry_data["term"]),
                        index=_ensure_int(entry_data["index"]),
                        command=_ensure_dict_str_object(entry_data["command"]),
                    )
                    self.log.append(entry)
                    self.logger.debug(f"Appended entry at index {entry_index}")

        # Update commit index
        if leader_commit > self.commit_index:
            old_commit = self.commit_index
            self.commit_index = min(leader_commit, len(self.log) - 1)
            if self.commit_index > old_commit:
                self.logger.debug(
                    f"Updated commit_index from {old_commit} to {self.commit_index}"
                )
                await self._apply_committed_entries()

        return {"success": True, "term": self.current_term}

    async def _handle_request_vote_response(self, msg: Message) -> dict[str, object]:
        """Handle RequestVote response"""
        if msg.term and msg.term > self.current_term:
            await self._step_down(msg.term)

        if self.state == NodeState.CANDIDATE and msg.payload.get("vote_granted"):
            self.votes_received.add(msg.sender_id)

            # Check if we won
            majority = (len(self.peers) + 1) // 2 + 1
            if len(self.votes_received) >= majority:
                await self._become_leader()

        return {}

    async def _handle_append_entries_response(self, msg: Message) -> dict[str, object]:
        """Handle AppendEntries response"""
        if self.state != NodeState.LEADER:
            return {}

        if msg.term and msg.term > self.current_term:
            await self._step_down(msg.term)
            return {}

        # Update replication state
        peer_addr = msg.sender_id
        if msg.payload.get("success"):
            # Update next_index and match_index
            if "entries" in msg.payload and msg.payload["entries"]:
                entries = _ensure_list(msg.payload["entries"])
                last_entry = _ensure_dict_str_object(entries[-1])
                self.match_index[peer_addr] = _ensure_int(last_entry["index"])
                self.next_index[peer_addr] = self.match_index[peer_addr] + 1
            # Update commit index if majority has replicated
            await self._update_commit_index()
        else:
            # Log inconsistency, decrement next_index
            self.next_index[peer_addr] = max(0, self.next_index.get(peer_addr, 0) - 1)

        return {}

    async def replicate_command(self, command: dict[str, Any]) -> bool:
        """Replicate a command through Raft consensus (only callable by leader)"""
        if self.state != NodeState.LEADER:
            self.logger.warning(
                f"Cannot replicate command, not leader. Current state: {self.state}"
            )
            return False

        # Append to local log
        entry = LogEntry(term=self.current_term, index=len(self.log), command=command)
        self.log.append(entry)

        self.logger.info(f"Replicating command: {command}")

        # Send AppendEntries to followers
        await self._send_append_entries()

        # Wait for replication (simplified - should use futures in production)
        max_wait = 5.0  # 5 second timeout
        start_time = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start_time < max_wait:
            if self.commit_index >= entry.index:
                self.logger.info(f"Command committed at index {entry.index}")
                return True
            await asyncio.sleep(0.01)

        self.logger.warning("Command replication timed out")
        return False

    def is_leader(self) -> bool:
        """Check if this node is the leader"""
        return self.state == NodeState.LEADER

    def get_leader_id(self) -> str | None:
        """Get the current leader ID"""
        return self.leader_id
