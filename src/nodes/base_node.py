# pyright: reportUnusedParameter=false

import asyncio
from collections.abc import Awaitable
from typing import Callable, cast
from aiohttp import web, ClientSession, ClientTimeout, TCPConnector
import signal

from ..utils.config import get_config, NodeConfig
from ..communication.message_passing import Message, MessageType


class BaseNode:
    """Base class for all distributed nodes with async HTTP communication"""

    def __init__(self, config: NodeConfig | None = None):
        self.config: NodeConfig = config or get_config()
        # Create logger directly, don't use global
        from ..utils.logger import NodeLogger

        self.logger: NodeLogger = NodeLogger(self.config.node_id, self.config.log_level)
        self.node_id: str = self.config.node_id

        # Network state
        self.app: web.Application = web.Application()
        self.runner: web.AppRunner | None = None
        self.session: ClientSession | None = None

        # Peer tracking
        self.peers: set[str] = set()  # Set of "host:port" strings
        self.peer_health: dict[str, bool] = {}  # peer -> is_healthy

        # Message handlers
        self.message_handlers: dict[
            MessageType, Callable[[Message], Awaitable[dict[str, object]]]
        ] = {}

        # Shutdown flag
        self.running: bool = False

        # Setup routes
        self._setup_routes()

        # Register default handlers
        self._register_default_handlers()

        self.logger.info(
            f"BaseNode initialized: {self.node_id} on port {self.config.port}"
        )

    def _setup_routes(self):
        """Setup HTTP routes for node communication"""
        _ = self.app.router.add_post("/message", self._handle_message)
        _ = self.app.router.add_get("/health", self._handle_health)
        _ = self.app.router.add_get("/status", self._handle_status)

    def _register_default_handlers(self):
        """Register default message handlers"""

        async def handle_heartbeat(msg: Message) -> dict[str, object]:
            """Default heartbeat handler"""
            return {
                "ack": True,
                "node_id": self.node_id,
                "timestamp": asyncio.get_event_loop().time(),
            }

        async def handle_ping(msg: Message) -> dict[str, object]:
            """Default ping handler"""
            return {"pong": True, "node_id": self.node_id}

        self.register_handler(MessageType.HEARTBEAT, handle_heartbeat)
        self.register_handler(MessageType.PING, handle_ping)

    async def start(self):
        """Start the node server and background tasks"""
        self.running = True

        # Create HTTP client session
        timeout = ClientTimeout(total=10, connect=5)
        connector = TCPConnector(limit=100, limit_per_host=30)
        self.session = ClientSession(timeout=timeout, connector=connector)

        # Start web server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.config.host, self.config.port)
        await site.start()

        self.logger.info(f"Node started on {self.config.host}:{self.config.port}")

        # Discover peers
        await self._discover_peers()

        # Start background tasks
        _ = asyncio.create_task(self._heartbeat_loop())
        _ = asyncio.create_task(self._health_check_loop())

        # Setup signal handlers
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

    async def shutdown(self):
        """Graceful shutdown"""
        self.logger.info("Shutting down node...")
        self.running = False

        if self.session:
            await self.session.close()

        if self.runner:
            await self.runner.cleanup()

        self.logger.info("Node shutdown complete")

    async def _discover_peers(self):
        """Discover peer nodes from configuration"""
        for host, port in self.config.get_peer_addresses():
            peer_addr = f"{host}:{port}"
            self.peers.add(peer_addr)
            self.peer_health[peer_addr] = False  # Unknown until checked
            self.logger.info(f"Discovered peer: {peer_addr}")

    async def _handle_message(self, request: web.Request) -> web.Response:
        """Handle incoming messages from other nodes"""
        try:
            data = await request.read()
            message = Message.from_bytes(data)

            self.logger.debug(
                f"Received {message.msg_type.value} from {message.sender_id}"
            )

            # Find handler for this message type
            handler = self.message_handlers.get(message.msg_type)
            if handler:
                response_payload = await handler(message)

                # Determine response message type based on request type
                response_type = message.msg_type
                if message.msg_type == MessageType.REQUEST_VOTE:
                    response_type = MessageType.REQUEST_VOTE_RESPONSE
                elif message.msg_type == MessageType.APPEND_ENTRIES:
                    response_type = MessageType.APPEND_ENTRIES_RESPONSE

                response_msg = Message.create(
                    msg_type=response_type,
                    sender_id=self.node_id,
                    receiver_id=message.sender_id,
                    payload=response_payload,
                    term=getattr(message, "term", None),
                )
                return web.Response(body=response_msg.to_bytes(), status=200)
            else:
                self.logger.warning(f"No handler for message type: {message.msg_type}")
                return web.Response(status=501, text="Message type not implemented")

        except Exception as e:
            self.logger.error(f"Error handling message: {e}")
            return web.Response(status=500, text=str(e))

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint"""
        return web.json_response(
            {
                "node_id": self.node_id,
                "status": "healthy" if self.running else "shutting_down",
                "peers": len(self.peers),
                "healthy_peers": sum(1 for h in self.peer_health.values() if h),
            }
        )

    async def _handle_status(self, request: web.Request) -> web.Response:
        """Detailed status endpoint"""
        return web.json_response(
            {
                "node_id": self.node_id,
                "running": self.running,
                "peers": list(self.peers),
                "peer_health": self.peer_health,
                "handlers": [mt.value for mt in self.message_handlers.keys()],
            }
        )

    async def send_message(
        self, peer_addr: str, message: Message, retry_count: int = 3
    ) -> Message | None:
        assert self.session is not None, "HTTP session not initialized"
        """Send message to a peer with retries and exponential backoff"""
        url = f"http://{peer_addr}/message"

        for attempt in range(retry_count):
            try:
                async with self.session.post(url, data=message.to_bytes()) as resp:
                    if resp.status == 200:
                        response_data = await resp.read()
                        response_msg = Message.from_bytes(response_data)
                        self.logger.debug(
                            f"Sent {message.msg_type.value} to {peer_addr}, got response"
                        )
                        return response_msg
                    else:
                        self.logger.warning(
                            f"Failed to send to {peer_addr}: HTTP {resp.status}"
                        )

            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Timeout sending to {peer_addr} (attempt {attempt + 1}/{retry_count})"
                )
            except Exception as e:
                self.logger.error(f"Error sending to {peer_addr}: {e}")

            # Exponential backoff
            if attempt < retry_count - 1:
                await asyncio.sleep(cast(float, 0.1 * (2**attempt)))

        self.logger.error(
            f"Failed to send message to {peer_addr} after {retry_count} attempts"
        )
        self.peer_health[peer_addr] = False
        return None

    async def broadcast_message(self, message: Message) -> dict[str, Message | None]:
        """Broadcast message to all peers"""
        # list of coroutines with an explicit element type
        coros: list[Awaitable[Message | None]] = [
            self.send_message(peer_addr, message) for peer_addr in self.peers
        ]

        # result of gather is now fully-typed
        responses: list[Message | None | BaseException] = await asyncio.gather(
            *coros, return_exceptions=True
        )

        result: dict[str, Message | None] = {}
        for peer_addr, response in zip(self.peers, responses):
            if isinstance(response, BaseException):
                self.logger.error(f"Exception broadcasting to {peer_addr}: {response}")
                result[peer_addr] = None
            else:
                result[peer_addr] = response

        return result

    async def _heartbeat_loop(self):
        """Send periodic heartbeats to peers"""
        while self.running:
            await asyncio.sleep(self.config.heartbeat_interval / 1000.0)

            heartbeat = Message.create(
                msg_type=MessageType.HEARTBEAT,
                sender_id=self.node_id,
                receiver_id="broadcast",
                payload={"timestamp": asyncio.get_event_loop().time()},
            )

            # Send to all peers without waiting for responses
            for peer_addr in list(self.peers):
                _ = asyncio.create_task(
                    self.send_message(peer_addr, heartbeat, retry_count=1)
                )

    async def _health_check_loop(self):
        """Periodically check peer health"""
        assert self.session is not None, "HTTP session not initialized"

        while self.running:
            await asyncio.sleep(5.0)  # Check every 5 seconds

            for peer_addr in list(self.peers):
                try:
                    async with self.session.get(f"http://{peer_addr}/health") as resp:
                        if resp.status == 200:
                            self.peer_health[peer_addr] = True
                        else:
                            self.peer_health[peer_addr] = False
                except Exception:
                    self.peer_health[peer_addr] = False

            healthy_count = sum(1 for h in self.peer_health.values() if h)
            self.logger.debug(
                f"Health check: {healthy_count}/{len(self.peers)} peers healthy"
            )

    def register_handler(
        self,
        msg_type: MessageType,
        handler: Callable[[Message], Awaitable[dict[str, object]]],
    ):
        """Register a handler for a specific message type"""
        self.message_handlers[msg_type] = handler
        self.logger.info(f"Registered handler for {msg_type.value}")

    async def run_forever(self):
        """Run the node until shutdown"""
        await self.start()

        # Keep running until shutdown signal
        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        finally:
            await self.shutdown()
