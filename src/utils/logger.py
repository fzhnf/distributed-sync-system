import logging
import sys
import os


class NodeLogger:
    """Structured logger for distributed nodes"""

    def __init__(self, node_id: str, log_level: str = "INFO") -> None:
        self.node_id: str = node_id
        self.logger: logging.Logger = logging.getLogger(f"node.{node_id}")

        # Clear existing handlers to avoid duplicates
        self.logger.handlers.clear()
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)
        self.logger.propagate = False

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        # Create logs directory if it doesn't exist
        os.makedirs("logs", exist_ok=True)

        # File handler
        file_handler = logging.FileHandler(f"logs/{node_id}.log")
        file_handler.setLevel(logging.DEBUG)

        # Formatter with node ID
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d [%(node_id)s] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def _log(self, level: int, msg: str, **kwargs: object) -> None:
        """Internal log method that adds node_id to extra"""
        extra: dict[str, object] = {"node_id": self.node_id}
        extra.update(kwargs)
        self.logger.log(level, msg, extra=extra)

    def debug(self, msg: str, **kwargs: object) -> None:
        self._log(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs: object) -> None:
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs: object) -> None:
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs: object) -> None:
        self._log(logging.ERROR, msg, **kwargs)

    def critical(self, msg: str, **kwargs: object) -> None:
        self._log(logging.CRITICAL, msg, **kwargs)


def init_logger(node_id: str, log_level: str = "INFO") -> NodeLogger:
    """Initialize logger - always creates new instance"""
    return NodeLogger(node_id, log_level)


# Keep for backwards compatibility but don't use global state
def get_logger() -> NodeLogger:
    """Get logger - requires config to be set"""
    from .config import get_config

    config = get_config()
    return NodeLogger(config.node_id, config.log_level)
