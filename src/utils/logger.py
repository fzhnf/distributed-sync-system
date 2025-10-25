import logging
import sys


class NodeLogger:
    """Structured logger for distributed nodes"""

    def __init__(self, node_id: str, log_level: str = "INFO"):
        self.node_id = node_id
        self.logger = logging.getLogger(f"node.{node_id}")
        self.logger.setLevel(getattr(logging, log_level.upper()))

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level.upper()))

        # File handler
        file_handler = logging.FileHandler(f"logs/{node_id}.log")
        file_handler.setLevel(logging.DEBUG)

        # Formatter with node ID and microsecond timestamp
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d [%(node_id)s] %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def _log(self, level: int, msg: str, **kwargs):
        """Internal log method that adds node_id to extra"""
        extra = {"node_id": self.node_id}
        extra.update(kwargs)
        self.logger.log(level, msg, extra=extra)

    def debug(self, msg: str, **kwargs):
        self._log(logging.DEBUG, msg, **kwargs)

    def info(self, msg: str, **kwargs):
        self._log(logging.INFO, msg, **kwargs)

    def warning(self, msg: str, **kwargs):
        self._log(logging.WARNING, msg, **kwargs)

    def error(self, msg: str, **kwargs):
        self._log(logging.ERROR, msg, **kwargs)

    def critical(self, msg: str, **kwargs):
        self._log(logging.CRITICAL, msg, **kwargs)


# Global logger instance
_logger: NodeLogger | None = None


def get_logger() -> NodeLogger:
    """Get the global logger instance"""
    global _logger
    if _logger is None:
        from .config import get_config

        config = get_config()
        _logger = NodeLogger(config.node_id, config.log_level)
    return _logger


def init_logger(node_id: str, log_level: str = "INFO") -> NodeLogger:
    """Initialize logger with specific parameters"""
    global _logger
    _logger = NodeLogger(node_id, log_level)
    return _logger
