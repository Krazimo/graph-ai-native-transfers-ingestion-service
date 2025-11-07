"""
Logging configuration module for JSON structured logging.

This module sets up JSON logging with CloudWatch-compatible format.
All logs include message, level, and timestamp fields, with support
for additional contextual fields.
"""

import logging
from pythonjsonlogger import jsonlogger


def setup_logger(name=None, level=logging.INFO):
    """
    Configure and return a JSON logger.

    Args:
        name: Logger name (default: root logger)
        level: Logging level (default: INFO)

    Returns:
        Configured logger instance
    """
    # Get logger
    logger = logging.getLogger(name)

    # Avoid adding multiple handlers if logger already configured
    if logger.handlers:
        return logger

    # Set log level
    logger.setLevel(level)

    # Create console handler (stdout for Docker/CloudWatch)
    handler = logging.StreamHandler()
    handler.setLevel(level)

    # Configure JSON formatter
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S.%fZ"
    )
    handler.setFormatter(formatter)

    # Add handler to logger
    logger.addHandler(handler)

    return logger


class CustomLogger:
    """
    Custom logger wrapper that provides a unified log() interface.

    Usage:
        logger.log("Message", level=logging.INFO)  # Explicit level
        logger.log("Message")  # Defaults to INFO
        logger.log("Error", level=logging.ERROR)
    """

    def __init__(self, logger_instance):
        self._logger = logger_instance

    def log(self, message, level=logging.INFO, extra=None):
        """
        Log a message with the specified level.

        Args:
            message: Log message string
            level: Log level (default: logging.INFO)
            extra: Dictionary with additional context fields
        """
        self._logger.log(level, message, extra=extra)

    def debug(self, message, extra=None):
        """Log a debug message."""
        self._logger.debug(message, extra=extra)

    def info(self, message, extra=None):
        """Log an info message."""
        self._logger.info(message, extra=extra)

    def warning(self, message, extra=None):
        """Log a warning message."""
        self._logger.warning(message, extra=extra)

    def error(self, message, extra=None):
        """Log an error message."""
        self._logger.error(message, extra=extra)

    def critical(self, message, extra=None):
        """Log a critical message."""
        self._logger.critical(message, extra=extra)


# Create default logger instance
_base_logger = setup_logger()
logger = CustomLogger(_base_logger)
