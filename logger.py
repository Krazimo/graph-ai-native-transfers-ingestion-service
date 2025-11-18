"""
Logging configuration module for JSON structured logging.

This module sets up JSON logging with CloudWatch-compatible format.
All logs include message, level, and timestamp fields, with support
for additional contextual fields.
"""

import sys
import logging
from datetime import datetime
from pythonjsonlogger import jsonlogger
import pytz


class ISTFormatter(jsonlogger.JsonFormatter):
    """Custom formatter that converts timestamps to IST and highlights messages."""

    # ANSI color codes for highlighting
    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
        "RESET": "\033[0m",  # Reset
        "MESSAGE_HIGHLIGHT": "\033[1m\033[94m",  # Bright blue, bold
        "KEY": "\033[36m",  # Cyan for keys
        "VALUE_ID": "\033[95m",  # Magenta for all values
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ist_tz = pytz.timezone("Asia/Kolkata")

    def formatTime(self, record, datefmt=None):
        """Convert UTC time to IST and format in readable format."""
        # Get the timestamp from record.created (always available)
        if hasattr(record, "created"):
            # record.created is in seconds since epoch (UTC)
            utc_dt = datetime.utcfromtimestamp(record.created)
        else:
            # Fallback to current time
            utc_dt = datetime.utcnow()

        # Make it timezone-aware (UTC)
        if utc_dt.tzinfo is None:
            utc_dt = pytz.utc.localize(utc_dt)

        # Convert to IST
        ist_dt = utc_dt.astimezone(self.ist_tz)

        # Format in readable format: "2025-11-15 20:25:40 IST"
        return ist_dt.strftime("%Y-%m-%d %H:%M:%S IST")

    def _get_value_color(self, key, value):
        """Return color for values - simple magenta for all."""
        return self.COLORS["VALUE_ID"]  # Magenta for all values

    def _format_structured_fields(self, message, extra_fields):
        """Format message with extra fields in a structured way."""
        if not extra_fields:
            return message

        reset = self.COLORS["RESET"]
        key_color = self.COLORS["KEY"]

        # Format as structured output
        lines = [f"  {message}"]
        for key, value in extra_fields.items():
            # Skip timestamp since we already have log time
            if key == "timestamp":
                continue
            # Format value with color
            display_value = str(value)
            value_color = self._get_value_color(key, value)
            lines.append(
                f"    {key_color}{key}{reset}: {value_color}{display_value}{reset}"
            )
        return "\n".join(lines)

    def format(self, record):
        """Format the log record with IST timezone and highlighted message."""
        # Format the base JSON
        log_dict = {
            "asctime": self.formatTime(record),
            "levelname": record.levelname,
            "message": record.getMessage(),
        }

        # Add extra fields if present
        if hasattr(record, "__dict__"):
            for key, value in record.__dict__.items():
                if key not in [
                    "name",
                    "msg",
                    "args",
                    "created",
                    "filename",
                    "funcName",
                    "levelname",
                    "levelno",
                    "lineno",
                    "module",
                    "msecs",
                    "message",
                    "pathname",
                    "process",
                    "processName",
                    "relativeCreated",
                    "thread",
                    "threadName",
                    "exc_info",
                    "exc_text",
                    "stack_info",
                    "asctime",
                ]:
                    log_dict[key] = value

        # Get color for level
        level_color = self.COLORS.get(record.levelname, self.COLORS["RESET"])
        reset = self.COLORS["RESET"]
        msg_highlight = self.COLORS["MESSAGE_HIGHLIGHT"]

        # Extract extra fields (from extra parameter in logging calls)
        extra_fields = {
            k: v
            for k, v in log_dict.items()
            if k not in ["asctime", "levelname", "message"]
        }

        # Format message with extra fields in structured way
        structured_msg = self._format_structured_fields(
            log_dict["message"], extra_fields
        )

        # Create formatted output with colors
        # Format: [IST_TIME] [LEVEL] \n  MESSAGE (structured)
        formatted = (
            f"{log_dict['asctime']} "
            f"{level_color}[{log_dict['levelname']}]{reset}\n"
            f"{msg_highlight}{structured_msg}{reset}"
        )

        return formatted


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

    # Disable propagation to prevent duplicate messages from parent loggers
    logger.propagate = False

    # Create console handler (stdout for Docker/CloudWatch)
    handler = logging.StreamHandler(sys.stdout)  # Changed to stdout
    handler.setLevel(level)

    # Configure custom formatter with IST timezone and highlighting
    formatter = ISTFormatter(
        fmt="%(asctime)s %(levelname)s %(message)s",  # Removed %(name)s
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
