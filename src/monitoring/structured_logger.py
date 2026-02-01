"""Structured logging for pipeline monitoring."""

import json
import logging
import sys
from typing import Any, Dict, Optional
from datetime import datetime


class StructuredFormatter(logging.Formatter):
    """Formats log records as structured JSON."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON.

        Args:
            record: Log record.

        Returns:
            Formatted JSON string.
        """
        log_obj = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info),
            }

        # Add custom fields from extra dict
        if hasattr(record, "extra_fields"):
            log_obj.update(record.extra_fields)

        return json.dumps(log_obj)


class StructuredLogger:
    """Provides structured logging interface."""

    def __init__(self, name: str, level: str = "INFO", log_file: Optional[str] = None):
        """Initialize structured logger.

        Args:
            name: Logger name.
            level: Logging level.
            log_file: Optional file to write logs to.
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level))

        # Remove existing handlers
        self.logger.handlers = []

        # Console handler with structured formatter
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(StructuredFormatter())
        self.logger.addHandler(console_handler)

        # File handler if specified
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(StructuredFormatter())
            self.logger.addHandler(file_handler)

    def info(self, message: str, **extra: Any) -> None:
        """Log info message.

        Args:
            message: Log message.
            **extra: Additional fields to include.
        """
        self._log(logging.INFO, message, extra)

    def warning(self, message: str, **extra: Any) -> None:
        """Log warning message.

        Args:
            message: Log message.
            **extra: Additional fields to include.
        """
        self._log(logging.WARNING, message, extra)

    def error(self, message: str, **extra: Any) -> None:
        """Log error message.

        Args:
            message: Log message.
            **extra: Additional fields to include.
        """
        self._log(logging.ERROR, message, extra)

    def debug(self, message: str, **extra: Any) -> None:
        """Log debug message.

        Args:
            message: Log message.
            **extra: Additional fields to include.
        """
        self._log(logging.DEBUG, message, extra)

    def _log(self, level: int, message: str, extra: Dict[str, Any]) -> None:
        """Internal logging method.

        Args:
            level: Log level.
            message: Log message.
            extra: Additional fields.
        """
        if extra:
            # Create a custom LogRecord with extra fields
            record = self.logger.makeRecord(
                self.logger.name,
                level,
                "",
                0,
                message,
                (),
                None,
            )
            record.extra_fields = extra
            self.logger.handle(record)
        else:
            self.logger.log(level, message)
