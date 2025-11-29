"""
Structured logging configuration.
Sets up JSON-formatted logs with optional redaction patterns.
"""

import logging
import json
from pathlib import Path
from datetime import datetime
from pythonjsonlogger import jsonlogger
import sys


class RedactingFilter(logging.Filter):
    """Filter that redacts sensitive information from logs."""
    
    REDACT_PATTERNS = [
        'password',
        'token',
        'secret',
        'api_key',
        'ssn',
        'credit_card',
    ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Redact sensitive fields from log record."""
        if hasattr(record, 'msg') and isinstance(record.msg, str):
            for pattern in self.REDACT_PATTERNS:
                if pattern.lower() in record.msg.lower():
                    record.msg = f"[REDACTED: {pattern}]"
        return True


def setup_logging(
    log_dir: Path,
    log_level: str = "INFO",
    json_format: bool = True,
    console_output: bool = True,
) -> None:
    """
    Configure structured logging.
    
    Args:
        log_dir: Directory to write log files
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        json_format: Use JSON-formatted logs if True
        console_output: Also output to console if True
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))
    
    # Remove any existing handlers
    root_logger.handlers.clear()
    
    # File handler with JSON formatting
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"automation_{timestamp}.log"
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(getattr(logging, log_level))
    
    redact_filter = RedactingFilter()
    file_handler.addFilter(redact_filter)
    
    if json_format:
        formatter = jsonlogger.JsonFormatter(
            '%(timestamp)s %(level)s %(name)s %(message)s',
            timestamp=True
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler (optional)
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, log_level))
        console_handler.addFilter(redact_filter)
        
        console_formatter = logging.Formatter(
            '%(levelname)s - %(name)s - %(message)s'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
    
    root_logger.info(f"Logging initialized: {log_file}")


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance with a given name."""
    return logging.getLogger(name)
