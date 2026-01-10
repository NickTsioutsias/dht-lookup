"""
Logging configuration for the DHT project.

Provides consistent logging across all modules.

Usage:
    from src.common.logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("This is an info message")
    logger.debug("This is a debug message")
"""

import logging
import sys
from typing import Optional

# Track if logging has been set up
_logging_initialized = False


def setup_logging(
    level: Optional[str] = None,
    log_to_file: Optional[bool] = None,
    log_file_path: Optional[str] = None
) -> None:
    """
    Initialize logging configuration for the project.
    
    This function should be called once at application startup.
    Subsequent calls will be ignored.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
               If None, uses config.LOG_LEVEL.
        log_to_file: Whether to also log to a file.
                     If None, uses config.LOG_TO_FILE.
        log_file_path: Path to log file.
                       If None, uses config.LOG_FILE_PATH.
    """
    global _logging_initialized
    
    if _logging_initialized:
        return
    
    # Import config here to avoid circular imports
    import config
    
    # Use provided values or fall back to config
    level = level or config.LOG_LEVEL
    log_to_file = log_to_file if log_to_file is not None else config.LOG_TO_FILE
    log_file_path = log_file_path or config.LOG_FILE_PATH
    
    # Convert string level to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Remove existing handlers (prevents duplicate logs)
    root_logger.handlers.clear()
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # File handler (optional)
    if log_to_file:
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    _logging_initialized = True


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for the given module name.
    
    Automatically initializes logging if not already done.
    
    Args:
        name: Name for the logger, typically __name__ of the calling module.
    
    Returns:
        Configured logger instance.
    
    Usage:
        logger = get_logger(__name__)
        logger.info("Message here")
    """
    # Ensure logging is set up
    if not _logging_initialized:
        setup_logging()
    
    return logging.getLogger(name)
