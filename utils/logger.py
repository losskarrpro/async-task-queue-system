import logging
import sys
from enum import Enum
from typing import Optional, Union


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class CustomFormatter(logging.Formatter):
    """Custom formatter with colors and detailed format"""
    grey = "\x1b[38;21m"
    blue = "\x1b[38;5;39m"
    yellow = "\x1b[38;5;226m"
    red = "\x1b[38;5;196m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    def __init__(self, fmt: Optional[str] = None):
        super().__init__()
        self.fmt = fmt or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        self.FORMATS = {
            logging.DEBUG: self.grey + self.fmt + self.reset,
            logging.INFO: self.blue + self.fmt + self.reset,
            logging.WARNING: self.yellow + self.fmt + self.reset,
            logging.ERROR: self.red + self.fmt + self.reset,
            logging.CRITICAL: self.bold_red + self.fmt + self.reset,
        }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


def setup_logging(
    level: Union[str, LogLevel] = LogLevel.INFO,
    format_string: Optional[str] = None,
    stream=None,
):
    """Configure global logging settings"""
    if isinstance(level, LogLevel):
        level = level.value
    
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Remove existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create console handler
    if stream is None:
        stream = sys.stdout
    console_handler = logging.StreamHandler(stream)
    console_handler.setLevel(level)
    
    # Create formatter
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    if sys.stdout.isatty():
        formatter = CustomFormatter(format_string)
    else:
        formatter = logging.Formatter(format_string)
    
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def get_logger(name: str, level: Optional[Union[str, LogLevel]] = None) -> logging.Logger:
    """Get a logger with the given name"""
    logger = logging.getLogger(name)
    
    if level is not None:
        if isinstance(level, LogLevel):
            level = level.value
        logger.setLevel(level)
    
    # If logger has no handlers, add a null handler to avoid "No handlers" warning
    if not logger.handlers:
        logger.addHandler(logging.NullHandler())
    
    return logger
