"""Structured logging setup."""
import logging
from pathlib import Path
from rich.logging import RichHandler


def setup_logger(config) -> logging.Logger:
    """Setup logger with console and file handlers."""
    logger = logging.getLogger("polymarket_bot")
    logger.setLevel(getattr(logging, config.bot.log_level, logging.INFO))
    
    # Prevent duplicate handlers if called multiple times
    if logger.handlers:
        return logger
    
    # Console handler with Rich
    console_handler = RichHandler(rich_tracebacks=True, markup=True)
    console_handler.setLevel(logging.INFO)
    
    # File handler
    log_file = Path(config.bot.log_file)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
    )
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
