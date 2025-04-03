#!/usr/bin/env python3
"""
Universal logger module for Python scripts using ConfigParser.
Features:
- Uses script filename as log file name
- Adds timestamp to log filename
- Rotates logs based on file size
- Configurable via config.cfg file
- Optional console output
"""

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Optional

# Import the config parser utility
from config_parser_util import get_config_value, get_config_int, get_config_bool

def get_script_name() -> str:
    """
    Get the name of the calling script without extension.
    
    Returns:
        Script name without extension
    """
    main_script = sys.argv[0]
    return os.path.splitext(os.path.basename(main_script))[0]

def setup_logger(
    logger_name: Optional[str] = None,
    log_level: Optional[str] = None,
    console_output: Optional[bool] = None
) -> logging.Logger:
    """
    Set up a logger with file rotation and optional console output.
    
    Args:
        logger_name: Name for the logger. If None, uses the script filename.
        log_level: Override log level from config.
        console_output: Override console output setting from config.
        
    Returns:
        Configured logger instance
    """
    # Get logger name (script name by default)
    if logger_name is None:
        logger_name = get_script_name()
    
    # Create logger instance
    logger = logging.getLogger(logger_name)
    
    # Get config values
    config_log_level = get_config_value("log_setup", "log_level", "INFO")
    config_max_size = get_config_int("log_setup", "max_log_size_mb", 20)
    config_log_dir = get_config_value("log_setup", "log_dir", "../logs")
    config_console = get_config_bool("log_setup", "console_output", True)
    
    # Set log level (override config if provided)
    level_str = log_level if log_level is not None else config_log_level
    level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers = []
    
    # Create log directory if it doesn't exist
    log_dir = os.path.abspath(os.path.join(
        os.path.dirname(os.path.abspath(sys.argv[0])),
        config_log_dir
    ))
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file path with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{logger_name}_{timestamp}.log")
    
    # Set up file handler with rotation
    max_bytes = config_max_size * 1024 * 1024  # Convert MB to bytes
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=5,
        encoding='utf-8'
    )
    
    # Set formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Add console output if enabled
    use_console = console_output if console_output is not None else config_console
    if use_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    logger.info(f"Logger initialized with level {level_str}, log file: {log_file}")
    return logger

# Global root logger for centralized logging
_root_logger = None

def get_root_logger():
    """
    Get the root logger that's shared across all modules.
    Initializes it if it doesn't exist yet.
    
    Returns:
        Root logger instance
    """
    global _root_logger
    if _root_logger is None:
        _root_logger = setup_logger(logger_name=get_script_name())
    return _root_logger

def get_module_logger(module_name: str):
    """
    Get a module-specific logger that shares the same handlers as the root logger.
    
    Args:
        module_name: Name of the module requesting a logger
        
    Returns:
        Module-specific logger
    """
    # Ensure root logger is initialized
    root_logger = get_root_logger()
    
    # Create a module logger
    logger = logging.getLogger(module_name)
    
    # Use the same level as the root logger
    logger.setLevel(root_logger.level)
    
    # Don't propagate to avoid duplicate logs
    logger.propagate = False
    
    # Use the same handlers as the root logger
    if not logger.handlers:
        for handler in root_logger.handlers:
            logger.addHandler(handler)
    
    return logger

if __name__ == "__main__":
    # Example usage
    logger = setup_logger()
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    
    # Example of module loggers
    module1_logger = get_module_logger("module1")
    module1_logger.info("This is a log from module1")
    
    module2_logger = get_module_logger("module2")
    module2_logger.info("This is a log from module2")
    
    print("\nLogger configuration created. Example log file created.")
    print("To use this logger in your scripts, import it with:")
    print("from logger_module import get_module_logger")
    print("logger = get_module_logger(__name__)")
