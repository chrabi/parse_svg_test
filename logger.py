#!/usr/bin/env python3
"""
Universal logger module for Python scripts.
Features:
- Uses script filename as log file name
- Adds timestamp to log filename
- Rotates logs based on file size
- Configurable via config file
- Optional console output
"""

import os
import sys
import logging
import json
import time
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any, Union


def get_script_name() -> str:
    """
    Get the name of the calling script without extension.
    
    Returns:
        Script name without extension
    """
    main_script = sys.argv[0]
    return os.path.splitext(os.path.basename(main_script))[0]


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load logger configuration from a JSON file.
    If the file doesn't exist, create it with default values.
    
    Args:
        config_path: Path to the config file. If None, uses default location.
        
    Returns:
        Dictionary with logger configuration
    """
    default_config = {
        "log_level": "INFO",
        "max_log_size_mb": 20,
        "log_dir": "../logs",
        "console_output": True
    }
    
    if config_path is None:
        # Place config file in the same directory as the script
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        config_path = os.path.join(script_dir, "logger_config.json")
    
    # Create config file with defaults if it doesn't exist
    if not os.path.exists(config_path):
        os.makedirs(os.path.dirname(config_path), exist_ok=True)
        with open(config_path, 'w') as f:
            json.dump(default_config, f, indent=4)
        return default_config
    
    # Load existing config
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Ensure all required keys exist, use defaults for missing keys
        for key, value in default_config.items():
            if key not in config:
                config[key] = value
        
        return config
    except Exception as e:
        print(f"Error loading logger config: {str(e)}. Using defaults.")
        return default_config


def setup_logger(
    logger_name: Optional[str] = None,
    config_path: Optional[str] = None,
    log_level: Optional[str] = None,
    console_output: Optional[bool] = None
) -> logging.Logger:
    """
    Set up a logger with file rotation and optional console output.
    
    Args:
        logger_name: Name for the logger. If None, uses the script filename.
        config_path: Path to the configuration file. If None, uses default location.
        log_level: Override log level from config.
        console_output: Override console output setting from config.
        
    Returns:
        Configured logger instance
    """
    # Load configuration
    config = load_config(config_path)
    
    # Get logger name (script name by default)
    if logger_name is None:
        logger_name = get_script_name()
    
    # Create logger instance
    logger = logging.getLogger(logger_name)
    
    # Set log level (override config if provided)
    level_str = log_level if log_level is not None else config["log_level"]
    level = getattr(logging, level_str.upper(), logging.INFO)
    logger.setLevel(level)
    
    # Clear existing handlers
    logger.handlers = []
    
    # Create log directory if it doesn't exist
    log_dir = os.path.abspath(os.path.join(
        os.path.dirname(os.path.abspath(sys.argv[0])),
        config["log_dir"]
    ))
    os.makedirs(log_dir, exist_ok=True)
    
    # Create log file path with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"{logger_name}_{timestamp}.log")
    
    # Set up file handler with rotation
    max_bytes = config["max_log_size_mb"] * 1024 * 1024  # Convert MB to bytes
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
    use_console = console_output if console_output is not None else config["console_output"]
    if use_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    logger.info(f"Logger initialized with level {level_str}, log file: {log_file}")
    return logger


if __name__ == "__main__":
    # Example usage
    logger = setup_logger()
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
    
    print("\nLogger configuration created. Example log file created.")
    print("To use this logger in your scripts, import it with:")
    print("from logger_module import setup_logger")
    print("logger = setup_logger()")
