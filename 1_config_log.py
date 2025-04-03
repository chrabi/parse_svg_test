#!/usr/bin/env python3
"""
Utility for reading configuration from config.cfg file using ConfigParser.
"""

import os
import sys
import configparser
from pathlib import Path
from typing import Any, Optional, Dict

def get_config_path() -> str:
    """
    Get the path to the configuration file.
    
    Returns:
        Absolute path to the config file
    """
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    config_dir = os.path.join(script_dir, "config")
    return os.path.join(config_dir, "config.cfg")

def create_default_config(config_path: str) -> configparser.ConfigParser:
    """
    Create a default configuration file if it doesn't exist.
    
    Args:
        config_path: Path where config file should be created
        
    Returns:
        ConfigParser object with default settings
    """
    config = configparser.ConfigParser()
    
    # Default log settings
    config['log_setup'] = {
        'log_level': 'INFO',
        'max_log_size_mb': '20',
        'log_dir': '../logs',
        'console_output': 'true'
    }
    
    # Default application settings
    config['application'] = {
        'debug_mode': 'false',
        'max_threads': '10',
        'concurrent_ome_scripts': '10'
    }
    
    # Default database settings
    config['database'] = {
        'host': 'localhost',
        'port': '5432',
        'username': 'user',
        'password': 'password',
        'database': 'mydb'
    }
    
    # Default API settings
    config['api'] = {
        'endpoint': 'https://api.example.com',
        'timeout': '30',
        'retry_count': '3'
    }
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(config_path), exist_ok=True)
    
    # Write to file
    with open(config_path, 'w') as configfile:
        config.write(configfile)
    
    return config

def load_config() -> configparser.ConfigParser:
    """
    Load the application configuration from config.cfg.
    Creates a default configuration if it doesn't exist.
    
    Returns:
        ConfigParser object with application settings
    """
    config_path = get_config_path()
    config = configparser.ConfigParser()
    
    # Check if config file exists
    if not os.path.exists(config_path):
        print(f"Config file not found at {config_path}. Creating default config.")
        return create_default_config(config_path)
    
    # Load existing config
    try:
        config.read(config_path)
        return config
    except Exception as e:
        print(f"Error loading config file: {str(e)}. Creating default config.")
        return create_default_config(config_path)

def get_config_value(section: str, key: str, default: Any = None) -> Any:
    """
    Get a specific value from the configuration.
    
    Args:
        section: Section name in the config
        key: Key within the section
        default: Default value if key is not found
        
    Returns:
        Value from config, or default if not found
    """
    config = load_config()
    try:
        return config[section][key]
    except (KeyError, ValueError):
        return default

def get_config_int(section: str, key: str, default: int = 0) -> int:
    """
    Get an integer value from the configuration.
    
    Args:
        section: Section name in the config
        key: Key within the section
        default: Default value if key is not found or not an integer
        
    Returns:
        Integer value from config, or default if not found or not an integer
    """
    value = get_config_value(section, key)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def get_config_float(section: str, key: str, default: float = 0.0) -> float:
    """
    Get a float value from the configuration.
    
    Args:
        section: Section name in the config
        key: Key within the section
        default: Default value if key is not found or not a float
        
    Returns:
        Float value from config, or default if not found or not a float
    """
    value = get_config_value(section, key)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

def get_config_bool(section: str, key: str, default: bool = False) -> bool:
    """
    Get a boolean value from the configuration.
    
    Args:
        section: Section name in the config
        key: Key within the section
        default: Default value if key is not found or not a boolean
        
    Returns:
        Boolean value from config, or default if not found or not a boolean
    """
    value = get_config_value(section, key)
    if value is None:
        return default
    
    if isinstance(value, bool):
        return value
    
    if isinstance(value, str):
        return value.lower() in ('true', 'yes', '1', 'on', 't', 'y')
    
    return bool(value)

def update_config_value(section: str, key: str, value: Any) -> bool:
    """
    Update a specific value in the configuration.
    
    Args:
        section: Section name in the config
        key: Key within the section
        value: New value to set
        
    Returns:
        True if successful, False otherwise
    """
    config_path = get_config_path()
    config = load_config()
    
    # Ensure section exists
    if section not in config:
        config[section] = {}
    
    # Update value
    config[section][key] = str(value)
    
    # Write to file
    try:
        with open(config_path, 'w') as configfile:
            config.write(configfile)
        return True
    except Exception as e:
        print(f"Error updating config: {str(e)}")
        return False

if __name__ == "__main__":
    # Create default config if it doesn't exist
    config = load_config()
    
    # Print all sections and values
    for section in config.sections():
        print(f"[{section}]")
        for key, value in config[section].items():
            print(f"{key} = {value}")
        print()
    
    # Example of getting values
    log_level = get_config_value("log_setup", "log_level", "INFO")
    max_threads = get_config_int("application", "max_threads", 4)
    debug_mode = get_config_bool("application", "debug_mode", False)
    
    print(f"Log level: {log_level}")
    print(f"Max threads: {max_threads}")
    print(f"Debug mode: {debug_mode}")
