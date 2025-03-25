#!/usr/bin/env python3
"""
Utility functions for loading and managing application configuration.
"""

import os
import sys
import json
from typing import Dict, Any, Optional

def get_config_path(custom_path: Optional[str] = None) -> str:
    """
    Get the path to the configuration file.
    
    Args:
        custom_path: Optional custom path to the config file
        
    Returns:
        Absolute path to the config file
    """
    if custom_path:
        return os.path.abspath(custom_path)
    
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    config_dir = os.path.join(script_dir, "config")
    return os.path.join(config_dir, "config.json")

def load_full_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load the full application configuration.
    
    Args:
        config_path: Optional path to the config file
        
    Returns:
        Dictionary with the full application configuration
    """
    config_file = get_config_path(config_path)
    
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Config file not found at {config_file}. Creating default config.")
        # Create default config
        default_config = {
            "log_setup": {
                "log_level": "INFO",
                "max_log_size_mb": 20,
                "log_dir": "../logs",
                "console_output": True
            },
            "application": {
                "debug_mode": False,
                "max_threads": 4
            }
        }
        
        # Create config directory if it doesn't exist
        os.makedirs(os.path.dirname(config_file), exist_ok=True)
        
        # Write default config to file
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=4)
        
        return default_config
    except Exception as e:
        print(f"Error loading config file: {str(e)}. Using defaults.")
        return {
            "log_setup": {
                "log_level": "INFO",
                "max_log_size_mb": 20,
                "log_dir": "../logs",
                "console_output": True
            }
        }

def get_config_section(section: str, config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Get a specific section from the configuration.
    
    Args:
        section: Section name to retrieve
        config_path: Optional path to the config file
        
    Returns:
        Dictionary with the requested configuration section
    """
    config = load_full_config(config_path)
    return config.get(section, {})

def update_config_section(section: str, new_values: Dict[str, Any], config_path: Optional[str] = None) -> bool:
    """
    Update a section in the configuration file.
    
    Args:
        section: Section name to update
        new_values: New values to set in the section
        config_path: Optional path to the config file
        
    Returns:
        True if update was successful, False otherwise
    """
    config_file = get_config_path(config_path)
    
    try:
        # Load existing config
        config = load_full_config(config_path)
        
        # Update section
        if section not in config:
            config[section] = {}
        
        # Update values
        for key, value in new_values.items():
            config[section][key] = value
        
        # Write updated config back to file
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=4)
        
        return True
    except Exception as e:
        print(f"Error updating config file: {str(e)}")
        return False

if __name__ == "__main__":
    # Example usage
    config = load_full_config()
    print("Full config:", json.dumps(config, indent=2))
    
    log_config = get_config_section("log_setup")
    print("\nLog config:", json.dumps(log_config, indent=2))
    
    # Example of updating a section
    success = update_config_section("application", {"new_setting": "value"})
    if success:
        print("\nConfig updated successfully.")
    
    print("\nUpdated config:", json.dumps(load_full_config(), indent=2))
