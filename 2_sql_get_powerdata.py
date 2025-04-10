#!/usr/bin/env python3.12
"""
CSV Power Data Processor

This script reads serial numbers from a CSV file, queries MSSQL databases for power data,
and appends the results back to the same CSV file with appropriate prefixes.

Requirements:
- Python 3.12
- pyodbc
- pandas
"""

import os
import sys
import argparse
import configparser
import logging
import pandas as pd
import pyodbc
import concurrent.futures
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Any

# Set up logging
def setup_logger():
    """Configure logging"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    script_name = Path(__file__).stem
    log_file = log_dir / f"{script_name}_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger(__name__)

# Load configuration
def load_config(config_path: str = "config.cfg") -> configparser.ConfigParser:
    """
    Load configuration from a config file
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        ConfigParser object with configuration settings
    """
    if not os.path.exists(config_path):
        create_default_config(config_path)
    
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def create_default_config(config_path: str):
    """
    Create a default configuration file
    
    Args:
        config_path: Path where the configuration file should be created
    """
    config = configparser.ConfigParser()
    
    config["database"] = {
        "server": "localhost",
        "database": "MyDatabase",
        "username": "sa",
        "password": "password",
        "driver": "ODBC Driver 17 for SQL Server"
    }
    
    config["processing"] = {
        "max_workers": "8",
        "timeout_seconds": "60"
    }
    
    config["queries"] = {
        "ov_query": "SELECT [DeviceName],[Model],[AveragePower],[PeakPower24h] FROM [STAGING].[OV].[ServerPower] WHERE SerialNumber LIKE (?)",
        "ome_query": "SELECT [DeviceSerialNumber],[Model],[AvgPower],[peakPower] FROM [STAGING].[OME].[ServerPower] WHERE DeviceSerialNumber LIKE (?) ORDER BY Timestamp DESC"
    }
    
    with open(config_path, 'w') as configfile:
        config.write(configfile)

# Database connection and query functions
def create_connection(config: configparser.ConfigParser) -> pyodbc.Connection:
    """
    Create a database connection
    
    Args:
        config: ConfigParser object with database settings
        
    Returns:
        Database connection object
    """
    conn_str = (
        f"DRIVER={{{config['database']['driver']}}};"
        f"SERVER={config['database']['server']};"
        f"DATABASE={config['database']['database']};"
        f"UID={config['database']['username']};"
        f"PWD={config['database']['password']};"
    )
    
    return pyodbc.connect(conn_str)

def query_ov_power_data(serial_number: str, conn: pyodbc.Connection, query: str) -> Dict[str, Any]:
    """
    Query OV power data for a specific serial number
    
    Args:
        serial_number: Serial number to query
        conn: Database connection
        query: SQL query to execute
        
    Returns:
        Dictionary with query results or empty dict if no results
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, serial_number)
        
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()
        
        if row:
            # Add OV_ prefix to column names
            return {f"OV_{col}": val for col, val in zip(columns, row)}
        return {}
        
    except Exception as e:
        logger.error(f"Error querying OV data for {serial_number}: {str(e)}")
        return {}

def query_ome_power_data(serial_number: str, conn: pyodbc.Connection, query: str) -> Dict[str, Any]:
    """
    Query OME power data for a specific serial number
    
    Args:
        serial_number: Serial number to query
        conn: Database connection
        query: SQL query to execute
        
    Returns:
        Dictionary with query results or empty dict if no results
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, serial_number)
        
        columns = [column[0] for column in cursor.description]
        row = cursor.fetchone()  # Get only the first (most recent) row
        
        if row:
            # Add OME_ prefix to column names
            return {f"OME_{col}": val for col, val in zip(columns, row)}
        return {}
        
    except Exception as e:
        logger.error(f"Error querying OME data for {serial_number}: {str(e)}")
        return {}

def process_serial_number(serial_number: str, config: configparser.ConfigParser) -> Dict[str, Any]:
    """
    Process a single serial number by querying both databases
    
    Args:
        serial_number: Serial number to process
        config: ConfigParser object with configuration settings
        
    Returns:
        Dictionary with combined query results
    """
    result = {"SerialNumber": serial_number}
    
    try:
        # Create a database connection
        conn = create_connection(config)
        
        # Query OV power data
        ov_query = config["queries"]["ov_query"]
        ov_data = query_ov_power_data(serial_number, conn, ov_query)
        result.update(ov_data)
        
        # Query OME power data
        ome_query = config["queries"]["ome_query"]
        ome_data = query_ome_power_data(serial_number, conn, ome_query)
        result.update(ome_data)
        
        conn.close()
        
        logger.info(f"Successfully processed serial number: {serial_number}")
        return result
        
    except Exception as e:
        logger.error(f"Error processing serial number {serial_number}: {str(e)}")
        return result

def process_csv_file(csv_path: str, config: configparser.ConfigParser) -> bool:
    """
    Process a CSV file by querying databases for each serial number
    
    Args:
        csv_path: Path to the CSV file
        config: ConfigParser object with configuration settings
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Read the CSV file
        df = pd.read_csv(csv_path)
        
        # Check if serialNumber column exists
        if 'serialNumber' not in df.columns:
            logger.error(f"CSV file does not contain a 'serialNumber' column: {csv_path}")
            return False
        
        # Get unique serial numbers
        serial_numbers = df['serialNumber'].unique().tolist()
        logger.info(f"Found {len(serial_numbers)} unique serial numbers in {csv_path}")
        
        # Process serial numbers in parallel
        max_workers = int(config["processing"]["max_workers"])
        results = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_serial = {
                executor.submit(process_serial_number, sn, config): sn 
                for sn in serial_numbers
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(future_to_serial)):
                serial_number = future_to_serial[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Log progress
                    if (i + 1) % 10 == 0 or (i + 1) == len(serial_numbers):
                        logger.info(f"Progress: {i + 1}/{len(serial_numbers)} serial numbers processed")
                        
                except Exception as e:
                    logger.error(f"Error processing {serial_number}: {str(e)}")
        
        # Create a DataFrame from results
        results_df = pd.DataFrame(results)
        
        # If no results, return
        if results_df.empty:
            logger.warning("No results retrieved from database")
            return False
        
        # Merge with original DataFrame on serialNumber
        merged_df = pd.merge(
            df, 
            results_df, 
            how='left', 
            left_on='serialNumber', 
            right_on='SerialNumber'
        )
        
        # Remove the redundant SerialNumber column if it exists
        if 'SerialNumber' in merged_df.columns and 'SerialNumber' != 'serialNumber':
            merged_df = merged_df.drop(columns=['SerialNumber'])
        
        # Save the result back to the same CSV file
        merged_df.to_csv(csv_path, index=False)
        logger.info(f"Successfully updated CSV file: {csv_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error processing CSV file {csv_path}: {str(e)}")
        return False

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Process server power data from CSV file')
    parser.add_argument('csv_path', help='Path to the CSV file containing serial numbers')
    parser.add_argument('--config', default='config.cfg', help='Path to configuration file')
    args = parser.parse_args()
    
    # Check if CSV file exists
    if not os.path.exists(args.csv_path):
        logger.error(f"CSV file not found: {args.csv_path}")
        return 1
    
    # Load configuration
    config = load_config(args.config)
    
    # Process CSV file
    success = process_csv_file(args.csv_path, config)
    
    return 0 if success else 1

if __name__ == "__main__":
    # Setup logger
    logger = setup_logger()
    
    try:
        logger.info("Starting CSV Power Data Processor")
        exit_code = main()
        if exit_code == 0:
            logger.info("Processing completed successfully")
        else:
            logger.error("Processing completed with errors")
        sys.exit(exit_code)
    except Exception as e:
        logger.exception(f"Unhandled exception: {str(e)}")
        sys.exit(1)
