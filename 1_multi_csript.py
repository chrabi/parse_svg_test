#!/usr/bin/env python3
"""
Multi-threaded script executor for launching OME inventory scripts.
Reads IP addresses from CSV output and launches get_ome_inventory.py scripts
in parallel with controlled concurrency.
"""

import os
import sys
import csv
import time
import subprocess
import threading
import queue
from typing import List, Dict, Any, Optional
import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Import our utilities
from config_parser_util import get_config_int
from universal_logger_configparser import get_module_logger

# Initialize logger
logger = get_module_logger(__name__)

class ScriptExecutor:
    """
    Class to manage execution of multiple scripts in parallel.
    """
    def __init__(self, max_concurrent: int = 10):
        """
        Initialize the script executor.
        
        Args:
            max_concurrent: Maximum number of concurrent scripts
        """
        self.max_concurrent = max_concurrent
        self.active_processes = {}  # Keep track of running processes
        self.lock = threading.Lock()  # Lock for thread-safe operations
        self.completed_count = 0
        
    def execute_script(self, ip_address: str, script_path: str = "get_ome_inventory.py") -> None:
        """
        Execute an OME inventory script for a specific IP address.
        
        Args:
            ip_address: IP address to pass to the script
            script_path: Path to the script to execute
        """
        try:
            # Prepare command
            cmd = [sys.executable, script_path, "--ip", ip_address]
            
            logger.info(f"Launching script for IP {ip_address}: {' '.join(cmd)}")
            
            # Start the process
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Keep track of this process
            process_id = id(process)
            with self.lock:
                self.active_processes[process_id] = {
                    'process': process,
                    'ip': ip_address,
                    'start_time': time.time()
                }
            
            # Wait for process to complete
            stdout, stderr = process.communicate()
            
            # Process completed
            with self.lock:
                if process_id in self.active_processes:
                    end_time = time.time()
                    duration = end_time - self.active_processes[process_id]['start_time']
                    del self.active_processes[process_id]
                    self.completed_count += 1
            
            # Log results
            if process.returncode == 0:
                logger.info(f"Script for IP {ip_address} completed successfully in {duration:.2f} seconds")
            else:
                logger.error(f"Script for IP {ip_address} failed with return code {process.returncode}")
                logger.error(f"Error output: {stderr}")
            
        except Exception as e:
            logger.error(f"Error executing script for IP {ip_address}: {str(e)}")
    
    def get_active_count(self) -> int:
        """
        Get the number of currently running processes.
        
        Returns:
            Number of active processes
        """
        with self.lock:
            return len(self.active_processes)
    
    def execute_batch(self, ip_addresses: List[str], script_path: str = "get_ome_inventory.py") -> None:
        """
        Execute scripts for multiple IP addresses with controlled concurrency.
        
        Args:
            ip_addresses: List of IP addresses to process
            script_path: Path to the script to execute
        """
        logger.info(f"Starting batch execution for {len(ip_addresses)} IP addresses with max concurrency {self.max_concurrent}")
        
        # Create a queue of IP addresses
        ip_queue = queue.Queue()
        for ip in ip_addresses:
            ip_queue.put(ip)
        
        total_ips = ip_queue.qsize()
        
        # Process IPs until queue is empty
        while not ip_queue.empty():
            # Check current active processes
            active_count = self.get_active_count()
            
            # Calculate how many new processes we can start
            slots_available = self.max_concurrent - active_count
            
            if slots_available > 0:
                # Start new processes up to the available slots
                for _ in range(min(slots_available, ip_queue.qsize())):
                    if ip_queue.empty():
                        break
                    
                    ip = ip_queue.get()
                    thread = threading.Thread(target=self.execute_script, args=(ip, script_path))
                    thread.daemon = True
                    thread.start()
            
            # Log progress
            completed = self.completed_count
            remaining = ip_queue.qsize()
            in_progress = active_count
            logger.info(f"Progress: {completed}/{total_ips} completed, {in_progress} in progress, {remaining} remaining")
            
            # Wait a bit before checking again
            time.sleep(5)
        
        # Wait for all processes to complete
        while self.get_active_count() > 0:
            logger.info(f"Waiting for {self.get_active_count()} processes to complete...")
            time.sleep(5)
        
        logger.info(f"Batch execution completed. Processed {self.completed_count} IP addresses.")

def read_ips_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    """
    Read IP addresses and device counts from a CSV file.
    
    Args:
        csv_path: Path to the CSV file
        
    Returns:
        List of dictionaries with 'IP' and 'ServerCount' keys
    """
    results = []
    
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                # Check if row has IP and ServerCount fields
                if 'IP' in row and 'ServerCount' in row:
                    # Only include rows with valid IPs and positive server counts
                    ip = row['IP'].strip()
                    
                    # Try to parse ServerCount
                    try:
                        server_count = int(row['ServerCount']) if row['ServerCount'] else 0
                    except (ValueError, TypeError):
                        server_count = 0
                    
                    # Add to results if valid
                    if ip and ip != '-' and server_count > 0:
                        results.append({
                            'IP': ip,
                            'ServerCount': server_count
                        })
    except Exception as e:
        logger.error(f"Error reading CSV file {csv_path}: {str(e)}")
    
    logger.info(f"Found {len(results)} valid IP addresses with positive server counts")
    return results

def main(csv_path: str, script_path: str = "get_ome_inventory.py"):
    """
    Main function to process CSV and execute OME inventory scripts.
    
    Args:
        csv_path: Path to the CSV file with IP addresses
        script_path: Path to the script to execute
    """
    # Read max concurrent scripts from config
    max_concurrent = get_config_int("application", "concurrent_ome_scripts", 10)
    
    # Read IPs from CSV
    ip_data = read_ips_from_csv(csv_path)
    
    if not ip_data:
        logger.error(f"No valid IP addresses found in {csv_path}")
        return
    
    # Extract just the IPs
    ip_list = [entry['IP'] for entry in ip_data]
    
    # Create executor and run batch
    executor = ScriptExecutor(max_concurrent=max_concurrent)
    executor.execute_batch(ip_list, script_path)

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Execute OME inventory scripts for IP addresses in a CSV file')
    parser.add_argument('csv_file', help='Path to the CSV file with IP addresses')
    parser.add_argument('--script', default='get_ome_inventory.py', help='Path to the script to execute (default: get_ome_inventory.py)')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.csv_file):
        print(f"Error: CSV file {args.csv_file} not found")
        sys.exit(1)
    
    main(args.csv_file, args.script)
