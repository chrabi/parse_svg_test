#!/usr/bin/env python3.12
"""
Script for executing GraphQL queries and saving results to CSV.
Query concerns servers from applicationXGenericHardwar with filter on applicationId (175442).
"""

import csv
import json
import logging
import os
import sys
import time
import socket
import subprocess
import concurrent.futures
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

import requests
from requests.exceptions import RequestException

# Logger configuration
def setup_logger() -> logging.Logger:
    """Configure the logging system."""
    logger = logging.getLogger("graphql_export")
    logger.setLevel(logging.INFO)
    
    # Handler for console logs
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    
    # Handler for file logs
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    file_handler = logging.FileHandler(f"{log_dir}/graphql_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

# Function to safely retrieve values from nested dictionaries
def safe_get(data, *keys, default=""):
    """
    Safely retrieves values from nested dictionaries.
    Returns a default value if any element in the path is None.
    
    Args:
        data: Dictionary data
        *keys: Keys for nested access
        default: Default value returned in case of error
        
    Returns:
        Value from the nested dictionary or default value
    """
    current = data
    for key in keys:
        if current is None or not isinstance(current, dict):
            return default
        current = current.get(key)
    return current if current is not None else default

# Function to remove duplicate servers
def remove_duplicate_servers(server_data: List[Dict[str, Any]], key_field: str, logger: logging.Logger) -> List[Dict[str, Any]]:
    """
    Removes duplicate servers based on the specified key field.
    
    Args:
        server_data: List of server data
        key_field: Field name to check for duplicates (e.g., "ServerName")
        logger: Logger object
        
    Returns:
        List of unique servers
    """
    seen_keys = set()
    unique_servers = []
    
    logger.info(f"Before deduplication: {len(server_data)} records")
    
    for server in server_data:
        key_value = server.get(key_field, "")
        if key_value and key_value not in seen_keys:
            seen_keys.add(key_value)
            unique_servers.append(server)
    
    logger.info(f"After deduplication: {len(unique_servers)} unique records")
    return unique_servers

# Function for pinging a server and returning its IP address (thread version)
def ping_server(server_name: str) -> Tuple[str, str]:
    """
    Pings the given server and returns its name and IP address.
    Performs 2 ping attempts for increased reliability.
    
    Args:
        server_name: Server name to ping
        
    Returns:
        Tuple (server_name, ip_address), where ip_address can be an empty string in case of failure
    """
    if not server_name:
        return (server_name, "-")
        
    try:
        # Try to resolve server name to IP address
        try:
            ip_address = socket.gethostbyname(server_name)
            return (server_name, ip_address)
        except socket.gaierror:
            # If name resolution fails, try to ping
            for attempt in range(1, 3):  # 2 ping attempts
                # Determine ping command based on operating system
                if sys.platform.startswith('win'):
                    ping_cmd = ["ping", "-n", "1", "-w", "2000", server_name]
                else:
                    ping_cmd = ["ping", "-c", "1", "-W", "2", server_name]
                    
                # Execute ping command
                process = subprocess.Popen(ping_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                output, error = process.communicate()
                output_str = output.decode('utf-8', errors='ignore')
                
                # Parse ping result to extract IP address
                if process.returncode == 0:
                    # Try to extract IP address from ping result
                    import re
                    ip_pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
                    ip_matches = re.findall(ip_pattern, output_str)
                    if ip_matches:
                        ip_address = ip_matches[0]
                        return (server_name, ip_address)
                elif attempt < 2:  # If this is not the last attempt, wait before the next one
                    time.sleep(1)  # Wait 1 second before the next attempt
            
            return (server_name, "-")
    except Exception:
        return (server_name, "-")

# Function for multithreaded server pinging
def get_server_ips_parallel(server_names: List[str], max_workers: int, logger: logging.Logger) -> Dict[str, str]:
    """
    Pings multiple servers in parallel and returns a mapping of server names to IP addresses.
    
    Args:
        server_names: List of server names to ping
        max_workers: Maximum number of parallel threads
        logger: Logger object
        
    Returns:
        Dictionary {server_name: ip_address}
    """
    if not server_names:
        return {}
        
    unique_names = list(set(server_names))  # Remove duplicates
    results = {}
    
    logger.info(f"Starting parallel pinging of {len(unique_names)} unique servers using {max_workers} threads")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start all pinging tasks
        future_to_server = {executor.submit(ping_server, name): name for name in unique_names}
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_server):
            server_name, ip = future.result()
            results[server_name] = ip
            
            completed += 1
            if completed % 10 == 0 or completed == len(unique_names):
                logger.info(f"Pinging progress: {completed}/{len(unique_names)} ({round(completed/len(unique_names)*100)}%)")
    
    # Results summary
    success_count = sum(1 for ip in results.values() if ip and ip != "-")
    logger.info(f"Pinging completed. Found IP addresses for {success_count}/{len(unique_names)} servers")
    
    return results

# Maintained old implementation for compatibility and single calls
def get_server_ip(server_name: str, logger: logging.Logger) -> str:
    """
    Pings the given server and returns its IP address.
    Performs 2 ping attempts for increased reliability.
    
    Args:
        server_name: Server name to ping
        logger: Logger object
        
    Returns:
        Server IP address or "-" in case of error
    """
    if not server_name:
        return "-"
        
    try:
        logger.info(f"Pinging server: {server_name}")
        server_name, ip = ping_server(server_name)
        if ip and ip != "-":
            logger.info(f"Found IP address for {server_name}: {ip}")
        else:
            logger.warning(f"Failed to get IP address for server {server_name}")
        return ip
    except Exception as e:
        logger.error(f"Error while pinging server {server_name}: {str(e)}")
        return "-"

# Function to check OneView session and retrieve the server count
def check_oneview_connection(ip_address: str) -> Tuple[bool, int, str]:
    """
    Attempts to connect to OneView instance and retrieve server count.
    
    Args:
        ip_address: OneView server IP address
    
    Returns:
        Tuple (session_success, server_count, system_type)
    """
    if not ip_address or ip_address == "-":
        return (False, None, "")
        
    try:
        # URL for HP OneView authentication using provided IP
        oneview_url = f"https://{ip_address}"
        auth_url = f"{oneview_url}/rest/login-sessions"
        server_hardware_url = f"{oneview_url}/rest/server-hardware"
        
        # Authentication data - replace with proper credentials
        auth_data = {
            "userName": "admin",
            "password": "password"
        }
        
        # Headers for the request
        headers = {
            "Content-Type": "application/json",
            "X-API-Version": "800"
        }
        
        # Execute authentication request
        auth_response = requests.post(auth_url, headers=headers, json=auth_data, verify=False, timeout=30)
        
        if auth_response.status_code != 200:
            return (False, None, "Dell OME")  # Assume Dell OME if authentication failed
            
        # Get SessionID
        session_id = auth_response.json().get("sessionID")
        
        if not session_id:
            return (False, None, "Dell OME")
            
        # Update headers with session token
        headers["Auth"] = session_id
        
        # Execute request to /rest/server-hardware
        servers_response = requests.get(server_hardware_url, headers=headers, verify=False, timeout=30)
        
        if servers_response.status_code != 200:
            return (True, None, "HP OV")  # Got session but failed to get server count
            
        # Get total number of servers
        total_servers = servers_response.json().get("total")
        
        return (True, total_servers, "HP OV")
    except Exception:
        return (False, None, "Dell OME")

# Function to check OneView for multiple IPs in parallel
def check_oneview_connections_parallel(ip_addresses: List[str], max_workers: int, logger: logging.Logger) -> Dict[str, Tuple[bool, int, str]]:
    """
    Checks OneView connections for multiple IP addresses in parallel.
    
    Args:
        ip_addresses: List of IP addresses to check
        max_workers: Maximum number of parallel threads
        logger: Logger object
        
    Returns:
        Dictionary {ip_address: (session_success, server_count, system_type)}
    """
    if not ip_addresses:
        return {}
        
    unique_ips = list(set([ip for ip in ip_addresses if ip and ip != "-"]))  # Remove duplicates and empty IPs
    results = {}
    
    if not unique_ips:
        logger.warning("No valid IP addresses to check for OneView")
        return results
    
    logger.info(f"Starting parallel OneView check for {len(unique_ips)} unique IPs using {max_workers} threads")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start all OneView check tasks
        future_to_ip = {executor.submit(check_oneview_connection, ip): ip for ip in unique_ips}
        
        # Process results as they complete
        completed = 0
        for future in concurrent.futures.as_completed(future_to_ip):
            ip = future_to_ip[future]
            session_success, server_count, system_type = future.result()
            results[ip] = (session_success, server_count, system_type)
            
            completed += 1
            if completed % 5 == 0 or completed == len(unique_ips):
                logger.info(f"OneView check progress: {completed}/{len(unique_ips)} ({round(completed/len(unique_ips)*100)}%)")
    
    # Results summary
    success_count = sum(1 for result in results.values() if result[0])
    hp_count = sum(1 for result in results.values() if result[2] == "HP OV")
    dell_count = sum(1 for result in results.values() if result[2] == "Dell OME")
    
    logger.info(f"OneView check completed. Successfully connected to {success_count}/{len(unique_ips)} systems")
    logger.info(f"System types: HP OV: {hp_count}, Dell OME: {dell_count}")
    
    return results

# Function to execute GraphQL query
def execute_graphql_query(url: str, query: str, variables: Dict[str, Any], logger: logging.Logger) -> Optional[Dict[str, Any]]:
    """
    Executes a GraphQL query and returns the result.
    
    Args:
        url: GraphQL endpoint URL
        query: GraphQL query text
        variables: Variables for the query
        logger: Logger object
        
    Returns:
        Dictionary with response or None in case of error
    """
    headers = {
        "Content-Type": "application/json",
        # Add additional headers here if required (e.g., authorization)
        # "Authorization": "Bearer YOUR_TOKEN"
    }
    
    payload = {
        "query": query,
        "variables": variables
    }
    
    try:
        logger.info(f"Executing GraphQL query with variables: {variables}")
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        if "errors" in data:
            logger.error(f"GraphQL error: {data['errors']}")
            return None
            
        return data
        
    except RequestException as e:
        logger.error(f"Error while executing query: {str(e)}")
        return None
    except ValueError as e:
        logger.error(f"Error while parsing JSON response: {str(e)}")
        return None

# Function to process data and save to CSV
def process_and_save_to_csv(data: Dict[str, Any], logger: logging.Logger) -> bool:
    """
    Processes data from GraphQL response and saves to CSV file.
    
    Args:
        data: Dictionary with response data
        logger: Logger object
        
    Returns:
        True if operation succeeded, False otherwise
    """
    try:
        # Check if data contains expected structure
        if not data or "data" not in data:
            logger.error("No data to process")
            return False
            
        # Adjust this path to your GraphQL data structure
        server_data = data.get("data", {}).get("applicationXGenericHardwar", [])
        
        if not server_data:
            logger.warning("No records to save")
            return False
            
        # Prepare output directory
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = f"output/{timestamp}/Inventory"
        os.makedirs(output_dir, exist_ok=True)
        
        # CSV filename
        current_date = datetime.now().strftime("%Y%m%d")
        output_file = f"{output_dir}/Inventory_{current_date}.csv"
        
        # CSV headers
        fieldnames = ["Id", "ServerName", "Serial", "AppId", "AppName", "TimestampEpoch", "IP", "ServerCount", "SystemType"]
        
        # Prepare data to save
        csv_data = []
        
        logger.info(f"Processing {len(server_data)} records")
        
        for server in server_data:
            try:
                # Safely retrieve data using safe_get function
                server_row = {
                    "Id": safe_get(server, "HardwareInfo", "Idresource"),
                    "ServerName": safe_get(server, "HardwareInfo", "serverName"),
                    "Serial": safe_get(server, "HardwareInfo", "serial"),
                    "AppId": safe_get(server, "applicationId"),
                    "AppName": f"App-{safe_get(server, 'applicationId')}",
                    "TimestampEpoch": int(time.time()),
                    "IP": "-",  # Default value, will be updated later
                    "ServerCount": None,  # Default value, will be updated later
                    "SystemType": ""  # Default value, will be updated later
                }
                csv_data.append(server_row)
            except Exception as e:
                logger.warning(f"Error while processing record: {str(e)}. Continuing with next records.")
                continue
        
        # Remove duplicates based on ServerName
        csv_data = remove_duplicate_servers(csv_data, "ServerName", logger)
        
        # Get all server names from records
        server_names = [server_row.get("ServerName") for server_row in csv_data if server_row.get("ServerName")]
        
        # Execute parallel pinging for all servers
        max_workers_ping = min(32, len(server_names))  # Maximum 32 threads, or less if we have fewer servers
        ip_results = get_server_ips_parallel(server_names, max_workers_ping, logger)
        
        # Update CSV records with IP addresses
        for server_row in csv_data:
            server_name = server_row.get("ServerName")
            if server_name and server_name in ip_results:
                server_row["IP"] = ip_results[server_name]
        
        # Get all IP addresses from records
        all_ips = [server_row.get("IP") for server_row in csv_data if server_row.get("IP") and server_row.get("IP") != "-"]
        
        # Check OneView for all IPs in parallel
        max_workers_ov = min(16, len(all_ips))  # Maximum 16 threads, or less if we have fewer IPs
        oneview_results = check_oneview_connections_parallel(all_ips, max_workers_ov, logger)
        
        # Update CSV records with OneView information
        for server_row in csv_data:
            ip = server_row.get("IP")
            if ip and ip != "-" and ip in oneview_results:
                session_success, server_count, system_type = oneview_results[ip]
                server_row["ServerCount"] = server_count
                server_row["SystemType"] = system_type
        
        # Save to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_data)
            
        logger.info(f"Saved {len(csv_data)} records to file {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error while processing data: {str(e)}")
        return False

def main():
    """Main program function."""
    logger = setup_logger()
    logger.info("Starting script execution")
    start_time = time.time()  # Start time measurement
    
    # GraphQL endpoint URL - replace with the correct address
    graphql_url = "https://example.com/graphql"
    
    # GraphQL query
    query = """
    query GetGenericHardware($page: Int!, $size: Int!, $applicationId: String!) {
      applicationXGenericHardwar(page: $page, size: $size, filter: { 
        applicationId: { equals: $applicationId }
      }) {
        applicationId
        driftId
        HardwareInfo {
          Idresource
          serverName
          serial
        }
      }
    }
    """
    
    # Variables for the query - base values
    base_variables = {
        "size": 100,
        "applicationId": "175442"
    }
    
    try:
        # List for all fetched data
        all_results = []
        
        # Execute query for pages 1 to 4
        for page_num in range(1, 5):
            logger.info(f"Fetching data for page {page_num}")
            
            # Update page number in variables
            variables = {**base_variables, "page": page_num}
            
            # Execute query for current page
            result = execute_graphql_query(graphql_url, query, variables, logger)
            
            if not result:
                logger.error(f"Failed to execute GraphQL query for page {page_num}")
                continue
            
            # Extract data from response
            server_data = safe_get(result, "data", "applicationXGenericHardwar", default=[])
            
            if not server_data:
                logger.warning(f"No records on page {page_num}")
                continue
                
            logger.info(f"Retrieved {len(server_data)} records from page {page_num}")
            all_results.extend(server_data)
        
        # Check if any data was retrieved
        if not all_results:
            logger.error("Failed to retrieve any data from all pages")
            sys.exit(1)
            
        logger.info(f"Total records retrieved from all pages: {len(all_results)}")
        
        # Deduplicate servers to remove repeated names
        all_results = remove_duplicate_servers(all_results, "HardwareInfo.serverName", logger)
        
        # Prepare data in format appropriate for process_and_save_to_csv
        combined_data = {"data": {"applicationXGenericHardwar": all_results}}
        
        # Process and save to CSV
        if not process_and_save_to_csv(combined_data, logger):
            logger.error("Failed to save data to CSV file")
            sys.exit(1)
            
        logger.info("Script completed successfully")
        total_time = time.time() - start_time
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        
    except Exception as e:
        logger.critical(f"Unexpected error occurred: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
