#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raidcom IOLimit data collection script
Compatible with Python 3.6.8 - uses only native libraries
"""

import subprocess
import os
import sys
import json
import csv
import re
import argparse
import configparser
import base64
import datetime
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging with thread safety
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')

# Thread lock for shared data access
data_lock = threading.Lock()
all_spm_data = []

def load_config_file(config_file):
    """Load configuration from file"""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def get_timestamp(format_type="epoch"):
    """Get timestamp in different formats"""
    if format_type == "epoch":
        return str(int(datetime.datetime.now().timestamp()))
    elif format_type == "days":
        return datetime.datetime.now().strftime("%Y-%m-%d")
    elif format_type == "5min":
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

def simple_encrypt(text, key="default_key"):
    """Simple base64 encoding (replacement for Fernet)"""
    try:
        # Combine text with key for basic obfuscation
        combined = f"{key}:{text}"
        encoded = base64.b64encode(combined.encode()).decode()
        return encoded
    except Exception as e:
        LOG.error(f"Error encoding: {e}")
        return text

def simple_decrypt(encoded_text, key="default_key"):
    """Simple base64 decoding"""
    try:
        decoded = base64.b64decode(encoded_text.encode()).decode()
        if decoded.startswith(f"{key}:"):
            return decoded[len(f"{key}:"):]
        return decoded
    except Exception as e:
        LOG.error(f"Error decoding: {e}")
        return encoded_text

def get_credentials(user_salt, password_salt, api_name):
    """Get credentials with simple decoding"""
    # This is a simplified version - in production you'd want proper credential management
    username = simple_decrypt(user_salt)
    password = simple_decrypt(password_salt) 
    return username, password

def run_command(command):
    """Execute shell command and return output"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        LOG.error(f"Error executing command: {command}")
        LOG.info(f"Error message: {e.stderr}")
        sys.exit(1)

def send_scp_file(user, file_path, remote_server, remote_path):
    """Send file to remote server using SCP"""
    try:
        # Prepare catalog on remote server
        remote_mkdir_command = f"ssh {user}@{remote_server} mkdir -p {remote_path}"
        remote_create_catalog = subprocess.run(
            remote_mkdir_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        
        if remote_create_catalog.returncode != 0:
            raise subprocess.CalledProcessError(
                remote_create_catalog.returncode, remote_mkdir_command
            )

        # SCP command
        scp_command = f"scp {file_path} {user}@{remote_server}:{remote_path}"
        result = subprocess.run(
            scp_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, scp_command)
        
        # Check if file was sent successfully
        if not os.path.exists(file_path):
            LOG.error(f"File {file_path} does not exist after sending.")
        else:
            # Check if the file exists on the remote server
            remote_check_result = subprocess.run(
                f"ssh {user}@{remote_server} ls {remote_path}",
                shell=True,
                capture_output=True,
                text=True,
            )
            if remote_check_result.returncode == 0:
                raise subprocess.CalledProcessError(
                    remote_check_result.returncode, "File not found on remote server"
                )
        
        LOG.info(f"File {file_path} exists on the remote server {remote_server}")
        
    except subprocess.CalledProcessError as e:
        LOG.error(f"Error sending file {file_path}: {e.stderr}")
        sys.exit(1)

def save_meta_json(filename, batch_epoch, column, region):
    """Save data to CSV file metadata"""
    data = {
        "db_name": f"SAN_INFO_{region.upper()}",
        "table_schema": "Hitachi",
        "table_name": column,
        "sql_timestamp": "2025-05-11T05:15:03",
        "index_key": "ArraySerial,ArrayPort,HostNickname",
        "staging_db": "STAGING",
        "batch_epoch_identifier": batch_epoch,
        "processed": True,
        "force_convert_dtypes": None,
        "delimiter": ",",
    }
    
    # Save as JSON
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def parse_csv_content(content, separator=","):
    """Parse CSV-like content into list of dictionaries"""
    lines = content.strip().split('\n')
    if not lines:
        return []
    
    # Get headers from first line
    headers = [h.strip() for h in lines[0].split(separator)]
    
    data = []
    for line in lines[1:]:
        if line.strip():
            values = [v.strip() for v in line.split(separator)]
            if len(values) >= len(headers):
                row_dict = {}
                for i, header in enumerate(headers):
                    row_dict[header] = values[i] if i < len(values) else ""
                data.append(row_dict)
    
    return data

def write_csv_file(filename, data, fieldnames=None):
    """Write data to CSV file"""
    if not data:
        return
    
    if fieldnames is None:
        fieldnames = list(data[0].keys()) if data else []
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def get_serial_numbers(instance):
    """Get all serial numbers from raidqry -l command"""
    try:
        cmd = f"raidqry -l -I{instance}"
        LOG.info(f"Getting serial numbers with command: {cmd}")
        
        output = run_command(cmd)
        
        # Extract serial numbers (assuming 5-digit numbers starting with 5)
        serial_numbers = re.findall(r'\b5\d{4}\b', output)
        
        if not serial_numbers:
            LOG.warning("No serial numbers found from raidqry command")
            return []
        
        # Remove duplicates while preserving order
        unique_serials = []
        seen = set()
        for serial in serial_numbers:
            if serial not in seen:
                seen.add(serial)
                unique_serials.append(serial)
        
        LOG.info(f"Found {len(unique_serials)} unique serial numbers: {unique_serials}")
        return unique_serials
        
    except Exception as e:
        LOG.error(f"Error getting serial numbers: {e}")
        return []

def chunk_list(lst, chunk_size):
    """Divide list into chunks of specified size"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def process_serial_number(serial_number, args, TIME_EPOCH, TIME_DAYS, TIME_5MIN):
    """Process a single serial number to collect SPM data"""
    thread_name = threading.current_thread().name
    LOG.info(f"[{thread_name}] Processing serial number: {serial_number}")
    
    try:
        local_spm_data = []
        
        # Get port information for this serial number
        port_cmd = f"raidcom get port -s {serial_number} -IH{{{args.inst}}} |grep {args.inst}"
        LOG.info(f"[{thread_name}] Getting ports for serial {serial_number}")
        
        port_output = run_command(port_cmd)
        
        # Parse port output to get port IDs
        port_ids = re.findall(r'CL\d-[A-Z]-[A-Z]', port_output)
        LOG.info(f"[{thread_name}] Found {len(port_ids)} ports for serial {serial_number}: {port_ids}")
        
        # For each port, check host groups and WWPNs
        for port_id in port_ids:
            LOG.info(f"[{thread_name}] Check spm_wwn in array {serial_number} for port {port_id}...")
            
            # Get SPM WWN information for this port
            host_cmd = f"raidcom get spm_wwn -port {port_id} -s {serial_number} -IH{{{args.inst}}} -T{args.inst}"
            host_output = run_command(host_cmd)
            
            if not host_output:
                LOG.warning(f"[{thread_name}] No data found for port {port_id} on serial {serial_number}")
                continue
            else:
                LOG.info(f"[{thread_name}] Data found in host_output for port {port_id}, {len(host_output)} lines")
                
                # Parse host data as CSV
                host_data = parse_csv_content(host_output, separator=" ")
                
                if not host_data:
                    LOG.info(f"[{thread_name}] No parseable data found in host_output for port {port_id}")
                    continue
                
                # Extract WWPNs
                wwpns_pattern = r"([0-9a-fa-f]{16})"
                wwpns = re.findall(wwpns_pattern, host_output)
                LOG.info(f"[{thread_name}] Found wwpns: {wwpns}")
                
                for host_wwpn in wwpns:
                    # Get SPM monitoring data for each WWPN
                    spm_cmd = f"raidcom get spm_wwn -port {port_id} -hba_wwn {host_wwpn} -s {serial_number} -IH{{{args.inst}}} -T{args.inst}"
                    spm_output = run_command(spm_cmd)
                    
                    # Parse SPM output
                    spm_data = parse_csv_content(spm_output, separator=" ")
                    
                    if spm_data:
                        # Extract required fields and add to collection
                        for row in spm_data:
                            # Extract values from the parsed data
                            spm_value = row.get('KBps', '')
                            spm_priority = row.get('Pri', '')
                            
                            # Find host nickname from the data
                            host_nickname = ""
                            nickname_match = re.search(r'10009440c9d0b045', host_output)
                            if nickname_match:
                                host_group = re.findall(r'host_pattern, host_nickname\.values\[0\]', host_output)
                                if host_group:
                                    host_nickname = host_group[0]
                            
                            # Format WWPN with colons for readability
                            formatted_wwpn = ":".join([
                                host_wwpn[i : i + 2] for i in range(0, len(host_wwpn), 2)
                            ])
                            
                            # Parse SPM output to extract monitoring data
                            spm_lines = spm_output.strip().split('\n')
                            if len(spm_lines) > 1:  # Header + data
                                for line in spm_lines[1:]:
                                    parts = line.split()
                                    if len(parts) >= 4:
                                        spm_data_entry = {
                                            "ArraySerial": serial_number,
                                            "ArrayPort": port_id,
                                            "HostNickname": host_nickname if host_nickname else "Unknown",
                                            "HostGroup": host_group[0] if 'host_group' in locals() else "",
                                            "HostWWPN": formatted_wwpn,
                                            "MonitorIOps": int(parts[2]) if parts[2].isdigit() else 0,
                                            "MonitorKBps": int(parts[3]) if parts[3].isdigit() else 0,
                                            "SPMLimitKBps": int(spm_value) if spm_value and spm_value.isdigit() else 0,
                                            "SPMPriority": spm_priority if spm_priority else "",
                                            "SourceLoadTimeEpoch": TIME_EPOCH,
                                            "SourceName": "HostIOLimit",
                                            "BatchCreateTimeEpoch": TIME_EPOCH,
                                        }
                                        local_spm_data.append(spm_data_entry)
        
        # Thread-safe adding to global data
        with data_lock:
            all_spm_data.extend(local_spm_data)
        
        LOG.info(f"[{thread_name}] Completed processing serial {serial_number}, collected {len(local_spm_data)} records")
        
    except Exception as e:
        LOG.error(f"[{thread_name}] Error processing serial number {serial_number}: {e}")

def process_serial_chunk(serial_chunk, args, TIME_EPOCH, TIME_DAYS, TIME_5MIN):
    """Process a chunk of serial numbers in parallel"""
    chunk_name = f"Chunk-{'-'.join(serial_chunk)}"
    LOG.info(f"[{chunk_name}] Starting processing chunk: {serial_chunk}")
    
    # Create threads for each serial number in the chunk
    threads = []
    for serial_number in serial_chunk:
        thread = threading.Thread(
            target=process_serial_number,
            args=(serial_number, args, TIME_EPOCH, TIME_DAYS, TIME_5MIN),
            name=f"Serial-{serial_number}"
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads in this chunk to complete
    for thread in threads:
        thread.join()
    
    LOG.info(f"[{chunk_name}] Completed processing chunk: {serial_chunk}")

def calculate_percentage(monitor_kbps, spml_limit_kbps):
    """Calculate percentage with 2 decimal places"""
    try:
        if monitor_kbps and spml_limit_kbps and float(spml_limit_kbps) > 0:
            return round((float(monitor_kbps) / float(spml_limit_kbps)) * 100, 2)
        else:
            return 0
    except (ValueError, ZeroDivisionError):
        return 0

def main():
    try:
        # Reset global data
        global all_spm_data
        all_spm_data = []
        
        # Parse arguments
        parser = argparse.ArgumentParser(
            description="Get data from HDS raidcom IOLIMIT SPM information"
        )
        parser.add_argument(
            "--inst", type=int, required=True, help="Raidcom instance default 99"
        )
        parser.add_argument(
            "--region",
            type=str,
            required=True,
            choices=["emea", "nam", "latam", "apac"],
            help="Region [ emea,nam,apac,latam ]",
        )
        parser.add_argument(
            "--username", type=str, required=False, help="username for raidcom AD user"
        )
        parser.add_argument(
            "--password", type=str, required=False, help="password for raidcom AD user"
        )
        parser.add_argument(
            "--threads", type=int, default=2, help="Number of threads to use (default: 2)"
        )

        args = parser.parse_args()
        region = args.region
        
        # Load configuration
        CONFIG = load_config_file("config/config.cfg")
        api_name = "HITACHI"
        
        # Get credentials
        user_salt = CONFIG.get(api_name, "user_salt")
        password_salt = CONFIG.get(api_name, "password_salt")
        scp_user = CONFIG.get(api_name, "scp_user")
        scp_server = CONFIG.get(api_name, "scp_server")
        scp_name = "scp_path_" + args.region
        scp_path = CONFIG.get(api_name, scp_name)
        
        credentials = get_credentials(user_salt, password_salt, api_name)
        username = credentials[0]
        password = credentials[1]
        
        if not args.username:
            args.username = username
        if not args.password:
            args.password = password

        # Extract timestamps
        TIME_EPOCH = get_timestamp("epoch")
        TIME_DAYS = get_timestamp("days")
        TIME_5MIN = get_timestamp("5min")

        # Login to instance
        LOG.info(f"Login to raidcom with user: {args.username} and Inst: {args.inst} and Region {args.region}")
        
        login_cmd = f"raidcom -login {args.username} {args.password} -I{args.inst}"
        run_command(login_cmd)

        # Get all serial numbers from raidqry -l
        LOG.info(f"Getting all serial numbers from instance {args.inst}")
        serial_numbers = get_serial_numbers(args.inst)
        
        if not serial_numbers:
            LOG.error("No serial numbers found. Exiting.")
            sys.exit(1)

        LOG.info(f"Found {len(serial_numbers)} serial numbers: {serial_numbers}")

        # Process serial numbers in chunks of 2 (or specified thread count)
        chunk_size = 2  # Always process 2 serial numbers at a time as requested
        serial_chunks = list(chunk_list(serial_numbers, chunk_size))
        
        LOG.info(f"Processing {len(serial_chunks)} chunks of {chunk_size} serial numbers each")
        
        # Process each chunk sequentially (but within each chunk, process in parallel)
        for i, chunk in enumerate(serial_chunks):
            LOG.info(f"Processing chunk {i+1}/{len(serial_chunks)}: {chunk}")
            process_serial_chunk(chunk, args, TIME_EPOCH, TIME_DAYS, TIME_5MIN)

        # Create DataFrame equivalent and save to CSV
        if all_spm_data:
            LOG.info(f"Total collected {len(all_spm_data)} SPM records from all serial numbers")
            
            # Remove duplicates based on specific columns
            seen = set()
            unique_data = []
            for item in all_spm_data:
                key = (item["HostNickname"], item["ArrayPort"], item["HostWWPN"])
                if key not in seen:
                    seen.add(key)
                    unique_data.append(item)
            
            LOG.info(f"After deduplication: {len(unique_data)} unique records")
            
            # Calculate percentage utilization
            for row in unique_data:
                if "MonitorKBps" in row and "SPMLimitKBps" in row:
                    row["SPMLimitUtilPct"] = calculate_percentage(
                        row["MonitorKBps"], row["SPMLimitKBps"]
                    )
                else:
                    LOG.warning("Required columns 'MonitorKBps' and 'SPMLimitKBps' are missing in some records.")
            
            # Generate filename with all processed serials
            all_serials_str = "_".join(serial_numbers)
            csv_filename = f"hds_iolimit_report_{args.region}_{all_serials_str}_{TIME_DAYS}.csv"
            
            # Write CSV file
            fieldnames = [
                "HostNickname", "ArrayPort", "HostGroup", "HostWWPN", "ArraySerial",
                "MonitorIOps", "MonitorKBps", "SPMLimitKBps", "SPMPriority", 
                "SPMLimitUtilPct", "SourceLoadTimeEpoch", "SourceName", "BatchCreateTimeEpoch"
            ]
            
            write_csv_file(csv_filename, unique_data, fieldnames)
            
            # Create metadata file
            meta_filename = f"{csv_filename.replace('.csv', '')}_meta.json"
            
            save_meta_json(meta_filename, int(TIME_EPOCH), "HostIOLimit", region)
            
            LOG.info(f"Report SPM written to file: {csv_filename}, metafile to {meta_filename}")
            
            # Send files to rem
