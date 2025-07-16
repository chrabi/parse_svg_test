#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raidcom IOLimit data collection script - Version 36
Compatible with Python 3.6.8 with pandas for data processing
Includes multithreading and immediate SCP transfer per file
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
import pandas as pd
import io
from typing import List, Dict, Tuple

# Setup logging with thread safety
LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')

# Global data structures with thread safety
all_spm_data = []
data_lock = threading.Lock()

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
    elif format_type == "hours_min":
        return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def get_file_path(array_serial):
    """Get the file path for array serial"""
    report_dir = "reports"
    DIR_TIME_5MIN = get_timestamp("5min")
    out_dir = os.path.join(report_dir, DIR_TIME_5MIN)
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    
    # Create a directory for the specific array serial
    array_dir = os.path.join(out_dir, array_serial)
    if not os.path.exists(array_dir):
        os.makedirs(array_dir, exist_ok=True)
    
    return array_dir

def simple_encrypt(text, key="default_key"):
    """Simple base64 encoding"""
    try:
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
        LOG.error(f"Error message: {e.stderr}")
        return None

def send_scp_file(user, file_path, remote_server, remote_path, scp_key):
    """Send file to remote server using SCP - Thread-safe version"""
    try:
        # Thread-safe remote directory creation
        remote_mkdir_command = f"ssh -i {scp_key} {user}@{remote_server} mkdir -p {remote_path}"
        LOG.info(f"[{threading.current_thread().name}] Creating remote directory: {remote_path}")
        
        result = subprocess.run(
            remote_mkdir_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )

        # SCP command
        scp_command = f"scp -i {scp_key} {file_path} {user}@{remote_server}:{remote_path}"
        LOG.info(f"[{threading.current_thread().name}] Sending file: {file_path} to {remote_server}:{remote_path}")
        
        result = subprocess.run(
            scp_command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
        )
        
        LOG.info(f"[{threading.current_thread().name}] ✅ File {file_path} sent successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        LOG.error(f"[{threading.current_thread().name}] ❌ Error sending file {file_path}: {e.stderr}")
        return False

def save_meta_json(filename, batch_epoch, column, region):
    """Save metadata to JSON file"""
    data = {
        "db_name": f"SAN_INFO_{region.upper()}",
        "table_schema": "Hitachi",
        "table_name": column,
        "sql_timestamp": datetime.datetime.now().isoformat(),
        "index_key": "ArraySerial,ArrayPort,HostNickname",
        "staging_db": "STAGING",
        "batch_epoch_identifier": batch_epoch,
        "processed": True,
        "force_convert_dtypes": None,
        "delimiter": ",",
    }
    
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def parse_raidcom_output_with_pandas(content: str, separator: str = " ") -> pd.DataFrame:
    """Parse raidcom output using pandas"""
    if not content or not content.strip():
        return pd.DataFrame()
    
    # Use io.StringIO to create file-like object from string
    string_buffer = io.StringIO(content)
    
    try:
        # Read with pandas, handling various separators
        df = pd.read_csv(string_buffer, sep=r'\s+', engine='python')
        return df
    except Exception as e:
        LOG.error(f"Error parsing with pandas: {e}")
        return pd.DataFrame()

def get_serial_numbers(instance):
    """Get all serial numbers from raidqry -l command"""
    try:
        cmd = f"raidqry -l -I{instance}"
        LOG.info(f"Getting serial numbers with command: {cmd}")
        
        output = run_command(cmd)
        if not output:
            return []
        
        # Extract serial numbers (5-digit numbers)
        serial_numbers = re.findall(r'\b\d{5}\b', output)
        
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

def calculate_percentage(monitor_kbps, spml_limit_kbps):
    """Calculate percentage with 2 decimal places"""
    try:
        if monitor_kbps and spml_limit_kbps and float(spml_limit_kbps) > 0:
            return round((float(monitor_kbps) / float(spml_limit_kbps)) * 100, 2)
        else:
            return 0
    except (ValueError, ZeroDivisionError):
        return 0

def process_and_save_serial_data(serial_number: str, data_list: List[Dict], 
                               TIME_EPOCH: str, region: str, scp_config: Dict):
    """Process data for a single serial and save to CSV/JSON with immediate SCP transfer"""
    if not data_list:
        LOG.warning(f"No data to save for serial {serial_number}")
        return
    
    try:
        # Convert to pandas DataFrame
        df = pd.DataFrame(data_list)
        
        # Add calculated percentage column
        if 'MonitorKBps' in df.columns and 'SPMLimitKBps' in df.columns:
            df['PercentageUsed'] = df.apply(
                lambda row: calculate_percentage(row['MonitorKBps'], row['SPMLimitKBps']), 
                axis=1
            )
        
        # Sort by ArrayPort and HostNickname
        if 'ArrayPort' in df.columns and 'HostNickname' in df.columns:
            df = df.sort_values(['ArrayPort', 'HostNickname'])
        
        # Get file paths
        array_dir = get_file_path(serial_number)
        csv_filename = f"HostIOLimit_{serial_number}_{TIME_EPOCH}.csv"
        json_filename = f"HostIOLimit_{serial_number}_{TIME_EPOCH}.json"
        
        csv_path = os.path.join(array_dir, csv_filename)
        json_path = os.path.join(array_dir, json_filename)
        
        # Save CSV using pandas
        df.to_csv(csv_path, index=False)
        LOG.info(f"[{threading.current_thread().name}] 💾 Saved CSV: {csv_path}")
        
        # Save metadata JSON
        save_meta_json(json_path, TIME_EPOCH, "HostIOLimit", region)
        LOG.info(f"[{threading.current_thread().name}] 💾 Saved JSON: {json_path}")
        
        # Send files via SCP immediately
        if scp_config:
            remote_array_dir = os.path.join(scp_config['scp_path'], get_timestamp("5min"), serial_number)
            
            # Send CSV
            send_scp_file(
                scp_config['scp_user'], 
                csv_path, 
                scp_config['scp_server'], 
                remote_array_dir, 
                scp_config['scp_key']
            )
            
            # Send JSON
            send_scp_file(
                scp_config['scp_user'], 
                json_path, 
                scp_config['scp_server'], 
                remote_array_dir, 
                scp_config['scp_key']
            )
        
    except Exception as e:
        LOG.error(f"[{threading.current_thread().name}] Error saving data for serial {serial_number}: {e}")

def process_serial_number(serial_number: str, args, TIME_EPOCH: str, TIME_DAYS: str, 
                         TIME_5MIN: str, scp_config: Dict):
    """Process a single serial number to collect SPM data"""
    thread_name = threading.current_thread().name
    LOG.info(f"[{thread_name}] 🔄 Processing serial number: {serial_number}")
    
    try:
        local_spm_data = []
        
        # Get port information for this serial number
        port_cmd = f"raidcom get port -s {serial_number} -IH{{{args.inst}}}"
        LOG.info(f"[{thread_name}] Getting ports for serial {serial_number}")
        
        port_output = run_command(port_cmd)
        if not port_output:
            LOG.warning(f"[{thread_name}] No port data for serial {serial_number}")
            return
        
        # Parse port output with pandas
        port_df = parse_raidcom_output_with_pandas(port_output)
        
        # Extract port IDs
        port_ids = []
        if not port_df.empty and 'PORT' in port_df.columns:
            port_ids = port_df['PORT'].tolist()
        else:
            # Fallback to regex
            port_ids = re.findall(r'CL\d-[A-Z]', port_output)
        
        LOG.info(f"[{thread_name}] Found {len(port_ids)} ports for serial {serial_number}")
        
        # Process each port
        for port_id in port_ids:
            LOG.info(f"[{thread_name}] Checking port {port_id}...")
            
            # Get host group information
            hg_cmd = f"raidcom get host_grp -port {port_id} -s {serial_number} -IH{{{args.inst}}}"
            hg_output = run_command(hg_cmd)
            
            if not hg_output:
                continue
            
            # Parse host groups with pandas
            hg_df = parse_raidcom_output_with_pandas(hg_output)
            
            if hg_df.empty:
                continue
            
            # Get SPM WWN information
            spm_cmd = f"raidcom get spm_wwn -port {port_id} -s {serial_number} -IH{{{args.inst}}}"
            spm_output = run_command(spm_cmd)
            
            if not spm_output:
                continue
            
            # Parse SPM data with pandas
            spm_df = parse_raidcom_output_with_pandas(spm_output)
            
            if not spm_df.empty:
                # Process SPM data
                for idx, row in spm_df.iterrows():
                    try:
                        spm_entry = {
                            "ArraySerial": serial_number,
                            "ArrayPort": port_id,
                            "HostNickname": row.get('NICK_NAME', 'Unknown'),
                            "HostGroup": str(row.get('GROUP', '')),
                            "HostWWPN": row.get('HBA_WWN', ''),
                            "MonitorIOps": int(row.get('IOPS', 0)),
                            "MonitorKBps": int(row.get('KBPS', 0)),
                            "SPMLimitKBps": int(row.get('SPM_LIMIT', 0)),
                            "SPMPriority": str(row.get('PRIORITY', '')),
                            "SourceLoadTimeEpoch": TIME_EPOCH,
                            "SourceName": "HostIOLimit",
                            "BatchCreateTimeEpoch": TIME_EPOCH,
                        }
                        local_spm_data.append(spm_entry)
                    except Exception as e:
                        LOG.error(f"[{thread_name}] Error processing row: {e}")
        
        # Save data for this serial number immediately
        if local_spm_data:
            process_and_save_serial_data(
                serial_number, 
                local_spm_data, 
                TIME_EPOCH, 
                args.region, 
                scp_config
            )
            LOG.info(f"[{thread_name}] ✅ Completed serial {serial_number}: {len(local_spm_data)} records")
        else:
            LOG.warning(f"[{thread_name}] ⚠️ No data collected for serial {serial_number}")
        
    except Exception as e:
        LOG.error(f"[{thread_name}] ❌ Error processing serial {serial_number}: {e}")

def process_serial_chunk(serial_chunk: List[str], args, TIME_EPOCH: str, 
                        TIME_DAYS: str, TIME_5MIN: str, scp_config: Dict):
    """Process a chunk of serial numbers in parallel with 2 threads"""
    chunk_name = f"Chunk-{'-'.join(serial_chunk)}"
    LOG.info(f"[{chunk_name}] 📦 Starting chunk with {len(serial_chunk)} serials")
    
    # Create threads for each serial number in the chunk
    threads = []
    for serial_number in serial_chunk:
        thread = threading.Thread(
            target=process_serial_number,
            args=(serial_number, args, TIME_EPOCH, TIME_DAYS, TIME_5MIN, scp_config),
            name=f"Serial-{serial_number}"
        )
        threads.append(thread)
        thread.start()
    
    # Wait for all threads in this chunk to complete
    for thread in threads:
        thread.join()
    
    LOG.info(f"[{chunk_name}] ✅ Completed chunk processing")

def show_directory_structure():
    """Show the generated directory structure"""
    if os.path.exists("reports"):
        LOG.info("📂 Generated directory structure:")
        for root, dirs, files in os.walk("reports"):
            level = root.replace("reports", "").count(os.sep)
            indent = "  " * level
            LOG.info(f"{indent}📁 {os.path.basename(root)}/")
            subindent = "  " * (level + 1)
            for file in files:
                file_size = os.path.getsize(os.path.join(root, file))
                LOG.info(f"{subindent}📄 {file} ({file_size} bytes)")

def main():
    try:
        LOG.info("🚀 Starting Raidcom IOLimit Script v36 - Python 3.6 + Pandas + Immediate SCP")
        
        # Parse arguments
        parser = argparse.ArgumentParser(
            description="Raidcom IOLimit data collection with pandas and immediate SCP transfer"
        )
        parser.add_argument(
            "--inst", type=int, required=True, help="Raidcom instance (e.g., 99)"
        )
        parser.add_argument(
            "--region",
            type=str,
            required=True,
            choices=["emea", "nam", "latam", "apac"],
            help="Region [emea,nam,apac,latam]",
        )
        parser.add_argument(
            "--username", type=str, required=False, help="Username for raidcom AD user"
        )
        parser.add_argument(
            "--password", type=str, required=False, help="Password for raidcom AD user"
        )

        args = parser.parse_args()
        region = args.region
        
        LOG.info(f"📊 Configuration: Instance={args.inst}, Region={region}")
        
        # Load configuration
        CONFIG = load_config_file("config/config.cfg")
        api_name = "HITACHI"
        
        # Get credentials and SCP configuration
        user_salt = CONFIG.get(api_name, "user_salt")
        password_salt = CONFIG.get(api_name, "password_salt")
        scp_user = CONFIG.get(api_name, "scp_user")
        scp_server = CONFIG.get(api_name, "scp_server")
        scp_name = "scp_path_" + args.region
        scp_path = CONFIG.get(api_name, scp_name)
        scp_key = CONFIG.get(api_name, "scp_key", fallback="/path/to/scp_key")
        
        # SCP configuration dictionary
        scp_config = {
            'scp_user': scp_user,
            'scp_server': scp_server,
            'scp_path': scp_path,
            'scp_key': scp_key
        }
        
        LOG.info(f"🔧 SCP Configuration: {scp_config['scp_server']}:{scp_config['scp_path']}")
        
        # Get credentials
        credentials = get_credentials(user_salt, password_salt, api_name)
        username = credentials[0]
        password = credentials[1]
        
        if not args.username:
            args.username = username
        if not args.password:
            args.password = password

        # Get all timestamps
        TIME_EPOCH = get_timestamp("epoch")
        TIME_DAYS = get_timestamp("days")
        TIME_5MIN = get_timestamp("5min")
        
        LOG.info(f"⏰ Timestamp: {TIME_5MIN} (Epoch: {TIME_EPOCH})")

        # Login to raidcom
        LOG.info(f"🔐 Login to raidcom with user: {args.username}")
        login_cmd = f"raidcom -login {args.username} {args.password} -I{args.inst}"
        
        login_result = run_command(login_cmd)
        if login_result is None:
            LOG.error("❌ Failed to login to raidcom")
            sys.exit(1)

        # Get all serial numbers
        LOG.info(f"🔍 Getting serial numbers from instance {args.inst}")
        serial_numbers = get_serial_numbers(args.inst)
        
        if not serial_numbers:
            LOG.error("❌ No serial numbers found")
            # Logout before exit
            run_command(f"raidcom -logout -I{args.inst}")
            sys.exit(1)

        LOG.info(f"📋 Found {len(serial_numbers)} serial numbers: {serial_numbers}")

        # Process in chunks of 2 with parallel threads
        chunk_size = 2
        serial_chunks = list(chunk_list(serial_numbers, chunk_size))
        
        LOG.info(f"📦 Processing {len(serial_chunks)} chunks ({chunk_size} serials per chunk)")
        LOG.info(f"🧵 Each chunk runs {chunk_size} threads in parallel")
        
        # Process each chunk
        for i, chunk in enumerate(serial_chunks):
            LOG.info(f"")
            LOG.info(f"{'='*60}")
            LOG.info(f"🔄 Processing chunk {i+1}/{len(serial_chunks)}: {chunk}")
            LOG.info(f"{'='*60}")
            
            process_serial_chunk(chunk, args, TIME_EPOCH, TIME_DAYS, TIME_5MIN, scp_config)
            
            LOG.info(f"✅ Completed chunk {i+1}/{len(serial_chunks)}")

        LOG.info(f"")
        LOG.info(f"{'='*60}")
        LOG.info("🎉 All processing completed!")
        LOG.info(f"{'='*60}")
        
        # Show final directory structure
        show_directory_structure()
        
        LOG.info(f"")
        LOG.info(f"📊 Summary:")
        LOG.info(f"   - Processed: {len(serial_numbers)} serial numbers")
        LOG.info(f"   - Chunks: {len(serial_chunks)}")
        LOG.info(f"   - Files location: reports/{TIME_5MIN}/")
        LOG.info(f"   - Remote location: {scp_config['scp_server']}:{scp_config['scp_path']}")

        # Logout from raidcom
        LOG.info(f"🔓 Logout from instance {args.inst}")
        logout_cmd = f"raidcom -logout -I{args.inst}"
        run_command(logout_cmd)
        
        LOG.info("✅ Script completed successfully!")

    except Exception as e:
        LOG.critical(f"💥 Critical error: {e}", exc_info=True)
        # Try to logout even on error
        try:
            run_command(f"raidcom -logout -I{args.inst}")
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
