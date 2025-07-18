#!/usr/bin/env python3
"""
SPM Data Collector
Pobiera dane z systemu storage używając raidcom, parsuje wyniki, 
tworzy pliki CSV i meta, oraz kopiuje je przez SCP.
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
from io import StringIO

# Setup logging
LOG = logging.getLogger(__name__)

def setup_logging(config):
    """Konfiguracja systemu logowania"""
    log_level = config.get('LOGGING', 'level', fallback='INFO')
    log_format = config.get('LOGGING', 'format', 
                           fallback='%(asctime)s - %(levelname)s - %(message)s')
    log_filename = config.get('LOGGING', 'filename', 
                             fallback='spm_data_collector.log')
    
    # Utwórz katalog logs jeśli nie istnieje
    log_dir = config.get('PATHS', 'log_dir', fallback='logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file_path = os.path.join(log_dir, log_filename)
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format=log_format,
        filename=log_file_path
    )
    
    # Dodaj handler dla konsoli
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, log_level))
    formatter = logging.Formatter(log_format)
    console.setFormatter(formatter)
    LOG.addHandler(console)

def get_script_name():
    """Pobierz nazwę skryptu bez rozszerzenia"""
    script_name = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    return script_name

def get_timestamp(format_type="epoch"):
    """Pobierz aktualny timestamp w różnych formatach"""
    if format_type == "epoch":
        return int(datetime.datetime.now().timestamp())
    elif format_type == "hours_min":
        return datetime.datetime.now().strftime("%Y%m%d%H%M")
    elif format_type == "days":
        return datetime.datetime.now().strftime("%Y-%m-%d")
    elif format_type == "5min":
        # Zaokrąglij do najbliższych 5 minut
        now = datetime.datetime.now()
        minutes = now.minute - (now.minute % 5)
        return now.replace(minute=minutes, second=0, microsecond=0).strftime("%Y%m%d%H%M")

def load_config(config_file):
    """Wczytaj konfigurację z pliku"""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config

def simple_encrypt(text, key="default_key"):
    """Proste kodowanie base64 (nie jest to prawdziwe szyfrowanie!)"""
    try:
        combined = f"{key}:{text}"
        encoded = base64.b64encode(combined.encode()).decode()
        return encoded
    except Exception as e:
        LOG.error(f"Error encoding: {e}")
        return text

def simple_decrypt(encoded_text, key="default_key"):
    """Proste dekodowanie base64"""
    try:
        decoded = base64.b64decode(encoded_text.encode()).decode()
        if decoded.startswith(f"{key}:"):
            return decoded[len(f"{key}:"):]
        return decoded
    except Exception as e:
        LOG.error(f"Error decoding: {e}")
        return encoded_text

def get_credentials(user_salt, password_salt, api_name):
    """Pobierz credentials z enkodowanymi danymi"""
    username = simple_decrypt(user_salt)
    password = simple_decrypt(password_salt)
    return username, password

def run_command(command):
    """Wykonaj polecenie shell i zwróć wynik"""
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        LOG.info(f"Command executed: {command}, Output: {result.stdout.strip()}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        LOG.error(f"Error executing command: {command}, Error message: {e.stderr}")
        sys.exit(1)

def get_file_path(array_serial):
    """Pobierz ścieżkę dla plików raportów"""
    report_dir = "reports"
    out_dir = os.path.join(report_dir, f"DIR_TIME_{get_timestamp('5min')}")
    
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    
    # Utwórz katalog dla konkretnego array serial
    array_dir = os.path.join(out_dir, array_serial)
    if not os.path.exists(array_dir):
        os.makedirs(array_dir, exist_ok=True)
    
    return os.path.join(array_dir)

def send_scp_file(user, file_path, remote_server, remote_path, scp_key):
    """Wyślij plik przez SCP"""
    try:
        # Przygotuj katalog na serwerze zdalnym
        remote_mkdir_command = f"ssh -i {scp_key} {user}@{remote_server} mkdir -p {remote_path}"
        LOG.info(f"Creating remote directory: {remote_path} on {remote_server}")
        run_command(remote_mkdir_command)
        
        # Skopiuj plik
        scp_command = f"scp -i {scp_key} {file_path} {user}@{remote_server}:{remote_path}"
        result = run_command(scp_command)
        LOG.info(f"SCP send: {file_path} to {remote_server}:{remote_path}")
        
        # Sprawdź czy plik został skopiowany
        remote_check_command = f"ssh -i {scp_key} {user}@{remote_server} ls {remote_path}"
        check_result = run_command(remote_check_command)
        
        if os.path.basename(file_path) in check_result:
            LOG.info(f"File {file_path} sent successfully")
            return True
        else:
            LOG.error(f"File {file_path} not found on remote server")
            return False
            
    except Exception as e:
        LOG.error(f"Error sending file via SCP: {e}")
        return False

def parse_spm_data(raw_data, array_serial):
    """Parsuj dane SPM z raidcom"""
    lines = raw_data.strip().split('\n')
    
    # Nagłówki kolumn
    headers = [
        "ArraySerial", "ArrayPort", "HostNickName", "HostGroup", 
        "HostWWPN", "MonitorIOps", "MonitorKBps", "SPMLimitKBps",
        "SPMPriority", "SourceLoadTimeEpoch", "SourceName", 
        "HostIOLimit", "BatchCreateTimeEpoch"
    ]
    
    parsed_data = []
    
    for line in lines[1:]:  # Pomiń pierwszy wiersz jeśli to nagłówek
        if not line.strip():
            continue
            
        # Parsuj linię - zakładam że dane są rozdzielone spacjami
        parts = line.split()
        if len(parts) >= 4:  # Minimalna liczba kolumn
            row_data = {
                "ArraySerial": array_serial,
                "ArrayPort": parts[0] if len(parts) > 0 else "",
                "HostNickName": parts[1] if len(parts) > 1 else "",
                "HostGroup": parts[2] if len(parts) > 2 else "",
                "HostWWPN": parts[3] if len(parts) > 3 else "",
                "MonitorIOps": int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0,
                "MonitorKBps": int(parts[5]) if len(parts) > 5 and parts[5].isdigit() else 0,
                "SPMLimitKBps": int(parts[6]) if len(parts) > 6 and parts[6].isdigit() else 0,
                "SPMPriority": int(parts[7]) if len(parts) > 7 and parts[7].isdigit() else 0,
                "SourceLoadTimeEpoch": get_timestamp("epoch"),
                "SourceName": "HostIOLimit",
                "HostIOLimit": "",
                "BatchCreateTimeEpoch": get_timestamp("epoch")
            }
            parsed_data.append(row_data)
    
    return parsed_data

def calculate_spm_limit_pct(data):
    """Oblicz SPMLimitPct dla każdego wiersza"""
    for row in data:
        monitor_kbps = row.get("MonitorKBps", 0)
        spm_limit_kbps = row.get("SPMLimitKBps", 0)
        
        if spm_limit_kbps > 0 and monitor_kbps > 0:
            row["SPMLimitPct"] = round((monitor_kbps / spm_limit_kbps) * 100, 2)
        else:
            row["SPMLimitPct"] = 0
    
    return data

def save_to_csv(data, filename, columns=None):
    """Zapisz dane do pliku CSV"""
    if not data:
        LOG.warning(f"No data to save to {filename}")
        return
    
    # Jeśli nie podano kolumn, użyj wszystkich kluczy z pierwszego wiersza
    if columns is None:
        columns = list(data[0].keys())
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data)
        LOG.info(f"Report SPM written to file: {filename}")
    except Exception as e:
        LOG.error(f"Error writing CSV file: {e}")

def save_meta_json(data, filename, region):
    """Zapisz plik meta w formacie JSON"""
    meta_data = {
        "timestamp": get_timestamp("epoch"),
        "region": region,
        "host_io_limit": "HostIOLimit",
        "source": "raidcom",
        "records_count": len(data)
    }
    
    try:
        with open(filename, 'w') as f:
            json.dump(meta_data, f, indent=2)
        LOG.info(f"Meta file written: {filename}")
    except Exception as e:
        LOG.error(f"Error writing meta file: {e}")

def get_storage_array_info(username, password, instance):
    """Pobierz informacje o storage array"""
    login_cmd = f"raidcom -login {username} {password} -I{instance}"
    run_command(login_cmd)
    
    # Pobierz serial number array
    raidqry_cmd = f"raidqry -l -I{instance}"
    raidqry_output = run_command(raidqry_cmd)
    
    # Parsuj serial number z wyniku
    for line in raidqry_output.split('\n'):
        if 'Serial#' in line:
            serial_number = line.split(':')[-1].strip()
            return serial_number
    
    return None

def process_spm_data(config, array_serial):
    """Główna funkcja przetwarzająca dane SPM"""
    # Pobierz dane z konfiguracji
    username = config.get('RAIDCOM', 'username')
    password = config.get('RAIDCOM', 'password')
    instance = config.get('RAIDCOM', 'instance')
    region = config.get('RAIDCOM', 'region')
    
    # Login do raidcom
    login_cmd = f"raidcom -login {username} {password} -I{instance}"
    run_command(login_cmd)
    
    # Pobierz dane SPM
    port_cmd = f"raidcom get port -s $(raidcom get port -s $(JFRAMESKEY) -IH{instance} | grep $(JFRAMEKEY)) -IH{instance}"
    port_output = run_command(port_cmd)
    
    # Parsuj dane
    spm_data = parse_spm_data(port_output, array_serial)
    
    # Oblicz SPMLimitPct
    spm_data = calculate_spm_limit_pct(spm_data)
    
    # Zapisz do CSV
    if spm_data:
        # Przygotuj ścieżki plików
        file_path = get_file_path(array_serial)
        csv_filename = os.path.join(file_path, f"spm_{get_timestamp('5min')}.csv")
        meta_filename = os.path.join(file_path, f"meta_{get_timestamp('5min')}.json")
        
        # Kolumny do CSV
        csv_columns = [
            "ArraySerial", "ArrayPort", "HostNickName", "HostGroup",
            "HostWWPN", "MonitorIOps", "MonitorKBps", "SPMLimitKBps",
            "SPMPriority", "SPMLimitPct", "SourceLoadTimeEpoch",
            "SourceName", "HostIOLimit", "BatchCreateTimeEpoch"
        ]
        
        # Zapisz pliki
        save_to_csv(spm_data, csv_filename, csv_columns)
        save_meta_json(spm_data, meta_filename, region)
        
        # Wyślij przez SCP
        scp_server = config.get('SCP', 'remote_server')
        scp_path = config.get('SCP', 'remote_path')
        scp_user = config.get('SCP', 'username')
        scp_key = config.get('SCP', 'scp_key', fallback='~/.ssh/id_rsa')
        
        send_scp_file(scp_user, csv_filename, scp_server, scp_path, scp_key)
        send_scp_file(scp_user, meta_filename, scp_server, scp_path, scp_key)
    
    # Logout z raidcom
    logout_cmd = f"raidcom -logout -I{instance}"
    run_command(logout_cmd)

def main():
    """Główna funkcja programu"""
    parser = argparse.ArgumentParser(description='SPM Data Collector')
    parser.add_argument('--config', default='config/config.cfg',
                       help='Path to configuration file')
    parser.add_argument('--array-serial', required=True,
                       help='Storage array serial number')
    
    args = parser.parse_args()
    
    # Wczytaj konfigurację
    config = load_config(args.config)
    
    # Setup logging
    setup_logging(config)
    
    LOG.info(f"Starting SPM data collection for array: {args.array_serial}")
    
    try:
        # Przetwórz dane SPM
        process_spm_data(config, args.array_serial)
        LOG.info("SPM data collection completed successfully")
    except Exception as e:
        LOG.error(f"Critical error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
