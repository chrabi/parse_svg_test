import configparser
import json
from typing import Dict, List, Any

def load_config() -> Dict[str, List[str]]:
    """Load column configurations from config file"""
    config = configparser.ConfigParser()
    config.read('config.cfg')
    
    columns_config = {}
    for section in config.sections():
        if section.startswith('COLUMNS_'):
            category = section.replace('COLUMNS_', '')
            columns_config[category] = json.loads(config.get(section, 'columns'))
    
    return columns_config

def parse_endpoint_data(result: Dict[str, Any], endpoint: str, device_info: Dict[str, Any], columns_config: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """
    Parse endpoint data based on endpoint type and configuration
    
    Args:
        result: API response data
        endpoint: Endpoint name
        device_info: Basic device information
        columns_config: Column configuration for each category
    
    Returns:
        List of parsed data records
    """
    parsed_data = []
    
    # Extract category from endpoint
    category = None
    if 'serverProcessors' in endpoint:
        category = 'serverProcessors'
    elif 'serverNetworkInterfaces' in endpoint:
        category = 'serverNetworkInterfaces'
    elif 'SystemUptime' in endpoint:
        category = 'SystemUptime'
    elif 'Power' in endpoint:
        category = 'Power'
    elif 'Temperature' in endpoint:
        category = 'Temperature'
    
    if not category or category not in columns_config:
        logger.warning(f"Unknown or unconfigured category: {category}")
        return []

    # Base device information
    base_info = {
        "Id": device_info["Id"],
        "OmeId": device_info["OmeId"],
        "SourceLoadTimepoch": device_info["SourceLoadTimepoch"],
        "DeviceServiceTag": device_info["DeviceServiceTag"]
    }

    if category == 'serverNetworkInterfaces':
        if 'InventoryInfo' in result:
            for nic in result['InventoryInfo']:
                nic_id = nic.get('NicId', '')
                vendor = nic.get('VendorName', '')
                
                for port in nic.get('Ports', []):
                    for partition in port.get('Partitions', []):
                        nic_data = {
                            **base_info,
                            'NicId': nic_id,
                            'VendorName': vendor,
                            'PortId': port.get('PortsId', ''),
                            'IpAddress': port.get('InitiatorIpAdress', ''),
                            'SubnetMask': port.get('InitiatorSubMask', ''),
                            'LinkStatus': port.get('LinkStatus', ''),
                            'LinkSpeed': port.get('LinkSpeed', ''),
                            'Fqdn': partition.get('Fqdn', ''),
                            'CurrentMacAddress': partition.get('CurrentMacAdress', ''),
                            'VirtualMacAddress': partition.get('VirtualMacAdress', ''),
                            'NicMode': partition.get('NicMode', ''),
                            'MinBandwidth': partition.get('MinBandwidth', ''),
                            'MaxBandwidth': partition.get('MaxBandwidth', '')
                        }
                        parsed_data.append(nic_data)

    elif category == 'serverProcessors':
        for processor in result.get('value', []):
            proc_data = {
                **base_info,
                'BrandName': processor.get('BrandName', ''),
                'ModelName': processor.get('ModelName', ''),
                'MaxSpeed': processor.get('MaxSpeed', ''),
                'NumberOfCores': processor.get('NumberOfCores', ''),
                'Status': processor.get('Status', '')
            }
            parsed_data.append(proc_data)

    elif category == 'SystemUptime':
        uptime_data = {
            **base_info,
            'systemUpTime': result.get('systemUpTime', '')
        }
        parsed_data.append(uptime_data)

    # Filter data based on configured columns
    filtered_data = []
    for record in parsed_data:
        filtered_record = {col: record.get(col, '') for col in columns_config[category]}
        filtered_data.append(filtered_record)

    return filtered_data
