def parse_endpoint_data(result: Dict[str, Any], endpoint: str, device_info: Dict[str, Any], columns_config: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    """
    Parse endpoint data based on endpoint type and configuration
    
    Args:
        result: API response data
        endpoint: Endpoint name
        device_info: Basic device information (should contain current device details)
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

    # Ensure we have the correct device information
    base_info = {
        "Id": str(device_info.get("Id", "")),
        "OmeId": str(device_info.get("OmeId", "")),
        "SourceLoadTimepoch": int(device_info.get("SourceLoadTimepoch", 0)),
        "DeviceServiceTag": str(device_info.get("DeviceServiceTag", ""))
    }

    
    if category == 'serverNetworkInterfaces':
        if 'InventoryInfo' in result:
            for nic in result['InventoryInfo']:
                nic_id = nic.get('NicId', '')
                vendor = nic.get('VendorName', '')
                
                for port in nic.get('Ports', []):
                    for partition in port.get('Partitions', []):
                        nic_data = {
                            **base_info,  # Spread base_info first
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
                            'MinBandwidth': str(partition.get('MinBandwidth', '')),
                            'MaxBandwidth': str(partition.get('MaxBandwidth', ''))
                        }
                        parsed_data.append(nic_data)
    elif category == 'Temperature':
        temp_data = {
            **base_info,
            'peakTemperatureUnit': str(result.get('peakTemperatureUnit', '')),
            'avgTemperatureUnit': str(result.get('avgTemperatureUnit', '')),
            'instantaneousTemperatureUnit': str(result.get('instantaneousTemperatureUnit', '')),
            'avgTemperatureTimeStamp': str(result.get('avgTemperatureTimeStamp', '')),
            'instantaneousTemperature': str(result.get('instantaneousTemperature', '')),
            'DateFormat': str(result.get('DateFormat', ''))
        }
        parsed_data.append(temp_data)
        
    elif category == 'serverProcessors':
        for processor in result.get('value', []):
            proc_data = {
                **base_info,  # Spread base_info first
                'BrandName': str(processor.get('BrandName', '')),
                'ModelName': str(processor.get('ModelName', '')),
                'MaxSpeed': str(processor.get('MaxSpeed', '')),
                'NumberOfCores': str(processor.get('NumberOfCores', '')),
                'Status': str(processor.get('Status', ''))
            }
            parsed_data.append(proc_data)

    elif category == 'SystemUptime':
        uptime_data = {
            **base_info,  # Spread base_info first
            'systemUpTime': str(result.get('systemUpTime', ''))
        }
        parsed_data.append(uptime_data)

    # Filter data based on configured columns and ensure all columns exist
    filtered_data = []
    for record in parsed_data:
        filtered_record = {}
        for col in columns_config[category]:
            filtered_record[col] = record.get(col, '')  # Use empty string as default
        filtered_data.append(filtered_record)

    return filtered_data

def main():
    try:
        # Load column configurations
        columns_config = load_config()
        
        # Get session token
        session_token = get_session_token()
        
        # Get device list
        inventory_base = get_devices(session_token, CHUNK_SIZE)
        
        # Save inventory_base to CSV
        save_to_csv(inventory_base, "inventory_base.csv", columns_config['Inventory_base'])
        
        # Create a dictionary to store data for each category
        endpoint_data = {category: [] for category in columns_config.keys()}
        
        # Get device details in parallel
        with ThreadPoolExecutor(max_workers=BATCH_SIZE) as executor:
            futures = []
            
            # Create futures with device info
            for device in inventory_base:
                for endpoint in ENDPOINTS:
                    futures.append((endpoint, device, executor.submit(get_device_details_with_retry, 
                                                                   session_token, 
                                                                   device["Id"], 
                                                                   endpoint)))
            
            # Process results
            for endpoint, device, future in futures:
                result = future.result()
                if result:
                    category = None
                    for cat in ['serverProcessors', 'serverNetworkInterfaces', 'SystemUptime', 'Power', 'Temperature']:
                        if cat in endpoint:
                            category = cat
                            break
                    
                    if category:
                        parsed_data = parse_endpoint_data(result, endpoint, device, columns_config)
                        if parsed_data:
                            endpoint_data[category].extend(parsed_data)
            
            # Save parsed data to CSV files
            for category, data in endpoint_data.items():
                if data and category in columns_config:
                    output_file = f"{category}.csv"
                    save_to_csv(data, output_file, columns_config[category])
                    logger.info(f"Saved {len(data)} records to {output_file}")

    except Exception as e:
        logger.critical(f"Critical error: {e}", exc_info=True)
