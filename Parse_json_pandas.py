import json
import pandas as pd

def parse_json_fast():
   # Wczytaj json
    with open('test.json', 'r') as file:
        #data = json.load(file)
        df = pd.read_json(file)
   
    uptime = df[['OmeId', 'DeviceId', 'TimeEpoch', 'SerialNumber']]
    uptime['systemUptime'] = df['Uptime_data'].apply(lambda x: x.get('systemUptime') if isinstance(x, dict) else '')
    uptime.columns = ['OmeId', 'DeviceId', 'TimeEpoch', 'SerialNumber', 'systemUptime']

    # CPU DataFrame - Explode nested data using pandas operations
    cpu = df[['OmeId', 'DeviceId', 'TimeEpoch']]
    cpu_info = df['InventoryDetailsCpu_data'].apply(lambda x: x['InventoryInfo'] if isinstance(x, dict) else [])
    cpu = cpu.join(pd.json_normalize(cpu_info.explode().dropna()))
    cpu = cpu[['OmeId', 'DeviceId', 
                'TimeEpoch', 'Id', 'DiskNumber', 'ModelNumber', 
                'SerialNumber', 'Status']]

    cpu.to_csv('InventoryDetailsCpu.csv', index=False)
    uptime.to_csv('Uptime.csv', index=False)

parse_json_fast()
