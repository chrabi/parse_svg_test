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


def parse_json_to_csv2():
   # Categories and their columns
   categories = {
       'inventory': ['OmeId', 'DeviceId', 'TimeEpoch', 'SerialNumber', 'Model', 'Name'],
       'Uptime': ['Uptime_OmeId', 'Uptime_DeviceId', 'Uptime_TimeEpoch', 'systemUptime'],
       'Temperature': ['Temperature_OmeId', 'Temperature_DeviceId', 'Temperature_TimeEpoch', 
                      'peakTemperature', 'avgTemperature', 'startTime', 'peakTemperatureTimeStamp'],
       'InventoryDetailsCpu': ['InventoryDetailsCpu_OmeId', 'InventoryDetailsCpu_DeviceId', 
                              'InventoryDetailsCpu_TimeEpoch', 'Id', 'DiskNumber', 'ModelNumber', 'Status']
   }

   # Read JSON
   #df = pd.read_json('text_2.json')
   df = pd.read_json('test.json')
   # Process inventory data
   inventory = pd.DataFrame(df[categories['inventory']].copy())
   inventory.to_csv('inventory.csv', index=False)

   # Process Uptime data
   uptime = pd.DataFrame(df[categories['Uptime'][:3]].copy())
   uptime.loc[:, 'systemUptime'] = df['Uptime_data'].apply(
       lambda x: x.get('systemUptime') if isinstance(x, dict) else ''
   )
   uptime.to_csv('Uptime.csv', index=False)

   # Process Temperature data
   temp = pd.DataFrame(df[categories['Temperature'][:3]].copy())
   temp_data = df['Temperature_data'].apply(
       lambda x: {k: x.get(k, '') for k in ['peakTemperature', 'avgTemperature', 
                                             'startTime', 'peakTemperatureTimeStamp']} 
       if isinstance(x, dict) else {k: '' for k in ['peakTemperature', 'avgTemperature', 
                                                     'startTime', 'peakTemperatureTimeStamp']}
   )
   temp = temp.join(pd.json_normalize(temp_data))
   temp.to_csv('Temperature.csv', index=False)

   # Process CPU data
   cpu = pd.DataFrame(df[categories['InventoryDetailsCpu'][:3]].copy())
   cpu_info = df['InventoryDetailsCpu_data'].apply(
       lambda x: x['InventoryInfo'] if isinstance(x, dict) else []
   )
   cpu = cpu.join(pd.json_normalize(cpu_info.explode().dropna())[
       ['Id', 'DiskNumber', 'ModelNumber', 'Status']
   ])
   cpu.to_csv('InventoryDetailsCpu.csv', index=False)

parse_json_to_csv2()

