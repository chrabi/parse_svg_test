# CSV Power Data Processor - Usage Instructions

This script reads serial numbers from a CSV file, queries MSSQL databases for power data (HP OneView and Dell OME), and adds the query results back to the same CSV file with appropriate prefixes.

## Prerequisites

1. Python 3.12
2. Required packages: 
   - pandas
   - pyodbc

Install required packages with:
```
pip install pandas pyodbc
```

3. ODBC Driver for SQL Server (version 17 recommended)
4. Access to the MSSQL server with proper credentials

## Configuration

Before running the script, configure the database connection and query settings in `config.cfg`:

```ini
[database]
server = YOUR_SQL_SERVER
database = YOUR_DATABASE
username = YOUR_USERNAME
password = YOUR_PASSWORD
driver = ODBC Driver 17 for SQL Server

[processing]
max_workers = 8
timeout_seconds = 60

[queries]
ov_query = SELECT [DeviceName],[Model],[AveragePower],[PeakPower24h] FROM [STAGING].[OV].[ServerPower] WHERE SerialNumber LIKE (?)
ome_query = SELECT [DeviceSerialNumber],[Model],[AvgPower],[peakPower] FROM [STAGING].[OME].[ServerPower] WHERE DeviceSerialNumber LIKE (?) ORDER BY Timestamp DESC
```

Adjust the settings according to your environment:
- Change the database connection details
- Modify the number of concurrent workers (threads) as needed
- Update the SQL queries if your database schema differs

## Input CSV Format

The script expects a CSV file with at least a `serialNumber` column. For example:

```
assetTag,deviceName,serialNumber,model,manufacturer
ABC123,server-01,SER123456,PowerEdge R740,Dell
DEF456,server-02,SER789012,PowerEdge R740,Dell
```

## Running the Script

Run the script with:

```
python power_data_processor.py input.csv
```

Or specify a custom config file:

```
python power_data_processor.py input.csv --config my_config.cfg
```

## Output

The script will update the input CSV file, adding columns with the following prefixes:
- `OV_` for data from the HP OneView database
- `OME_` for data from the Dell OME database

For example, the output CSV might look like:

```
assetTag,deviceName,serialNumber,model,manufacturer,OV_DeviceName,OV_Model,OV_AveragePower,OV_PeakPower24h,OME_DeviceSerialNumber,OME_Model,OME_AvgPower,OME_peakPower
ABC123,server-01,SER123456,PowerEdge R740,Dell,server-01,PowerEdge R740,450,520,SER123456,PowerEdge R740,460,525
DEF456,server-02,SER789012,PowerEdge R740,Dell,server-02,PowerEdge R740,400,480,SER789012,PowerEdge R740,410,490
```

## Logging

The script creates log files in the `logs` directory. Each log file is named with the script name and timestamp.

The logs include:
- Information about the processing progress
- Errors encountered during execution
- Summary of results

## Troubleshooting

If you encounter issues:

1. Check the log file for detailed error messages
2. Verify the database connection details in config.cfg
3. Ensure the SQL queries are compatible with your database schema
4. Make sure the input CSV has a column named 'serialNumber'
5. Verify that the ODBC driver is installed correctly
