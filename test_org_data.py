#!/usr/bin/env python3
"""
Quick validation test for organizations-1000.csv
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from backend.connectors.s3_connector import S3Connector
from dq_engine.checks.null_check import check_nulls
from dotenv import load_dotenv

load_dotenv()

# Test organizations-1000.csv
config = {
    'bucket': 'hackathon-dq',
    'key': 'organizations-1000.csv',
    'file_format': 'csv',
    'region': 'ap-south-1'
}

print("Loading organizations-1000.csv...")
connector = S3Connector(config)
connector.connect()
df = connector.read_data()

print(f"\nLoaded {len(df)} rows")
print(f"Columns: {list(df.columns)}")

# Check for nulls
print("\nChecking for nulls...")
null_result = check_nulls(df)

print(f"\nNull Check Status: {null_result['status']}")
print(f"Total Nulls: {null_result['summary']['total_nulls']}")
if null_result['summary']['failed_columns']:
    print(f"Columns with nulls: {null_result['summary']['failed_columns']}")
else:
    print("✅ No null values found - data is clean!")

# Show actual null counts per column
print("\nPer-column null counts:")
for col in df.columns:
    null_count = df[col].isnull().sum()
    if null_count > 0:
        print(f"  {col}: {null_count} nulls")
