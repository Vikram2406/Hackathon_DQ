#!/usr/bin/env python3
"""
Simple Data Quality Validation Script (No Database Dependencies)
Runs validation and shows results in terminal
"""
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from backend.connectors.s3_connector import S3Connector
from dq_engine.checks.null_check import check_nulls
from dq_engine.checks.duplicate_check import check_duplicates
from dq_engine.checks.freshness_check import check_freshness
from dq_engine.checks.volume_check import check_volume
from datetime import datetime
from dotenv import load_dotenv
import json
import boto3

# Load environment variables
load_dotenv()

def print_header(title):
    """Print section header"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80 + "\n")


def print_result(check_name, result):
    """Print check result"""
    status = result['status']
    emoji = "✅" if status == "PASS" else "❌" if status == "FAIL" else "⏭️"
    print(f"{emoji} {check_name}: {status}")
    
    if result.get('details'):
        print(f"   Details: {result['details']}")
    print()


def main():
    """Run data quality validation"""
    
    print("\n" + "🔍" * 40)
    print("DATA QUALITY VALIDATION - STANDALONE MODE")
    print("🔍" * 40)
    
    # Step 1: Load data from S3
    print_header("Step 1: Loading Data from S3")
    
    s3_config = {
        'bucket': 'hackathon-dq',
        'key': 'customers-10000.csv',
        'file_format': 'csv',
        'region': 'ap-south-1'
    }
    
    print(f"S3 Configuration:")
    print(f"  Bucket: {s3_config['bucket']}")
    print(f"  Key: {s3_config['key']}")
    print(f"  Region: {s3_config['region']}")
    print()
    
    try:
        connector = S3Connector(s3_config)
        connector.connect()
        
        if not connector.test_connection():
            print("❌ Failed to connect to S3")
            print(" Please check your AWS credentials in .env file")
            return
        
        print("✅ S3 connection successful")
        
        # Read data
        df = connector.read_data()
        print(f"✅ Loaded {len(df)} rows from S3")
        print(f"   Columns: {list(df.columns)}")
        print()
        print("First 5 rows:")
        print(df.head())
        
    except Exception as e:
        print(f"❌ Error loading data from S3: {str(e)}")
        return
    
    # Step 2: Run Quality Checks
    print_header("Step 2: Running Quality Checks")
    
    results = {}
    
    # 2.1 Null Check
    print("Running Null Check...")
    null_result = check_nulls(df, columns=list(df.columns))
    results['null_check'] = null_result
    print_result("Null Check", null_result)
    
    # 2.2 Duplicate Check
    print("Running Duplicate Check...")
    # Find ID column
    primary_key = None
    for col in df.columns:
        if 'id' in col.lower():
            primary_key = col
            break
    if not primary_key and len(df.columns) > 0:
        primary_key = df.columns[0]
    
    print(f"   Using primary key: {primary_key}")
    dup_result = check_duplicates(df, primary_key=[primary_key])
    results['duplicate_check'] = dup_result
    print_result("Duplicate Check", dup_result)
    
    # 2.3 Freshness Check
    print("Running Freshness Check...")
    timestamp_col = None
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['date', 'time', 'created', 'updated', 'timestamp']):
            timestamp_col = col
            break
    
    if timestamp_col:
        print(f"   Using timestamp column: {timestamp_col}")
        freshness_result = check_freshness(df, timestamp_column=timestamp_col, max_age_hours=24*365*10)  # Large number for old data
    else:
        print("   No timestamp column found, skipping")
        freshness_result = {'status': 'SKIP', 'details': {'message': 'No timestamp column'}}
    
    results['freshness_check'] = freshness_result
    print_result("Freshness Check", freshness_result)
    
    # 2.4 Volume Check
    print("Running Volume Check...")
    current_count = len(df)
    
    print(f"   Current count: {current_count}")
    print(f"   No historical data (first run)")
    
    volume_result = check_volume(
        current_count=current_count,
        historical_counts=[],
        threshold_pct=20
    )
    results['volume_check'] = volume_result
    print_result("Volume Check", volume_result)
    
    # Summary
    print_header("VALIDATION COMPLETE")
    
    print("Summary:")
    print(f"  Total Checks: {len(results)}")
    print(f"  Passed: {sum(1 for r in results.values() if r['status'] == 'PASS')}")
    print(f"  Failed: {sum(1 for r in results.values() if r['status'] == 'FAIL')}")
    print(f"  Skipped/Warning: {sum(1 for r in results.values() if r['status'] in ['SKIP', 'WARNING'])}")
    print()
    
    # Detailed results
    print("Detailed Results:")
    print("-" * 80)
    
    for check_name, result in results.items():
        print(f"\n{check_name.upper().replace('_', ' ')}:")
        print(f"  Status: {result['status']}")
        
        if check_name == 'null_check' and 'summary' in result:
            summary = result['summary']
            print(f"  Total Nulls: {summary.get('total_nulls', 0)}")
            if summary.get('failed_columns'):
                print(f"  Columns with Nulls: {', '.join(summary['failed_columns'])}")
        
        elif check_name == 'duplicate_check':
            print(f"  Duplicate Count: {result.get('duplicate_count', 0)}")
            print(f"  Duplicate %: {result.get('duplicate_percentage', 0)}%")
        
        elif check_name == 'freshness_check' and result['status'] != 'SKIP':
            print(f"  Latest Timestamp: {result.get('latest_timestamp', 'N/A')}")
            print(f"  Age (hours): {result.get('age_hours', 0)}")
        
        elif check_name == 'volume_check':
            print(f"  Current Count: {result.get('current_count', current_count)}")
            if result['status'] == 'WARNING':
                print(f"  Message: {result.get('message', 'No historical data')}")
    
    print("\n" + "=" * 80)
    print("✅ Data Quality Check Complete!")
    print("=" * 80)
    print()
    
    # Save results to storage
    try:
        save_results_to_storage(s3_config, results, current_count)
    except Exception as e:
        print(f"⚠️  Warning: Could not save results: {e}")
    
    print("Next Steps:")
    print("  • Results saved to S3 for AI chatbot reference")
    print("  • To enable database storage: Start Docker and PostgreSQL")
    print("  • To set up Airflow: Run ./setup_airflow.sh")
    print("  • To use the UI: Start backend and frontend services")
    print()


def save_results_to_storage(s3_config, results, row_count):
    """Save validation results using appropriate storage backend"""
    
    from dq_engine.storage import StorageFactory
    
    # Get source-specific storage
    storage = StorageFactory.get_storage('s3')
    
    # Create source identifier (bucket/key without .csv)
    source_id = f"{s3_config['bucket']}/{s3_config['key'].replace('.csv', '').replace('.parquet', '')}"
    
    # Build result object
    result_data = {
        "timestamp": datetime.now().isoformat(),
        "dataset": s3_config['key'],
        "source": f"s3://{s3_config['bucket']}/{s3_config['key']}",
        "row_count": row_count,
        "results": {
            "null_check": {
                "status": results['null_check']['status'],
                "total_nulls": results['null_check'].get('summary', {}).get('total_nulls', 0),
                "failed_columns": results['null_check'].get('summary', {}).get('failed_columns', [])
            },
            "duplicate_check": {
                "status": results['duplicate_check']['status'],
                "duplicate_count": results['duplicate_check'].get('duplicate_count', 0),
                "duplicate_percentage": results['duplicate_check'].get('duplicate_percentage', 0)
            },
            "freshness_check": {
                "status": results['freshness_check']['status'],
                "latest_timestamp": str(results['freshness_check'].get('latest_timestamp', 'N/A')),
                "age_hours": results['freshness_check'].get('age_hours', 0)
            },
            "volume_check": {
                "status": results['volume_check']['status'],
                "current_count": results['volume_check'].get('current_count', row_count),
                "message": results['volume_check'].get('message', '')
            }
        },
        "summary": {
            "total_checks": len(results),
            "passed": sum(1 for r in results.values() if r['status'] == 'PASS'),
            "failed": sum(1 for r in results.values() if r['status'] == 'FAIL'),
            "warnings": sum(1 for r in results.values() if r['status'] in ['WARNING', 'SKIP']),
            "quality_score": round((sum(1 for r in results.values() if r['status'] == 'PASS') / len(results)) * 100, 2)
        }
    }
    
    # Save using storage backend
    success = storage.save_results(result_data, source_id)
    
    if success:
        print(f"\n📦 Results saved to storage:")
        print(f"   Source Type: s3")
        print(f"   Source ID: {source_id}")
        print(f"   Storage: S3 (s3://project-cb/dq-reports/s3/{source_id}/)")
    else:
        print(f"\n⚠️ Warning: Failed to save results to storage")
    
    return success


if __name__ == "__main__":
    main()
