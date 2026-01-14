"""
Validation Service - Triggered by UI Configuration
"""
import sys
from pathlib import Path
from typing import Dict, Any

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from backend.connectors.s3_connector import S3Connector
from dq_engine.checks.null_check import check_nulls
from dq_engine.checks.duplicate_check import check_duplicates
from dq_engine.checks.freshness_check import check_freshness
from dq_engine.checks.volume_check import check_volume
from dq_engine.storage import StorageFactory
from datetime import datetime


def run_validation(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run validation based on configuration
    
    Args:
        config: Configuration dict with source_type, connection_details, etc.
        
    Returns:
        Dict with validation results
    """
    source_type = config['source_type']
    connection_details = config['connection_details']
    
    # Currently only S3 supported
    if source_type != 's3':
        raise ValueError(f"Source type {source_type} not yet implemented")
    
    # Load data from S3
    connector = S3Connector(connection_details)
    connector.connect()
    
    if not connector.test_connection():
        raise Exception("Failed to connect to S3")
    
    df = connector.read_data()
    
    # Get quality checks to run
    quality_checks = config.get('quality_checks', ['null_check', 'duplicate_check', 'freshness_check', 'volume_check'])
    if not quality_checks:
        quality_checks = ['null_check', 'duplicate_check', 'freshness_check', 'volume_check']  # Default to all
    
    print(f"DEBUG: quality_checks = {quality_checks}")  # DEBUG
    
    # Run quality checks
    results = {}
    current_count = len(df)
    
    print(f"DEBUG: Starting validation with {len(df)} rows")  # DEBUG
    
    # Null check (if selected)
    if 'null_check' in quality_checks:
        try:
            print("DEBUG: Running null_check...")
            columns = config.get('required_columns', list(df.columns))
            results['null_check'] = check_nulls(df, columns=columns)
            print(f"DEBUG: null_check completed - status: {results['null_check']['status']}")
        except Exception as e:
            print(f"ERROR in null_check: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    # Duplicate check (if selected)
    if 'duplicate_check' in quality_checks:
        try:
            print("DEBUG: Running duplicate_check...")
            primary_key = config.get('primary_key')
            if not primary_key:
                # Auto-detect
                for col in df.columns:
                    if 'id' in col.lower():
                        primary_key = col
                        break
                if not primary_key:
                    primary_key = df.columns[0]
            
            results['duplicate_check'] = check_duplicates(df, primary_key=[primary_key])
            print(f"DEBUG: duplicate_check completed - status: {results['duplicate_check']['status']}")
        except Exception as e:
            print(f"ERROR in duplicate_check: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    # Freshness check (if selected)
    if 'freshness_check' in quality_checks:
        try:
            print("DEBUG: Running freshness_check...")
            timestamp_col = None
            for col in df.columns:
                if any(kw in col.lower() for kw in ['date', 'time', 'created', 'updated', 'timestamp']):
                    timestamp_col = col
                    break
            
            if timestamp_col:
                results['freshness_check'] = check_freshness(df, timestamp_column=timestamp_col, max_age_hours=24*365*10)
            else:
                results['freshness_check'] = {'status': 'SKIP', 'message': 'No timestamp column found'}
            print(f"DEBUG: freshness_check completed - status: {results['freshness_check']['status']}")
        except Exception as e:
            print(f"ERROR in freshness_check: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    # Volume check (if selected)
    if 'volume_check' in quality_checks:
        try:
            print("DEBUG: Running volume_check...")
            results['volume_check'] = check_volume(
                current_count=current_count,
                historical_counts=[],
                threshold_pct=20
            )
            print(f"DEBUG: volume_check completed - status: {results['volume_check']['status']}")
        except Exception as e:
            print(f"ERROR in volume_check: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    
    print(f"DEBUG: Completed checks. Results keys: {list(results.keys())}")  # DEBUG
    print(f"DEBUG: Results count: {len(results)}")  # DEBUG
    
    # Build result object
    source_id = f"{connection_details['bucket']}/{connection_details['key'].replace('.csv', '').replace('.parquet', '')}"
    
    result_data = {
        "timestamp": datetime.now().isoformat(),
        "dataset": connection_details['key'],
        "source": f"s3://{connection_details['bucket']}/{connection_details['key']}",
        "row_count": current_count,
        "config_name": config.get('name', 'unnamed'),
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
                "current_count": results['volume_check'].get('current_count', current_count),
                "message": results['volume_check'].get('message', '')
            }
        },
        "summary": {
            "total_checks": len(results),
            "passed": sum(1 for r in results.values() if r['status'] == 'PASS'),
            "failed": sum(1 for r in results.values() if r['status'] == 'FAIL'),
            "warnings": sum(1 for r in results.values() if r['status'] in ['WARNING', 'SKIP']),
            "quality_score": round((sum(1 for r in results.values() if r['status'] == 'PASS') / len(results)) * 100, 2) if len(results) > 0 else 0
        }
    }
    
    # Save using storage backend
    storage = StorageFactory.get_storage(source_type)
    success = storage.save_results(result_data, source_id)
    
    if not success:
        raise Exception("Failed to save validation results")
    
    return result_data
