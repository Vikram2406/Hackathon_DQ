"""
Validation Service - Triggered by UI Configuration
"""
import sys
from pathlib import Path
from typing import Dict, Any
import os as os_global

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
import pandas as pd


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
    
    # Load data from S3 (optionally with row limit to keep validation fast)
    try:
        connector = S3Connector(connection_details)
        connector.connect()
        
        if not connector.test_connection():
            # Create simple error without any formatting that might use os
            err_msg = "Failed to connect to S3"
            raise Exception(err_msg)
    except Exception as orig_error:
        # Re-raise with a simple message to avoid any string formatting issues
        raise Exception("Failed to connect to S3")
    
    # Allow max_rows in config to limit how many rows we read (for very large files)
    max_rows = config.get('max_rows', 10000)  # Default to 10k if not specified
    print(f"DEBUG: Reading up to {max_rows} rows from S3...")
    df = connector.read_data(limit=max_rows)
    print(f"DEBUG: Loaded {len(df)} rows from S3")
    
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
            # Use required_columns if provided and non-empty, otherwise use all columns
            columns = config.get('required_columns') or list(df.columns)
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
                historical_counts=[],  # No historical data yet
                threshold_pct=20
            )
            print(f"DEBUG: volume_check completed - status: {results['volume_check']['status']}")
        except ZeroDivisionError as e:
            # Gracefully handle any division-by-zero inside volume logic
            print(f"WARNING in volume_check (division by zero): {e}")
            results['volume_check'] = {
                'check_type': 'volume_check',
                'status': 'WARNING',
                'message': 'Volume check unavailable due to insufficient historical data',
                'current_count': current_count
            }
        except Exception as e:
            print(f"ERROR in volume_check: {e}")
            import traceback
            traceback.print_exc()
            # Fallback to a warning instead of failing entire validation
            results['volume_check'] = {
                'check_type': 'volume_check',
                'status': 'WARNING',
                'message': f'Volume check error: {e}',
                'current_count': current_count
            }
    
    
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
    
    # Run agentic data quality agents
    try:
        from agents.orchestrator import AgentsOrchestrator
        from agents.llm_provider import LLMProviderFactory, LLMProvider
        from config import settings
        # os is already imported at module level - don't import again
        
        # Initialize orchestrator with LLM client if available
        llm_client = None
        try:
            # Load environment variables from .env file if not already loaded
            from dotenv import load_dotenv
            from pathlib import Path
            
            # Try to load from root .env first, then backend/.env
            project_root = Path(__file__).parent.parent.parent
            root_env = project_root / '.env'
            backend_env = Path(__file__).parent.parent / '.env'
            
            if root_env.exists():
                load_dotenv(root_env)
            if backend_env.exists():
                load_dotenv(backend_env, override=False)
            
            # Load Gemini API key from environment (from .env file or system env)
            gemini_key = os_global.getenv('GEMINI_API_KEY') or os_global.getenv('GOOGLE_API_KEY')
            if gemini_key:
                os_global.environ['GOOGLE_API_KEY'] = gemini_key
                os_global.environ['GEMINI_API_KEY'] = gemini_key
                print(f"✅ ValidationService: Gemini API key loaded from environment")
            else:
                print("⚠️ ValidationService: Warning - GEMINI_API_KEY or GOOGLE_API_KEY not found")
            
            # Set LLM provider (can be overridden by LLM_PROVIDER env var)
            llm_provider = os_global.getenv('LLM_PROVIDER', 'gemini').lower()
            os_global.environ['LLM_PROVIDER'] = llm_provider
            print(f"✅ ValidationService: LLM Provider set to: {llm_provider}")
            
            # Create LLM client using factory
            llm_client = LLMProviderFactory.create_llm_client()
            provider = LLMProviderFactory.get_provider()
            print(f"✅ Initialized {provider.value.upper()} LLM client for agents")
        except Exception as e:
            error_str = str(e)
            if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str or 'quota' in error_str.lower():
                print(f"❌ LLM API quota exhausted! Cannot initialize LLM client for agents.")
            else:
                print(f"⚠️ Could not initialize LLM client for agents: {e}")
                import traceback
                traceback.print_exc()
        
        orchestrator = AgentsOrchestrator(llm_client=llm_client)
        
        # Convert DataFrame to list of dicts for agents (sample if too large)
        sample_size = 1000
        if len(df) > sample_size:
            df_sample = df.head(sample_size)
            dataset_rows = df_sample.to_dict('records')
        else:
            dataset_rows = df.to_dict('records')
        
        # Run agents with progress indication
        print(f"🔄 Running agentic data quality agents on {len(dataset_rows)} rows...")
        print(f"   This may take 30-120 seconds depending on data size and API availability.")
        agentic_results = orchestrator.run(
            validation_result=result_data,
            dataset_rows=dataset_rows,
            sample_size=sample_size
        )
        
        agentic_issues = agentic_results.get('agentic_issues', [])
        agentic_summary = agentic_results.get('agentic_summary', {})
        
        print(f"✅ Agentic agents completed: {len(agentic_issues)} issues found")
        
        # Debug: Print issue categories
        if agentic_issues:
            categories = {}
            for issue in agentic_issues:
                cat = issue.get('category', 'Unknown')
                categories[cat] = categories.get(cat, 0) + 1
            print(f"DEBUG: Issue categories: {categories}")
        
        # Attach agentic results to result_data
        result_data['agentic_issues'] = agentic_issues
        result_data['agentic_summary'] = agentic_summary
        
        print(f"DEBUG: Saved {len(agentic_issues)} issues to result_data")
        print(f"DEBUG: Result_data keys: {list(result_data.keys())}")
        print(f"DEBUG: Result_data['dataset']: {result_data.get('dataset', 'N/A')}")
    except Exception as e:
        print(f"⚠️ Error running agentic agents: {e}")
        import traceback
        traceback.print_exc()
        # Continue without agentic results
        result_data['agentic_issues'] = []
        result_data['agentic_summary'] = {}
    
    # Persisting results is optional.
    # By default we do NOT store validation JSON history; we only generate issues live.
    persist_results = config.get("persist_results", False)
    if persist_results:
        storage = StorageFactory.get_storage(source_type)
        success = storage.save_results(result_data, source_id)
        if not success:
            raise Exception("Failed to save validation results")
    else:
        print("DEBUG: persist_results=False; skipping save_results (no validation history stored)")
    
    return result_data
