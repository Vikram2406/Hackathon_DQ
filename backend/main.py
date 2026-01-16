"""
FastAPI Backend - Main Application
"""
import sys
import os
# Add parent directory to path to find chatbot module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Load environment variables from .env file
# Try to load from multiple locations: root .env, project .env, backend/.env
from dotenv import load_dotenv
import os
from pathlib import Path

# Try multiple .env file locations
backend_dir = Path(__file__).parent
project_root = backend_dir.parent
root_env = Path('/Users/kunal.khedkar/Desktop/Hackethon_bot/.env')  # Absolute path to root .env
project_env = project_root / '.env'
backend_env = backend_dir / '.env'

# Load .env files in priority order (first found wins, later ones don't override)
env_loaded = False
if root_env.exists():
    load_dotenv(root_env)
    print(f"✅ Loaded .env from root: {root_env}")
    env_loaded = True
if project_env.exists() and not env_loaded:
    load_dotenv(project_env)
    print(f"✅ Loaded .env from project: {project_env}")
    env_loaded = True
if backend_env.exists():
    load_dotenv(backend_env, override=False)  # Don't override if already loaded
    print(f"✅ Loaded .env from backend: {backend_env}")
    env_loaded = True

if not env_loaded:
    print("⚠️ No .env file found in any location")

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid
import json
import boto3
import re

from database import get_db, init_db
from models import (
    DatasetConfig, QualityMetric, AnomalyRecord, ChatHistory, DAGRun,
    DatasetConfigCreate, DatasetConfigResponse, QualityMetricResponse,
    AnomalyRecordResponse, ChatRequest, ChatResponse,
    DAGTriggerRequest, DAGTriggerResponse, HealthResponse,
    AgenticIssue, AgenticIssueSummary, ListAgentRunsResponse,
    ListAgentIssuesResponse, AgenticSummaryResponse, ApplyFixesRequest, ApplyFixesResponse
)
from config import settings
from agents.llm_provider import LLMProviderFactory, LLMProvider



# Initialize FastAPI app
app = FastAPI(
    title="Data Quality Platform API",
    description="AI-powered data quality automation platform",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize chatbot with configurable LLM provider
try:
    # Load Gemini API key from environment (from .env file or system env)
    gemini_key = os.getenv('GEMINI_API_KEY') or os.getenv('GOOGLE_API_KEY')
    if gemini_key:
        os.environ['GOOGLE_API_KEY'] = gemini_key
        os.environ['GEMINI_API_KEY'] = gemini_key
        print(f"✅ Gemini API key loaded from environment (length: {len(gemini_key)})")
    else:
        print("⚠️ Warning: GEMINI_API_KEY or GOOGLE_API_KEY not found in environment variables")

    # Set LLM provider to Gemini (can be overridden by LLM_PROVIDER env var)
    llm_provider = os.getenv('LLM_PROVIDER', 'gemini').lower()
    os.environ['LLM_PROVIDER'] = llm_provider
    print(f"✅ LLM Provider set to: {llm_provider}")
    
    query_engine = LLMProviderFactory.create_query_engine()
    provider = LLMProviderFactory.get_provider()
    print(f"✅ Chatbot initialized with {provider.value.upper()}")
except Exception as e:
    print(f"Warning: Could not initialize chatbot: {e}")
    import traceback
    traceback.print_exc()
    query_engine = None


# ==================== Helper Functions for File-Based Chat ====================

def extract_file_name_from_query(query: str) -> Optional[str]:
    """Extract file name or validation folder name from user query"""
    # Patterns to match validation folder names (e.g., "2026-01-13_19-58-10_validation")
    validation_patterns = [
        r'\b(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_validation)\b',  # "2026-01-13_19-58-10_validation"
        r'in\s+(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_validation)',  # "in 2026-01-13_19-58-10_validation"
        r'this\s+(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_validation)',  # "this 2026-01-13_19-58-10_validation"
    ]
    
    for pattern in validation_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            return match.group(1)  # Return validation folder name
    
    # Patterns to match file names with extensions
    file_patterns = [
        r'in\s+([\w\-]+\.(?:csv|json|parquet))',  # "in people-10000.csv"
        r'from\s+([\w\-]+\.(?:csv|json|parquet))',  # "from people-10000.csv"
        r'file\s+([\w\-]+\.(?:csv|json|parquet))',  # "file people-10000.csv"
        r'["\']([\w\-]+\.(?:csv|json|parquet))["\']',  # "people-10000.csv"
        r'\b([\w\-]+\.(?:csv|json|parquet))\b',  # people-10000.csv (anywhere)
    ]
    
    for pattern in file_patterns:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            file_name = match.group(1)
            # Make sure we got a reasonable file name (not just "file.csv")
            if len(file_name.split('.')[0]) > 2:  # At least 3 chars before extension
                return file_name
    
    return None


def search_file_in_s3(file_name: str) -> Optional[Dict[str, Any]]:
    """Search for a file in S3 and return its data"""
    try:
        # Get S3 configuration
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=settings.aws_default_region or os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        
        # Search in results bucket
        results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/')
        
        # Check if it's a validation folder name (e.g., "2026-01-13_19-58-10_validation")
        is_validation_folder = '_validation' in file_name and re.match(r'\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_validation', file_name)
        
        if is_validation_folder:
            # Extract timestamp from validation folder name
            # "2026-01-13_19-58-10_validation" -> "2026-01-13_19-58-10"
            timestamp = file_name.replace('_validation', '')
            
            # Search for files with this timestamp in the name
            # Files are stored as: dq-reports/s3/{source_id}/{timestamp}_validation.json
            print(f"🔍 Searching for validation with timestamp: {timestamp}")
            
            response = s3_client.list_objects_v2(
                Bucket=results_bucket,
                Prefix=results_prefix
            )
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    # Look for files matching the timestamp pattern
                    if f"{timestamp}_validation.json" in key or key.endswith(f"{timestamp}_validation.json"):
                        try:
                            print(f"🔍 Found matching file: {key}")
                            result = s3_client.get_object(Bucket=results_bucket, Key=key)
                            data = json.loads(result['Body'].read().decode('utf-8'))
                            print(f"✅ Loaded validation data from: {key}")
                            return {
                                'file_name': file_name,
                                's3_key': key,
                                'data': data,
                                'matched_exactly': True
                            }
                        except Exception as e:
                            print(f"❌ Error reading {key}: {e}")
                            continue
            
            print(f"❌ No validation file found with timestamp: {timestamp}")
            return None
        
        # Normalize file name (remove extension for search)
        base_name = file_name.replace('.csv', '').replace('.json', '').replace('.parquet', '')
        
        # List objects in S3
        response = s3_client.list_objects_v2(
            Bucket=results_bucket,
            Prefix=results_prefix
        )
        
        if 'Contents' not in response:
            return None
        
        # Search for matching files - check dataset name inside JSON files
        # Files are stored as: dq-reports/{timestamp}_validation/latest.json
        # But the dataset name is inside the JSON file
        exact_match = None
        partial_matches = []
        
        for obj in response['Contents']:
            key = obj['Key']
            if not (key.endswith('latest.json') or key.endswith('.json')):
                continue
                
            try:
                # Read the JSON to check the dataset name
                result = s3_client.get_object(Bucket=results_bucket, Key=key)
                data = json.loads(result['Body'].read().decode('utf-8'))
                dataset_name_in_file = data.get('dataset', '').lower()
                source_in_file = data.get('source', '').lower()
                
                file_name_lower = file_name.lower()
                base_name_lower = base_name.lower()
                
                # Check if file name matches dataset name or source
                dataset_match = (base_name_lower in dataset_name_in_file or 
                               dataset_name_in_file in base_name_lower or
                               file_name_lower.replace('.csv', '').replace('.json', '').replace('.parquet', '') in dataset_name_in_file)
                
                source_match = base_name_lower in source_in_file or file_name_lower in source_in_file
                
                if dataset_match or source_match:
                    if dataset_name_in_file == base_name_lower or dataset_name_in_file == file_name_lower.replace('.csv', '').replace('.json', '').replace('.parquet', ''):
                        exact_match = {'key': key, 'data': data}
                        break
                    else:
                        partial_matches.append({'key': key, 'data': data, 'dataset': dataset_name_in_file})
            except Exception as e:
                print(f"Error reading file {key} for matching: {e}")
                continue
        
        # Use exact match if found, otherwise use first partial match
        target = exact_match if exact_match else (partial_matches[0] if partial_matches else None)
        
        if target:
            return {
                'file_name': file_name,
                's3_key': target['key'],
                'data': target['data'],
                'matched_exactly': exact_match is not None
            }
        
        return None
    except Exception as e:
        print(f"Error searching S3: {e}")
        return None


def answer_question_from_data(query: str, data: Dict[str, Any], dataset_name: str, requested_file_name: Optional[str] = None) -> str:
    """Answer specific questions from file data without AI"""
    query_lower = query.lower()
    results = data.get('results', {})
    summary = data.get('summary', {})
    
    # Use requested file name if it's a validation folder, otherwise use dataset name
    display_name = requested_file_name if requested_file_name and '_validation' in requested_file_name else dataset_name
    
    # Check what the user is asking about
    if 'null' in query_lower and ('count' in query_lower or 'number' in query_lower or 'how many' in query_lower):
        null_count = results.get('null_check', {}).get('total_nulls', 0)
        failed_cols = results.get('null_check', {}).get('failed_columns', [])
        return f"The null count in **{display_name}** is **{null_count}**.\n\n" + \
               (f"Columns with nulls: {', '.join(failed_cols)}" if failed_cols else "✅ No null values found in any columns.")
    
    elif 'null' in query_lower or 'missing' in query_lower:
        null_count = results.get('null_check', {}).get('total_nulls', 0)
        failed_cols = results.get('null_check', {}).get('failed_columns', [])
        status = results.get('null_check', {}).get('status', 'UNKNOWN')
        return f"**Null Check Results for {display_name}:**\n\n" + \
               f"Status: {status}\n" + \
               f"Total Nulls: **{null_count}**\n" + \
               (f"Columns with Nulls: {', '.join(failed_cols)}" if failed_cols else "✅ No columns have null values.")
    
    elif 'duplicate' in query_lower:
        dup_count = results.get('duplicate_check', {}).get('duplicate_count', 0)
        dup_pct = results.get('duplicate_check', {}).get('duplicate_percentage', 0)
        status = results.get('duplicate_check', {}).get('status', 'UNKNOWN')
        return f"**Duplicate Check Results for {display_name}:**\n\n" + \
               f"Status: {status}\n" + \
               f"Duplicate Count: **{dup_count}**\n" + \
               f"Duplicate Percentage: **{dup_pct}%**"
    
    elif 'quality' in query_lower or 'score' in query_lower or 'overall' in query_lower:
        score = summary.get('quality_score', 0)
        passed = summary.get('passed', 0)
        failed = summary.get('failed', 0)
        warnings = summary.get('warnings', 0)
        total = summary.get('total_checks', 0)
        
        explanation = ""
        if warnings > 0:
            explanation = f"\n\n*Note: Quality score is {score}% because {passed} checks passed, {warnings} check(s) had warnings or were skipped (not failures), and {failed} check(s) failed.*"
        elif failed > 0:
            explanation = f"\n\n*Note: {failed} check(s) failed, which affects the quality score.*"
        
        return f"**Data Quality for {display_name}:**\n\n" + \
               f"Quality Score: **{score}%**\n" + \
               f"Checks Passed: {passed}/{total}\n" + \
               f"Checks Failed: {failed}\n" + \
               (f"Warnings/Skipped: {warnings}\n" if warnings > 0 else "") + \
               ("✅ Good quality!" if score >= 75 else "⚠️ Needs attention" if score >= 50 else "❌ Poor quality") + \
               explanation
    
    elif 'row' in query_lower and ('count' in query_lower or 'number' in query_lower or 'how many' in query_lower or 'total' in query_lower):
        row_count = data.get('row_count', 0)
        return f"The total row count in **{display_name}** is **{row_count:,}** rows."
    
    elif ('passed' in query_lower or 'pass' in query_lower) and 'fail' not in query_lower:
        # User is asking specifically about passed checks
        passed = summary.get('passed', 0)
        total = summary.get('total_checks', 0)
        
        passed_checks = []
        check_details = []
        check_types = {
            'null_check': ('Null Check', 'No null values found'),
            'duplicate_check': ('Duplicate Check', 'No duplicates found'),
            'freshness_check': ('Freshness Check', 'Data is up to date'),
            'volume_check': ('Volume Check', 'Row count is normal')
        }
        
        for check_key, (check_name, description) in check_types.items():
            check_result = results.get(check_key, {})
            status = check_result.get('status', 'UNKNOWN')
            if status == 'PASS':
                passed_checks.append(check_name)
                # Add details for each passed check
                if check_key == 'null_check':
                    check_details.append(f"✅ **{check_name}**: All columns are complete with no missing values")
                elif check_key == 'duplicate_check':
                    check_details.append(f"✅ **{check_name}**: No duplicate records found")
                elif check_key == 'freshness_check':
                    age = check_result.get('age_hours', 0)
                    check_details.append(f"✅ **{check_name}**: Data is fresh (age: {age:.1f} hours)")
                elif check_key == 'volume_check':
                    count = check_result.get('current_count', 0)
                    check_details.append(f"✅ **{check_name}**: Row count is {count:,} (within expected range)")
        
        if passed_checks:
            return f"**Passed Checks in {display_name}:**\n\n" + \
                   "\n".join(check_details) + \
                   f"\n\n**Summary:** {passed} out of {total} checks passed successfully."
        else:
            return f"❌ No checks passed in **{display_name}**. All checks either failed or had warnings."
    
    elif ('fail' in query_lower and 'passed' not in query_lower) or ('which check' in query_lower and 'fail' in query_lower and 'passed' not in query_lower):
        failed = summary.get('failed', 0)
        warnings = summary.get('warnings', 0)
        passed = summary.get('passed', 0)
        total = summary.get('total_checks', 0)
        
        failed_checks = []
        warning_checks = []
        skipped_checks = []
        
        # Check each check type
        check_types = {
            'null_check': 'Null Check',
            'duplicate_check': 'Duplicate Check',
            'freshness_check': 'Freshness Check',
            'volume_check': 'Volume Check'
        }
        
        for check_key, check_name in check_types.items():
            check_result = results.get(check_key, {})
            status = check_result.get('status', 'UNKNOWN')
            
            if status == 'FAIL':
                if check_key == 'null_check':
                    failed_checks.append(f"{check_name}: {check_result.get('total_nulls', 0)} null values found")
                elif check_key == 'duplicate_check':
                    failed_checks.append(f"{check_name}: {check_result.get('duplicate_count', 0)} duplicates found")
                elif check_key == 'freshness_check':
                    failed_checks.append(f"{check_name}: Data is stale (age: {check_result.get('age_hours', 0):.1f} hours)")
                elif check_key == 'volume_check':
                    failed_checks.append(f"{check_name}: Unusual row count")
            elif status == 'WARNING':
                warning_checks.append(f"{check_name}: {check_result.get('message', 'Warning detected')}")
            elif status == 'SKIP':
                skipped_checks.append(f"{check_name}: {check_result.get('message', 'Check was skipped')}")
        
        # Build response
        response_parts = []
        
        if failed_checks:
            response_parts.append(f"**Failed Checks ({len(failed_checks)}):**")
            response_parts.extend([f"- {check}" for check in failed_checks])
        
        if warning_checks:
            response_parts.append(f"\n**Warnings ({len(warning_checks)}):**")
            response_parts.extend([f"- {check}" for check in warning_checks])
        
        if skipped_checks:
            response_parts.append(f"\n**Skipped Checks ({len(skipped_checks)}):**")
            response_parts.extend([f"- {check}" for check in skipped_checks])
        
        if failed_checks:
            # Only show failed checks when specifically asked
            return f"**Failed Checks in {display_name}:**\n\n" + \
                   "\n".join([f"❌ {check}" for check in failed_checks]) + \
                   f"\n\n**Summary:** {failed} check(s) failed out of {total} total checks."
        elif warning_checks or skipped_checks:
            # Show warnings/skipped when no failures but user asked about issues
            response = f"**No Failed Checks in {display_name}**\n\n"
            if warning_checks:
                response += f"**Warnings ({len(warning_checks)}):**\n" + \
                           "\n".join([f"⚠️ {check}" for check in warning_checks]) + "\n\n"
            if skipped_checks:
                response += f"**Skipped Checks ({len(skipped_checks)}):**\n" + \
                           "\n".join([f"⏭️ {check}" for check in skipped_checks]) + "\n\n"
            response += f"**Summary:** {passed} passed, {failed} failed, {warnings} warnings/skipped out of {total} total checks."
            return response
        else:
            return f"✅ **No Failed Checks in {display_name}**\n\n" + \
                   f"All {total} checks passed successfully. No issues detected."
    
    else:
        # Default: show summary but focused on what was asked
        score = summary.get('quality_score', 0)
        return f"**Data Quality Summary for {display_name}:**\n\n" + \
               f"Quality Score: **{score}%**\n" + \
               f"Total Rows: {data.get('row_count', 0):,}\n" + \
               f"Checks Passed: {summary.get('passed', 0)}/{summary.get('total_checks', 0)}\n\n" + \
               f"*For specific details, ask about: null count, duplicates, quality score, or row count.*"


def list_available_files() -> List[str]:
    """List all available validation result files in S3"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=settings.aws_default_region or os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        
        results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/')
        
        response = s3_client.list_objects_v2(
            Bucket=results_bucket,
            Prefix=results_prefix
        )
        
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('latest.json') or obj['Key'].endswith('.json'):
                    # Extract file name from path
                    key = obj['Key']
                    file_name = key.split('/')[-1].replace('latest.json', '').replace('.json', '')
                    if file_name and file_name not in files:
                        files.append(file_name)
        
        return files
    except Exception as e:
        print(f"Error listing files: {e}")
        return []


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        init_db()
        print("Database initialized successfully")
    except Exception as e:
        print(f"Database initialization error: {e}")


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint"""
    return {
        "message": "Data Quality Platform API",
        "version": "1.0.0",
        "docs": "/docs"
    }


@app.get("/api/health", response_model=HealthResponse, tags=["Health"])
async def health_check(db: Session = Depends(get_db)):
    """Health check endpoint"""
    services = {
        "api": "healthy",
        "database": "unknown",
        "chatbot": "unknown"
    }
    
    # Check database
    try:
        db.execute("SELECT 1")
        services["database"] = "healthy"
    except Exception:
        services["database"] = "unhealthy"
    
    # Check chatbot
    services["chatbot"] = "healthy" if query_engine else "unavailable"
    
    return HealthResponse(
        status="healthy" if all(v in ["healthy", "unavailable"] for v in services.values()) else "degraded",
        timestamp=datetime.utcnow(),
        services=services
    )


# ==================== Agentic Data Quality Agents Endpoints ====================

@app.get("/api/agents/runs", response_model=ListAgentRunsResponse, tags=["Agents"])
async def list_agent_runs():
    """List all validation runs that have agentic data"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=settings.aws_default_region or os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/s3/')
        
        runs = []
        response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('_validation.json'):
                    try:
                        result = s3_client.get_object(Bucket=results_bucket, Key=obj['Key'])
                        data = json.loads(result['Body'].read().decode('utf-8'))
                        
                        # Check if it has agentic data
                        if 'agentic_issues' in data or 'agentic_summary' in data:
                            # Extract validation ID from key
                            key_parts = obj['Key'].split('/')
                            validation_id = key_parts[-1].replace('_validation.json', '')
                            
                            runs.append({
                                'dataset': data.get('dataset', ''),
                                'source': data.get('source', ''),
                                'validation_id': validation_id,
                                'timestamp': data.get('timestamp', ''),
                                'row_count': data.get('row_count', 0),
                                'total_issues': len(data.get('agentic_issues', []))
                            })
                    except Exception as e:
                        print(f"Error reading {obj['Key']}: {e}")
                        continue
        
        return ListAgentRunsResponse(runs=runs)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing agent runs: {str(e)}")


@app.get("/api/agents/issues", response_model=ListAgentIssuesResponse, tags=["Agents"])
async def list_agent_issues(
    dataset: Optional[str] = None,
    validation_id: Optional[str] = None,
    category: Optional[str] = None,
    issue_type: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """List agentic issues with filters"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=settings.aws_default_region or os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/s3/')
        
        all_issues = []
        
        # Find the validation result file
        if validation_id:
            # Search for specific validation
            response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if validation_id in obj['Key'] and obj['Key'].endswith('_validation.json'):
                        result = s3_client.get_object(Bucket=results_bucket, Key=obj['Key'])
                        data = json.loads(result['Body'].read().decode('utf-8'))
                        all_issues.extend(data.get('agentic_issues', []))
                        break
        elif dataset:
            # Search by dataset name
            response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('latest.json'):
                        result = s3_client.get_object(Bucket=results_bucket, Key=obj['Key'])
                        data = json.loads(result['Body'].read().decode('utf-8'))
                        if data.get('dataset') == dataset:
                            all_issues.extend(data.get('agentic_issues', []))
                            break
        else:
            # Get from latest.json files
            response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('latest.json'):
                        try:
                            result = s3_client.get_object(Bucket=results_bucket, Key=obj['Key'])
                            data = json.loads(result['Body'].read().decode('utf-8'))
                            all_issues.extend(data.get('agentic_issues', []))
                        except Exception:
                            continue
        
        # Apply filters
        filtered_issues = []
        for issue_dict in all_issues:
            issue = AgenticIssue(**issue_dict)
            
            if category and issue.category != category:
                continue
            if issue_type and issue.issue_type != issue_type:
                continue
            
            filtered_issues.append(issue)
        
        # Apply pagination
        total = len(filtered_issues)
        paginated = filtered_issues[offset:offset + limit]
        
        return ListAgentIssuesResponse(
            issues=paginated,
            total=total,
            limit=limit,
            offset=offset
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing agent issues: {str(e)}")


def _check_llm_quota_status() -> Optional[Dict[str, Any]]:
    """Check if LLM API quota is exhausted (works for both OpenAI and Gemini)"""
    try:
        provider = LLMProviderFactory.get_provider()
        
        if provider == LLMProvider.OPENAI:
            # Check OpenAI
            try:
                from openai import OpenAI
                api_key = os.getenv('OPENAI_API_KEY') or os.getenv('OPENAI_KEY')
                if not api_key or api_key == '':
                    return {
                        'exhausted': True,
                        'working_model': None,
                        'message': 'OpenAI API key not configured',
                        'error': True,
                        'provider': 'openai'
                    }
                
                # For OpenAI, just check if key is configured (don't make test calls to avoid rate limits)
                # The actual agents will handle quota errors when they occur
                return {
                    'exhausted': False,
                    'working_model': 'gpt-4o-mini',
                    'message': 'OpenAI is configured and available',
                    'provider': 'openai'
                }
            except Exception as e:
                error_str = str(e)
                if '429' in error_str or 'quota' in error_str.lower() or 'rate_limit' in error_str.lower():
                    return {
                        'exhausted': True,
                        'working_model': None,
                        'message': 'OpenAI API quota/rate limit exceeded. Please check your OpenAI account limits.',
                        'provider': 'openai'
                    }
                else:
                    # For other errors, assume it might work (don't mark as exhausted)
                    return {
                        'exhausted': False,
                        'working_model': 'gpt-4o-mini',
                        'message': 'OpenAI is available (test had error but may work)',
                        'provider': 'openai',
                        'warning': str(e)[:100]
                    }
        
        elif provider == LLMProvider.GEMINI:
            # Check Gemini (original logic)
            import google.genai as genai
            api_key = os.getenv('GOOGLE_API_KEY') or os.getenv('GEMINI_API_KEY')
            if not api_key:
                return None
            
            client = genai.Client(api_key=api_key)
            test_models = ['gemini-3-flash-preview', 'gemini-flash-lite-latest', 'gemini-2.5-flash-lite']
            
            for model in test_models:
                try:
                    response = client.models.generate_content(
                        model=f'models/{model}',
                        contents='test'
                    )
                    if hasattr(response, 'text') and response.text:
                        return {
                            'exhausted': False,
                            'working_model': model,
                            'message': 'AI is available and working',
                            'provider': 'gemini'
                        }
                except Exception as e:
                    error_str = str(e)
                    if '429' in error_str or 'RESOURCE_EXHAUSTED' in error_str:
                        continue
                    elif '503' in error_str:
                        continue
            
            return {
                'exhausted': True,
                'working_model': None,
                'message': 'All Gemini models have quota exhausted. AI fixes cannot be applied until quota resets (24 hours) or billing is enabled.',
                'solution': 'Enable billing at https://console.cloud.google.com/billing for higher limits (250-1,000 requests/day)',
                'provider': 'gemini'
            }
        
        return None
    except Exception as e:
        return {
            'exhausted': True,
            'working_model': None,
            'message': f'Unable to check quota status: {str(e)}',
            'error': True
        }


@app.get("/api/agents/summary", response_model=AgenticSummaryResponse, tags=["Agents"])
async def get_agent_summary(
    dataset: Optional[str] = None,
    validation_id: Optional[str] = None
):
    """Get agentic issues summary (for matrix view)"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=settings.aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=settings.aws_default_region or os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/s3/')
        
        # Find the validation result file
        data = None
        if validation_id:
            response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
            if 'Contents' in response:
                for obj in response['Contents']:
                    if validation_id in obj['Key'] and obj['Key'].endswith('_validation.json'):
                        result = s3_client.get_object(Bucket=results_bucket, Key=obj['Key'])
                        data = json.loads(result['Body'].read().decode('utf-8'))
                        break
        elif dataset:
            print(f"DEBUG: get_agent_summary: Searching for dataset '{dataset}'")
            
            # NEW APPROACH: Search ONLY latest.json files first (these have the newest validation results)
            response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
            if 'Contents' in response:
                print(f"DEBUG: get_agent_summary: Found {len(response['Contents'])} total files in S3")
                
                # Step 1: Check ALL latest.json files first (priority!)
                latest_json_files = [obj for obj in response['Contents'] if obj['Key'].endswith('latest.json')]
                print(f"DEBUG: get_agent_summary: Found {len(latest_json_files)} latest.json files")
                
                for obj in latest_json_files:
                    key = obj['Key']
                    try:
                        result = s3_client.get_object(Bucket=results_bucket, Key=key)
                        temp_data = json.loads(result['Body'].read().decode('utf-8'))
                        file_dataset = temp_data.get('dataset', '')
                        file_source_id = temp_data.get('source_id', '')
                        saved_at = temp_data.get('saved_at', 'unknown')
                        
                        print(f"DEBUG: Checking latest.json: {key}")
                        print(f"       - dataset: '{file_dataset}'")
                        print(f"       - source_id: '{file_source_id}'")
                        print(f"       - saved_at: {saved_at}")
                        print(f"       - agentic_issues count: {len(temp_data.get('agentic_issues', []))}")
                        
                        # Match by dataset name
                        if file_dataset == dataset:
                            data = temp_data
                            print(f"DEBUG: ✅ MATCH FOUND in latest.json: {key}")
                            print(f"DEBUG: ✅ This file has {len(data.get('agentic_issues', []))} agentic issues")
                            break
                        
                        # Also try matching by source_id (sometimes dataset is stored differently)
                        if file_source_id and (file_source_id.endswith(f'/{dataset}') or file_source_id.endswith(f'/{dataset.replace(".csv", "")}')):
                            data = temp_data
                            print(f"DEBUG: ✅ MATCH FOUND by source_id in latest.json: {key}")
                            print(f"DEBUG: ✅ This file has {len(data.get('agentic_issues', []))} agentic issues")
                            break
                    except Exception as e:
                        print(f"DEBUG: Error reading {key}: {e}")
                        continue
                
                # Step 2: If not found in latest.json, check timestamped _validation.json files (fallback)
                if not data:
                    print(f"DEBUG: ❌ No match in latest.json files, checking timestamped _validation.json files")
                    validation_json_files = [obj for obj in response['Contents'] if obj['Key'].endswith('_validation.json')]
                    print(f"DEBUG: Found {len(validation_json_files)} _validation.json files")
                    
                    # Sort by LastModified descending to get newest first
                    validation_json_files = sorted(validation_json_files, key=lambda x: x.get('LastModified', ''), reverse=True)
                    
                    for obj in validation_json_files[:5]:  # Only check 5 most recent
                        key = obj['Key']
                        try:
                            result = s3_client.get_object(Bucket=results_bucket, Key=key)
                            temp_data = json.loads(result['Body'].read().decode('utf-8'))
                            file_dataset = temp_data.get('dataset', '')
                            
                            if file_dataset == dataset:
                                data = temp_data
                                print(f"DEBUG: ✅ MATCH FOUND in timestamped validation.json: {key}")
                                print(f"DEBUG: ✅ This file has {len(data.get('agentic_issues', []))} agentic issues")
                                break
                        except Exception as e:
                            continue
            
            if not data:
                print(f"DEBUG: ❌ No matching data found for dataset '{dataset}'")
                print(f"DEBUG: Available datasets in S3 (from latest.json files):")
                response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
                if 'Contents' in response:
                    for obj in response['Contents'][:20]:  # Show up to 20 files
                        key = obj['Key']
                        if key.endswith('latest.json'):
                            try:
                                result = s3_client.get_object(Bucket=results_bucket, Key=key)
                                temp_data = json.loads(result['Body'].read().decode('utf-8'))
                                print(f"   - {key}")
                                print(f"     dataset: '{temp_data.get('dataset', 'N/A')}'")
                                print(f"     source_id: '{temp_data.get('source_id', 'N/A')}'")
                            except:
                                pass
                
                raise HTTPException(status_code=404, detail=f"Validation result not found for dataset '{dataset}'. Please run a new validation.")
        
        agentic_issues = data.get('agentic_issues', [])
        agentic_summary = data.get('agentic_summary', {})
        
        print(f"DEBUG: get_agent_summary: Found {len(agentic_issues)} agentic issues in S3 data")
        
        # Debug: Show all categories and issue types
        if agentic_issues:
            categories_debug = {}
            for issue_dict in agentic_issues:
                cat = issue_dict.get('category', 'N/A')
                issue_type = issue_dict.get('issue_type', 'N/A')
                key = f"{cat}/{issue_type}"
                categories_debug[key] = categories_debug.get(key, 0) + 1
            print(f"DEBUG: get_agent_summary: All categories/issue_types: {categories_debug}")
        else:
            print(f"DEBUG: get_agent_summary: WARNING - No agentic_issues found in data!")
            print(f"DEBUG: get_agent_summary: Data keys: {list(data.keys())}")
        
        # Build matrix (group by category and issue_type)
        matrix_dict = {}
        processed_count = 0
        error_count = 0
        
        for issue_dict in agentic_issues:
            try:
                issue = AgenticIssue(**issue_dict)
                key = (issue.category, issue.issue_type)
                
                if key not in matrix_dict:
                    matrix_dict[key] = {
                        'category': issue.category,
                        'issue_type': issue.issue_type,
                        'count': 0,
                        'dirty_example': str(issue.dirty_value)[:50] if issue.dirty_value is not None else 'N/A',
                        'smart_fix_example': str(issue.suggested_value)[:50] if issue.suggested_value is not None else 'N/A',
                        'why_agentic': issue.why_agentic or issue.explanation or 'AI-Powered'
                    }
                
                matrix_dict[key]['count'] += 1
                processed_count += 1
            except Exception as e:
                error_count += 1
                print(f"DEBUG: Error processing issue: {e}")
                print(f"DEBUG: Issue dict keys: {list(issue_dict.keys()) if isinstance(issue_dict, dict) else 'Not a dict'}")
                print(f"DEBUG: Issue dict: {str(issue_dict)[:200]}")
                continue
        
        print(f"DEBUG: get_agent_summary: Processed {processed_count} issues, {error_count} errors")
        print(f"DEBUG: get_agent_summary: Matrix dict has {len(matrix_dict)} unique category/issue_type combinations")
        
        matrix = [AgenticIssueSummary(**v) for v in matrix_dict.values()]
        print(f"DEBUG: get_agent_summary: Built matrix with {len(matrix)} entries")
        if matrix:
            for m in matrix:
                print(f"DEBUG:   - {m.category} / {m.issue_type}: {m.count} issues")
        
        # Check quota status
        quota_status = _check_llm_quota_status()
        
        response_data = AgenticSummaryResponse(
            dataset=data.get('dataset', ''),
            validation_id=validation_id,
            total_rows_scanned=agentic_summary.get('total_rows_scanned', data.get('row_count', 0)),
            total_issues=agentic_summary.get('total_issues', len(agentic_issues)),
            rows_affected=agentic_summary.get('rows_affected', 0),
            rows_affected_percent=agentic_summary.get('rows_affected_percent', 0.0),
            summary_by_category=agentic_summary.get('category_counts', {}),
            matrix=matrix,
            quota_status=quota_status
        )
        
        print(f"DEBUG: get_agent_summary: Returning response with {len(matrix)} matrix entries")
        print(f"DEBUG: get_agent_summary: Response total_issues: {response_data.total_issues}")
        print(f"DEBUG: get_agent_summary: Response summary_by_category: {response_data.summary_by_category}")
        
        return response_data
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting agent summary: {str(e)}")


@app.post("/api/agents/apply", response_model=ApplyFixesResponse, tags=["Agents"])
async def apply_fixes(request: ApplyFixesRequest):
    """Apply agentic fixes (preview, export, or commit)"""
    try:
        # DEBUG: Log first few issues to see what's being received
        if request.issues:
            print(f"DEBUG: apply_fixes - Received {len(request.issues)} issues from frontend")
            for i, issue in enumerate(request.issues[:5]):
                issue_dict = issue.model_dump() if hasattr(issue, 'model_dump') else issue
                print(f"DEBUG: apply_fixes - Issue {i}: type={issue_dict.get('issue_type')}, row={issue_dict.get('row_id')}, col={issue_dict.get('column')}, suggested_value='{issue_dict.get('suggested_value')}'")
        
        if not request.issue_ids:
            raise HTTPException(status_code=400, detail="No issue_ids provided")

        # S3 client used to load original CSV (and optionally stored validation JSON)
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=settings.aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=settings.aws_default_region or os.getenv("AWS_DEFAULT_REGION", "ap-south-1"),
        )
        results_bucket = os.getenv("DQ_RESULTS_BUCKET", "project-cb")
        results_prefix = os.getenv("DQ_RESULTS_PREFIX", "dq-reports/s3/")

        dataset = request.dataset
        validation_id = request.validation_id
        data = None
        validation_key = None

        # NEW: If frontend provides issues + source_bucket/key, do not rely on stored results.
        if request.issues and request.source_bucket and request.source_key:
            agentic_issues = [i.model_dump() for i in request.issues]
            selected_issue_ids = set(request.issue_ids)
            selected_issues = [i for i in agentic_issues if i.get("id") in selected_issue_ids]
            if not selected_issues:
                raise HTTPException(status_code=400, detail="None of the requested issue_ids were found in provided issues")

            src_bucket = request.source_bucket
            src_key = request.source_key
        else:
            # Load validation result JSON from S3 (same logic as summary endpoint)
            # Try to locate the validation JSON
            response = s3_client.list_objects_v2(Bucket=results_bucket, Prefix=results_prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    key = obj["Key"]
                    if not key.endswith("_validation.json") and not key.endswith("latest.json"):
                        continue

                    result_obj = s3_client.get_object(Bucket=results_bucket, Key=key)
                    temp_data = json.loads(result_obj["Body"].read().decode("utf-8"))

                    # Match by validation_id if provided
                    if validation_id and validation_id in key and key.endswith("_validation.json"):
                        data = temp_data
                        validation_key = key
                        break

                    # Otherwise match by dataset name
                    if not validation_id and dataset and temp_data.get("dataset") == dataset:
                        data = temp_data
                        validation_key = key
                        # Prefer latest.json but accept any match
                        if key.endswith("latest.json"):
                            break

            if not data:
                raise HTTPException(status_code=404, detail="Validation result not found for apply_fixes")

            agentic_issues = data.get("agentic_issues", [])
            if not agentic_issues:
                raise HTTPException(status_code=400, detail="No agentic issues found to apply")

            # Filter to selected issues
            selected_issue_ids = set(request.issue_ids)
            selected_issues = [i for i in agentic_issues if i.get("id") in selected_issue_ids]

            if not selected_issues:
                raise HTTPException(status_code=400, detail="None of the requested issue_ids were found")

            # Load original CSV from S3 (needed for both preview and export)
            source = data.get("source", "")
            # Expect source like s3://bucket/key
            if not source.startswith("s3://"):
                raise HTTPException(status_code=400, detail=f"Invalid source in validation data: {source}")

            _, _, src_rest = source.partition("s3://")
            src_bucket, _, src_key = src_rest.partition("/")
            if not src_bucket or not src_key:
                raise HTTPException(status_code=400, detail=f"Could not parse source S3 path: {source}")

        # Load original CSV into DataFrame
        import pandas as pd
        from io import BytesIO

        try:
            obj = s3_client.get_object(Bucket=src_bucket, Key=src_key)
            body = obj["Body"].read()
            df = pd.read_csv(BytesIO(body))
            # Keep a copy of original for comparison
            df_original = df.copy()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to load original CSV from S3: {e}")

        # Get unit preferences from request
        unit_preferences = request.unit_preferences or {}
        
        # Track which cells changed (row_id, column) -> (old_value, new_value)
        changed_cells = {}
        
        # First, standardize ALL values in columns with unit issues (not just flagged rows)
        from utils.data_cleaning import parse_units, convert_units
        
        # Find columns with unit issues and determine target unit for each
        columns_to_standardize = {}  # {column_name: target_unit}
        
        for issue in selected_issues:
            if issue.get("issue_type") == "ScaleMismatch":
                col = issue.get("column")
                suggested_value = issue.get("suggested_value")
                
                if col and suggested_value:
                    # Extract target unit from suggested value (e.g., "180.00 cm" -> "cm")
                    # Try to parse the suggested value to get the unit
                    parsed = parse_units(str(suggested_value))
                    if parsed:
                        _, target_unit, _ = parsed
                        if col not in columns_to_standardize:
                            columns_to_standardize[col] = target_unit
                            print(f"DEBUG: apply_fixes - Will standardize ALL values in column '{col}' to '{target_unit}'")
        
        # Also check unit_preferences from request (if user explicitly selected a unit)
        if unit_preferences:
            for col, unit in unit_preferences.items():
                if col not in columns_to_standardize:
                    columns_to_standardize[col] = unit
                    print(f"DEBUG: apply_fixes - Will standardize ALL values in column '{col}' to '{unit}' (from user preference)")
        
        # Standardize ALL values in these columns to the target unit
        for col, target_unit in columns_to_standardize.items():
            if col not in df.columns:
                continue
            
            print(f"DEBUG: apply_fixes - Standardizing ALL rows in column '{col}' to unit '{target_unit}'")
            converted_count = 0
            for idx in range(len(df)):
                value = df.at[idx, col]
                old_value = df_original.at[idx, col]
                
                if value and str(value).strip():
                    value_str = str(value).strip()
                    parsed = parse_units(value_str)
                    
                    if parsed:
                        numeric_value, current_unit, _ = parsed
                        
                        # ALWAYS reformat to target unit (even if already in target unit, for consistency)
                        if current_unit == target_unit:
                            # Already in target unit, just reformat for consistency
                            new_value = f"{numeric_value:.2f} {target_unit}"
                        else:
                            # Convert from current unit to target unit
                            converted = convert_units(numeric_value, current_unit, target_unit)
                            if converted is not None:
                                new_value = f"{converted:.2f} {target_unit}"
                            else:
                                continue  # Skip if conversion failed
                        
                        # Apply the standardized format
                        if str(old_value).strip() != new_value.strip():
                            df.at[idx, col] = new_value
                            changed_cells[(idx, col)] = (str(old_value), new_value)
                            converted_count += 1
                            if converted_count <= 10:  # Only print first 10 to avoid log spam
                                print(f"DEBUG: apply_fixes - Row {idx}, Column '{col}': '{old_value}' → '{new_value}'")
                    
                    elif value_str.replace('.', '').replace('-', '').replace(' ', '').isdigit():
                        # Value is just a number with no unit - assume it's already in target unit
                        try:
                            numeric_value = float(value_str)
                            new_value = f"{numeric_value:.2f} {target_unit}"
                            if str(old_value).strip() != new_value.strip():
                                df.at[idx, col] = new_value
                                changed_cells[(idx, col)] = (str(old_value), new_value)
                                converted_count += 1
                                if converted_count <= 10:
                                    print(f"DEBUG: apply_fixes - Row {idx}, Column '{col}': '{old_value}' (no unit) → '{new_value}'")
                        except ValueError:
                            pass  # Skip if can't convert to float
                    else:
                        # Could not parse this value - log it for debugging
                        if converted_count == 0 or idx % 10 == 0:  # Log occasionally
                            print(f"⚠️ DEBUG: apply_fixes - Row {idx}, Column '{col}': Could not parse value '{value_str}' for unit conversion")
            
            print(f"DEBUG: apply_fixes - ✅ Standardized {converted_count} values in column '{col}' to '{target_unit}'")
            
            # CRITICAL: If we didn't standardize all values, log which ones failed
            if converted_count < len(df):
                missing_count = len(df) - converted_count
                print(f"⚠️ WARNING: {missing_count} values in column '{col}' were not standardized - may need better parsing")
        
        # Apply fixes for other issue types (non-unit issues)
        applied = 0
        applied_details = []
        # CRITICAL: Track which (row, column) pairs have been fixed to avoid duplicates
        fixed_cells = set()
        
        for issue in selected_issues:
            row_id = issue.get("row_id")
            column = issue.get("column")
            suggested_value = issue.get("suggested_value")
            dirty_value = issue.get("dirty_value")
            issue_type = issue.get("issue_type")

            if row_id is None or column is None:
                continue
            if column not in df.columns:
                continue
            if row_id < 0 or row_id >= len(df):
                continue

            # Skip unit issues - already handled above for entire column
            if issue_type == "ScaleMismatch":
                continue
            
            # CRITICAL: Skip if this (row, column) was already fixed (avoid duplicates overwriting better fixes)
            cell_key = (row_id, column)
            if cell_key in fixed_cells:
                print(f"⚠️ SKIPPING duplicate issue for Row {row_id}, Column '{column}', issue_type={issue_type} (already fixed)")
                continue
            
            # Log what we're applying
            print(f"DEBUG: apply_fixes - Applying {issue_type} fix: Row {row_id}, Column '{column}', suggested='{str(suggested_value)[:50]}'...")
            
            # CRITICAL: Never apply fixes to protected columns (names, cities)
            col_lower = column.lower()
            is_name_column = any(kw in col_lower for kw in ['firstname', 'first_name', 'lastname', 'last_name',
                                               'fullname', 'full_name', 'username', 'user_name',
                                               'name', 'person', 'customer', 'employee', 'contact'])
            is_city_column = any(kw in col_lower for kw in ['city', 'town', 'location', 'place'])
            
            if is_name_column:
                print(f"⚠️ SKIPPING {issue_type} fix for personal name column '{column}' at row {row_id}")
                continue  # Skip ALL fixes to name columns
            
            if is_city_column:
                print(f"⚠️ SKIPPING {issue_type} fix for city column '{column}' at row {row_id} (cities are never modified)")
                continue  # NEVER modify city columns

            # Get current value before applying fix
            old_value = df.at[row_id, column]
            
            # Handle None/null suggested_value (set to null/empty for impossible values like temporal paradoxes)
            # Check for Python None, string "None", string "null", or actual null
            print(f"DEBUG: apply_fixes - Row {row_id}, Column {column}: suggested_value={suggested_value}, type={type(suggested_value)}")
            
            if suggested_value is None or str(suggested_value).lower() in ['none', 'null', '']:
                print(f"DEBUG: apply_fixes - Setting {column} at row {row_id} to None (temporal paradox or impossible value)")
                df.at[row_id, column] = None  # Set to null/empty
                applied += 1
                applied_details.append({
                    "row_id": row_id,
                    "column": column,
                    "old_value": str(old_value),
                    "new_value": "null (impossible value)"
                })
                changed_cells[(row_id, column)] = (str(old_value), "null")
                fixed_cells.add(cell_key)  # Mark as fixed
            elif suggested_value:  # Only apply if suggested_value is not empty
                # Apply the fix
                df.at[row_id, column] = suggested_value
                applied += 1
                applied_details.append({
                    "row_id": row_id,
                    "column": column,
                    "old_value": str(old_value),
                    "new_value": str(suggested_value)
                })
                # Track this change
                changed_cells[(row_id, column)] = (str(old_value), str(suggested_value))
                fixed_cells.add(cell_key)  # Mark as fixed
        
        # Count unit standardizations
        unit_standardizations = 0
        for col in columns_to_standardize:
            if col in df.columns:
                # Count how many values were in this column (approximate)
                unit_standardizations += len(df[df[col].notna()])
        
        applied += unit_standardizations

        # Convert DataFrame to CSV string
        from io import StringIO
        csv_buf = StringIO()
        df.to_csv(csv_buf, index=False)
        csv_content = csv_buf.getvalue()

        # For preview mode, return CSV content as base64 for download
        if request.mode == "preview":
            import base64
            # json is already imported at top of file
            csv_base64 = base64.b64encode(csv_content.encode("utf-8")).decode("utf-8")
            
            # Also return original CSV for comparison
            csv_buf_original = StringIO()
            df_original.to_csv(csv_buf_original, index=False)
            csv_original_base64 = base64.b64encode(csv_buf_original.getvalue().encode("utf-8")).decode("utf-8")
            
            filename = src_key.split('/')[-1].replace('.csv', '') + '_cleaned.csv'
            
            # Convert changed_cells dict to serializable format
            changed_cells_serializable = {}
            for (row, col), (old, new) in changed_cells.items():
                key = f"{row}_{col}"
                changed_cells_serializable[key] = {
                    "old": str(old) if old is not None else "",
                    "new": str(new) if new is not None else ""
                }
            
            return ApplyFixesResponse(
                status="success",
                message=f"Preview: {applied} fixes applied. Ready to download.",
                preview_data={
                    "csv_base64": csv_base64,
                    "csv_original_base64": csv_original_base64,
                    "filename": filename,
                    "applied_count": applied,
                    "applied_details": applied_details,
                    "changed_cells": changed_cells_serializable
                },
                applied_count=applied,
                download_url=None,
            )

        # For export mode, save to S3 and return URL
        base_key = src_key
        if base_key.lower().endswith(".csv"):
            base_no_ext = base_key[:-4]
        else:
            base_no_ext = base_key

        cleaned_key = f"{base_no_ext}_cleaned.csv"

        # Write cleaned CSV back to S3
        csv_bytes = csv_content.encode("utf-8")

        s3_client.put_object(
            Bucket=src_bucket,
            Key=cleaned_key,
            Body=csv_bytes,
            ContentType="text/csv",
        )

        cleaned_s3_path = f"s3://{src_bucket}/{cleaned_key}"

        return ApplyFixesResponse(
            status="success",
            message=f"Applied {applied} fixes and saved cleaned CSV to S3",
            preview_data=None,
            applied_count=applied,
            download_url=cleaned_s3_path,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error applying fixes: {str(e)}")


# ==================== S3 File Browser Endpoint ====================

@app.get("/api/s3/list-files", tags=["S3"])
async def list_s3_files(bucket: str, prefix: str = ""):
    """List files in S3 bucket for file browser dropdown"""
    try:
        import boto3
        from botocore.exceptions import ClientError, NoCredentialsError
        
        # Trim whitespace from bucket and prefix to prevent validation errors
        bucket = bucket.strip() if bucket else ""
        prefix = prefix.strip() if prefix else ""
        
        # Validate bucket name
        if not bucket:
            raise HTTPException(
                status_code=400,
                detail="Bucket name cannot be empty"
            )
        
        # Get AWS credentials from environment (check both AWS_REGION and AWS_DEFAULT_REGION)
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION') or 'us-east-1'
        
        print(f"DEBUG: S3 credentials check - Access Key: {'Set' if aws_access_key else 'Missing'}, Secret Key: {'Set' if aws_secret_key else 'Missing'}, Region: {aws_region}")
        print(f"DEBUG: S3 request - Bucket: '{bucket}' (length: {len(bucket)}), Prefix: '{prefix}'")
        
        # Check if credentials are available
        if not aws_access_key or not aws_secret_key:
            raise HTTPException(
                status_code=400, 
                detail="AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in environment variables or .env file"
            )
        
        # Create S3 client
        try:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create S3 client: {str(e)}"
            )
        
        # List files
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1000
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip directories (keys ending with /)
                    if not obj['Key'].endswith('/'):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat()
                        })
            
            return {
                "bucket": bucket,
                "prefix": prefix,
                "files": files,
                "count": len(files)
            }
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', str(e))
            print(f"DEBUG: S3 ClientError - Code: {error_code}, Message: {error_message}")
            
            # Handle specific error codes more gracefully
            if error_code == 'NoSuchBucket':
                raise HTTPException(
                    status_code=404,
                    detail=f"Bucket '{bucket}' does not exist. Please check the bucket name."
                )
            elif error_code == 'AccessDenied':
                raise HTTPException(
                    status_code=403,
                    detail=f"Access denied to bucket '{bucket}'. Please check your AWS credentials and permissions."
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail=f"AWS S3 error ({error_code}): {error_message}"
                )
        except NoCredentialsError:
            raise HTTPException(
                status_code=401,
                detail="AWS credentials not found. Please configure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY in your .env file"
            )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"ERROR: Exception in list_s3_files: {error_details}")
        print(f"ERROR: Exception type: {type(e).__name__}, Message: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error listing S3 files: {str(e)}. Check backend logs for details."
        )


# ==================== Validation Trigger Endpoint ====================

@app.post("/api/validate", tags=["Validation"])
async def trigger_validation(config: dict):
    """
    Trigger validation for a configured dataset
    
    Expects config with:
    - name: Dataset name
    - source_type: 's3', 'databricks', etc.
    - connection_details: Source-specific connection info
    - primary_key: Optional primary key column
    - required_columns: Optional list of required columns
    """
    try:
        from services.validation_service import run_validation
        
        print(f"DEBUG: /api/validate called with config: {config.get('connection_details', {}).get('key', 'N/A')}")
        
        # Run validation
        results = run_validation(config)
        
        print(f"DEBUG: /api/validate: Validation completed")
        print(f"DEBUG: /api/validate: Agentic issues count: {len(results.get('agentic_issues', []))}")
        if results.get('agentic_issues'):
            categories = {}
            for issue in results['agentic_issues']:
                cat = issue.get('category', 'N/A')
                categories[cat] = categories.get(cat, 0) + 1
            print(f"DEBUG: /api/validate: Issue categories in response: {categories}")
        
        return {
            "status": "success",
            "message": "Validation completed successfully",
            "results": results,
            "source_id": f"{config['connection_details']['bucket']}/{config['connection_details']['key'].replace('.csv', '').replace('.parquet', '')}"
        }
    except Exception as e:
        # Get error message without using f-strings to avoid any scoping issues
        error_str = str(e)
        print("ERROR: /api/validate failed: " + error_str)
        import traceback
        traceback.print_exc()
        # Use string concatenation instead of f-string to avoid any os scoping issues
        raise HTTPException(status_code=500, detail="Validation failed: " + error_str)


# ==================== Dataset Configuration Endpoints ====================

@app.post("/api/config", response_model=DatasetConfigResponse, tags=["Configuration"])
async def create_dataset_config(
    config: DatasetConfigCreate,
    db: Session = Depends(get_db)
):
    """Create a new dataset configuration"""
    
    # Check if name already exists
    existing = db.query(DatasetConfig).filter(DatasetConfig.name == config.name).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Dataset with name '{config.name}' already exists"
        )
    
    # Create new config
    db_config = DatasetConfig(
        id=uuid.uuid4(),
        name=config.name,
        source_type=config.source_type,
        connection_details=config.connection_details,
        schema_definition=config.schema_definition,
        primary_key=config.primary_key,
        required_columns=config.required_columns,
        quality_checks=config.quality_checks,
        is_active=True
    )
    
    db.add(db_config)
    db.commit()
    db.refresh(db_config)
    
    return db_config


@app.get("/api/config/{dataset_id}", response_model=DatasetConfigResponse, tags=["Configuration"])
async def get_dataset_config(dataset_id: str, db: Session = Depends(get_db)):
    """Get dataset configuration by ID"""
    
    config = db.query(DatasetConfig).filter(DatasetConfig.id == dataset_id).first()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found"
        )
    
    return config


@app.get("/api/datasets", response_model=List[DatasetConfigResponse], tags=["Configuration"])
async def list_datasets(
    skip: int = 0,
    limit: int = 100,
    active_only: bool = True,
    db: Session = Depends(get_db)
):
    """List all dataset configurations"""
    
    query = db.query(DatasetConfig)
    if active_only:
        query = query.filter(DatasetConfig.is_active == True)
    
    datasets = query.offset(skip).limit(limit).all()
    return datasets


@app.delete("/api/config/{dataset_id}", tags=["Configuration"])
async def delete_dataset_config(dataset_id: str, db: Session = Depends(get_db)):
    """Delete (deactivate) dataset configuration"""
    
    config = db.query(DatasetConfig).filter(DatasetConfig.id == dataset_id).first()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found"
        )
    
    # Soft delete
    config.is_active = False
    db.commit()
    
    return {"message": f"Dataset {dataset_id} deactivated successfully"}


# ==================== DAG Trigger Endpoints ====================

@app.post("/api/trigger/{dataset_id}", response_model=DAGTriggerResponse, tags=["Execution"])
async def trigger_dag(
    dataset_id: str,
    db: Session = Depends(get_db)
):
    """Trigger Airflow DAG for dataset validation"""
    
    # Get dataset config
    config = db.query(DatasetConfig).filter(DatasetConfig.id == dataset_id).first()
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Dataset {dataset_id} not found"
        )
    
    # Generate run ID
    run_id = f"manual__{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
    dag_id = f"dq_validation_{config.name}"
    
    # In a real implementation, this would call Airflow API
    # For now, we'll create a DAG run record
    dag_run = DAGRun(
        dataset_id=dataset_id,
        run_id=run_id,
        dag_id=dag_id,
        status="running",
        started_at=datetime.utcnow()
    )
    
    db.add(dag_run)
    db.commit()
    
    return DAGTriggerResponse(
        run_id=run_id,
        dag_id=dag_id,
        status="triggered",
        message=f"DAG {dag_id} triggered successfully with run_id {run_id}"
    )


# ==================== Metrics Endpoints ====================

@app.get("/api/metrics/{dataset_id}", response_model=List[QualityMetricResponse], tags=["Metrics"])
async def get_metrics(
    dataset_id: str,
    run_id: Optional[str] = None,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """Get quality metrics for a dataset"""
    
    query = db.query(QualityMetric).filter(QualityMetric.dataset_id == dataset_id)
    
    if run_id:
        query = query.filter(QualityMetric.run_id == run_id)
    
    metrics = query.order_by(QualityMetric.timestamp.desc()).limit(limit).all()
    return metrics


@app.get("/api/metrics/{dataset_id}/latest", tags=["Metrics"])
async def get_latest_metrics(dataset_id: str, db: Session = Depends(get_db)):
    """Get latest metrics for a dataset"""
    
    # Get latest run_id
    latest_metric = db.query(QualityMetric).filter(
        QualityMetric.dataset_id == dataset_id
    ).order_by(QualityMetric.timestamp.desc()).first()
    
    if not latest_metric:
        return {"metrics": [], "run_id": None}
    
    # Get all metrics for that run
    metrics = db.query(QualityMetric).filter(
        QualityMetric.dataset_id == dataset_id,
        QualityMetric.run_id == latest_metric.run_id
    ).all()
    
    return {
        "run_id": latest_metric.run_id,
        "timestamp": latest_metric.timestamp,
        "metrics": [
            {
                "check_type": m.check_type,
                "status": m.status,
                "value": float(m.value) if m.value else None,
                "details": m.details
            }
            for m in metrics
        ]
    }


@app.get("/api/anomalies/{dataset_id}", response_model=List[AnomalyRecordResponse], tags=["Anomalies"])
async def get_anomalies(
    dataset_id: str,
    limit: int = 50,
    db: Session = Depends(get_db)
):
    """Get anomaly records for a dataset"""
    
    anomalies = db.query(AnomalyRecord).filter(
        AnomalyRecord.dataset_id == dataset_id
    ).order_by(AnomalyRecord.detected_at.desc()).limit(limit).all()
    
    return anomalies


# ==================== Chatbot Endpoints ====================

@app.get("/api/chat/test-file/{file_name}", tags=["Chatbot"])
async def test_file_fetch(file_name: str):
    """Test endpoint to verify file fetching works"""
    try:
        file_data = search_file_in_s3(file_name)
        if file_data:
            return {
                "status": "success",
                "file_name": file_data['file_name'],
                "s3_key": file_data['s3_key'],
                "data_keys": list(file_data['data'].keys()),
                "summary": file_data['data'].get('summary', {}),
                "row_count": file_data['data'].get('row_count', 0)
            }
        else:
            available_files = list_available_files()
            return {
                "status": "file_not_found",
                "file_name": file_name,
                "available_files": available_files
            }
    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }


@app.post("/api/chat", response_model=ChatResponse, tags=["Chatbot"])
async def chat(request: ChatRequest, db: Session = Depends(get_db)):
    """
    Process chatbot query - supports file-based queries
    
    Examples:
    - "What is the data quality in customers.csv?"
    - "Tell me about null values in orders.json"
    - "What issues are in my_data.parquet?"
    """
    
    if not query_engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chatbot service is not available. Please configure OPENAI_API_KEY."
        )
    
    # Extract file name from query if not provided
    file_name = request.file_name
    if not file_name:
        file_name = extract_file_name_from_query(request.query)
        
        # Handle follow-up questions like "what are the total rows in the file?"
        # Check if query mentions "this file", "the file", "it" but no file name was extracted
        query_lower = request.query.lower()
        if not file_name and any(phrase in query_lower for phrase in ['this file', 'the file', 'in the file', 'from the file']):
            # Try to find any file-like pattern in the query
            # Look for validation folder patterns or file extensions
            validation_match = re.search(r'\b(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}_validation)\b', request.query, re.IGNORECASE)
            if validation_match:
                file_name = validation_match.group(1)
            else:
                # Look for any file with extension
                file_match = re.search(r'\b([\w\-]+\.(?:csv|json|parquet))\b', request.query, re.IGNORECASE)
                if file_match:
                    file_name = file_match.group(1)
    
    # STEP 1: Extract and fetch file data first (before AI processing)
    file_data = None
    if file_name:
        print(f"🔍 Searching for file: {file_name}")
        file_data = search_file_in_s3(file_name)
        if not file_data:
            # File not found - return helpful message
            available_files = list_available_files()
            files_list = ", ".join(available_files[:10]) if available_files else "none"
            return ChatResponse(
                query=request.query,
                response=f"I couldn't find a file named '{file_name}' in the validation results.\n\n"
                        f"Available files: {files_list if files_list != 'none' else 'No files found. Please run validation first.'}\n\n"
                        f"Please check the file name and try again, or run validation to generate results.",
                metadata={"intent": "file_not_found", "file_name": file_name, "available_files": available_files},
                timestamp=datetime.utcnow()
            )
        else:
            matched_exactly = file_data.get('matched_exactly', True)
            actual_dataset = file_data['data'].get('dataset', '')
            requested_base = file_name.lower().replace('.csv', '').replace('.json', '').replace('.parquet', '')
            actual_base = actual_dataset.lower().replace('.csv', '').replace('.json', '').replace('.parquet', '')
            
            # Only warn if it's clearly a different file
            if not matched_exactly and requested_base != actual_base and requested_base not in actual_base and actual_base not in requested_base:
                # Warn user that a different file was matched
                return ChatResponse(
                    query=request.query,
                    response=f"⚠️ I couldn't find an exact match for '{file_name}'. I found data for '{actual_dataset}' instead.\n\n"
                            f"Please verify the file name. Available files: {', '.join(list_available_files()[:10])}",
                    metadata={"intent": "file_mismatch", "requested_file": file_name, "matched_file": actual_dataset},
                    timestamp=datetime.utcnow()
                )
            print(f"✅ File found: {file_data['file_name']} -> {actual_dataset} at {file_data['s3_key']} (exact match: {matched_exactly})")
    
    # STEP 2: Prepare metadata from file data
    metadata = {}
    dataset_name = None
    
    # If file data found, use it as context
    if file_data:
        data = file_data['data']
        print(f"📊 File data loaded: {len(str(data))} bytes, keys: {list(data.keys())}")
        
        metadata = {
            "file_name": file_data['file_name'],
            "s3_key": file_data['s3_key'],
            "dataset": data.get('dataset', file_data['file_name']),
            "source": data.get('source', ''),
            "row_count": data.get('row_count', 0),
            "timestamp": data.get('timestamp', ''),
            "summary": data.get('summary', {}),
            "results": data.get('results', {}),
            "agentic_issues": data.get('agentic_issues', []),
            "agentic_summary": data.get('agentic_summary', {})
        }
        dataset_name = data.get('dataset', file_data['file_name'])
        print(f"✅ Metadata prepared for dataset: {dataset_name}")
    # Otherwise, try to get from database if dataset_id provided
    elif request.dataset_id:
        latest_metrics = db.query(QualityMetric).filter(
            QualityMetric.dataset_id == request.dataset_id
        ).order_by(QualityMetric.timestamp.desc()).limit(10).all()
        
        metadata = {
            "metrics": [
                {
                    "check_type": m.check_type,
                    "status": m.status,
                    "value": float(m.value) if m.value else None,
                    "details": m.details,
                    "timestamp": m.timestamp.isoformat()
                }
                for m in latest_metrics
            ]
        }
        
        # Get dataset info
        config = db.query(DatasetConfig).filter(DatasetConfig.id == request.dataset_id).first()
        if config:
            metadata["dataset_name"] = config.name
            dataset_name = config.name
        else:
            dataset_name = None
    else:
        # No file or dataset_id - try to list available files
        available_files = list_available_files()
        query_lower = request.query.lower()
        
        # Check if user is asking about "the file" or "this file" (follow-up question)
        is_followup = any(phrase in query_lower for phrase in ['the file', 'this file', 'in the file', 'from the file', 'it'])
        
        if available_files:
            files_list = ", ".join(available_files[:10])
            if is_followup:
                response_msg = f"I need to know which file you're referring to. Please include the file name or validation folder name in your question.\n\n"
            else:
                response_msg = f"I need to know which file you're asking about. Please specify a file name in your question.\n\n"
            
            response_msg += f"Available files: {files_list}\n\n"
            response_msg += f"Example: 'What is the data quality in {available_files[0] if available_files else 'filename.csv'}?'\n"
            response_msg += f"Or: '{available_files[0] if available_files else '2026-01-13_19-58-10_validation'} what are the total rows?'"
            
            return ChatResponse(
                query=request.query,
                response=response_msg,
                metadata={"intent": "file_required", "available_files": available_files},
                timestamp=datetime.utcnow()
            )
        else:
            return ChatResponse(
                query=request.query,
                response="No validation results found. Please run validation first to generate data quality reports.",
                metadata={"intent": "no_data"},
                timestamp=datetime.utcnow()
            )
    
    # STEP 3: Process with AI (only if query_engine is available)
    if not query_engine:
        # If AI not available, return file data in readable format
        if file_data:
            data = file_data['data']
            summary = data.get('summary', {})
            results = data.get('results', {})
            
            # Use requested file name if it's a validation folder, otherwise use dataset name
            display_name = file_name if file_name and '_validation' in file_name else (dataset_name or file_name)
            
            response_text = f"""**Data Quality Report for {display_name}**

**Summary:**
- Quality Score: {summary.get('quality_score', 0)}%
- Total Checks: {summary.get('total_checks', 0)}
- Passed: {summary.get('passed', 0)}
- Failed: {summary.get('failed', 0)}
- Total Rows: {data.get('row_count', 0):,}

**Detailed Results:**

**Null Check:** {results.get('null_check', {}).get('status', 'N/A')}
- Total Nulls: {results.get('null_check', {}).get('total_nulls', 0)}
- Failed Columns: {', '.join(results.get('null_check', {}).get('failed_columns', [])) or 'None'}

**Duplicate Check:** {results.get('duplicate_check', {}).get('status', 'N/A')}
- Duplicate Count: {results.get('duplicate_check', {}).get('duplicate_count', 0)}
- Duplicate Percentage: {results.get('duplicate_check', {}).get('duplicate_percentage', 0)}%

**Freshness Check:** {results.get('freshness_check', {}).get('status', 'N/A')}
- Latest Timestamp: {results.get('freshness_check', {}).get('latest_timestamp', 'N/A')}
- Age (hours): {results.get('freshness_check', {}).get('age_hours', 0):.2f}

**Volume Check:** {results.get('volume_check', {}).get('status', 'N/A')}
- Current Count: {results.get('volume_check', {}).get('current_count', 0):,} rows

*Note: AI features require OPENAI_API_KEY to be configured.*
"""
            return ChatResponse(
                query=request.query,
                response=response_text,
                metadata={
                    "intent": "file_data_returned",
                    "file_name": file_name,
                    "dataset_name": dataset_name
                },
                timestamp=datetime.utcnow()
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Chatbot service is not available. Please configure OPENAI_API_KEY."
            )
    
    # AI is available - process with AI
    try:
        print(f"🤖 Processing query with AI: {request.query[:50]}...")
        result = query_engine.process_query(
            query=request.query,
            metadata=metadata,
            dataset_name=dataset_name or metadata.get('dataset') or metadata.get('file_name')
        )
        print(f"✅ AI response generated")
        
        # Check if AI returned an error response (quota exceeded, etc.)
        response_text = result.get('response', '')
        is_error_response = (
            'encountered an error' in response_text.lower() or 
            'error code' in response_text.lower() or 
            'quota' in response_text.lower() or 
            'insufficient' in response_text.lower() or
            'does not exist' in response_text.lower() or
            'model' in response_text.lower() and 'error' in response_text.lower()
        )
        
        if is_error_response:
            print(f"⚠️ AI returned error response (likely quota/access issue), using intelligent fallback")
            # Use intelligent fallback that answers the specific question
            if file_data:
                fallback_response = answer_question_from_data(
                    request.query, 
                    file_data['data'], 
                    dataset_name or file_name,
                    requested_file_name=file_name
                )
                # Add note that AI is unavailable
                fallback_response += "\n\n*Note: AI-powered responses are currently unavailable. Showing data-driven analysis.*"
                return ChatResponse(
                    query=request.query,
                    response=fallback_response,
                    metadata={
                        "intent": "fallback_response",
                        "file_name": file_name,
                        "dataset_name": dataset_name,
                        "ai_unavailable": True
                    },
                    timestamp=datetime.utcnow()
                )
            raise Exception("AI service error - using fallback")
        
        # IMPORTANT: DO NOT save file-based queries to database (they don't have dataset_id)
        # Only save if dataset_id is explicitly provided (for database-based queries)
        # File-based queries should never be saved to avoid database constraint errors
        
        return ChatResponse(
            query=request.query,
            response=result['response'],
            metadata={
                "intent": result.get('intent'),
                "file_name": file_name,
                "dataset_name": dataset_name or metadata.get('dataset')
            },
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        # If AI fails, fall back to answering the specific question from file data
        error_message = str(e)
        print(f"⚠️ AI processing failed: {error_message}")
        
        if file_data:
            # Answer the specific question instead of showing everything
            # Pass the requested file_name so responses use validation folder name when appropriate
            fallback_response = answer_question_from_data(
                request.query, 
                file_data['data'], 
                dataset_name or file_name,
                requested_file_name=file_name  # Pass original requested name
            )
            return ChatResponse(
                query=request.query,
                response=fallback_response,
                metadata={
                    "intent": "fallback_response",
                    "file_name": file_name,
                    "error": error_message[:200]
                },
                timestamp=datetime.utcnow()
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error processing query: {error_message}"
            )


@app.get("/api/chat/files", tags=["Chatbot"])
async def list_available_chat_files():
    """List all available validation result files"""
    files = list_available_files()
    return {
        "files": files,
        "count": len(files)
    }


@app.get("/api/chat/history/{dataset_id}", tags=["Chatbot"])
async def get_chat_history(
    dataset_id: str,
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """Get chat history for a dataset"""
    
    history = db.query(ChatHistory).filter(
        ChatHistory.dataset_id == dataset_id
    ).order_by(ChatHistory.timestamp.desc()).limit(limit).all()
    
    return [
        {
            "query": h.query,
            "response": h.response,
            "timestamp": h.timestamp.isoformat()
        }
        for h in reversed(history)  # Reverse to show chronological order
    ]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.backend_host,
        port=settings.backend_port,
        reload=True
    )
