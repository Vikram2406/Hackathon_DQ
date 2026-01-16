"""
Pydantic schemas for request/response validation
"""
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Optional
from datetime import datetime
from uuid import UUID


class DatasetConfigCreate(BaseModel):
    """Schema for creating a new dataset configuration"""
    name: str = Field(..., min_length=1, max_length=255)
    source_type: str = Field(..., pattern="^(s3|snowflake|csv)$")
    connection_details: Dict[str, Any]
    schema_definition: Dict[str, Any]
    primary_key: Optional[str] = None
    required_columns: List[str] = []
    quality_checks: Dict[str, Any]


class DatasetConfigResponse(BaseModel):
    """Schema for dataset configuration response"""
    model_config = ConfigDict(from_attributes=True)
    
    id: UUID
    name: str
    source_type: str
    connection_details: Dict[str, Any]
    schema_definition: Dict[str, Any]
    primary_key: Optional[str]
    required_columns: List[str]
    quality_checks: Dict[str, Any]
    is_active: bool
    created_at: datetime
    updated_at: datetime


class QualityMetricResponse(BaseModel):
    """Schema for quality metric response"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    dataset_id: UUID
    run_id: str
    check_type: str
    status: str
    value: Optional[float]
    threshold: Optional[float]
    details: Optional[Dict[str, Any]]
    timestamp: datetime


class AnomalyRecordResponse(BaseModel):
    """Schema for anomaly record response"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    dataset_id: UUID
    metric_id: Optional[int]
    run_id: str
    severity: str
    ai_explanation: Optional[str]
    root_cause: Optional[str]
    recommended_actions: Optional[List[str]]
    risk_level: Optional[str]
    detected_at: datetime


class ChatRequest(BaseModel):
    """Schema for chatbot query request"""
    dataset_id: Optional[UUID] = None
    query: str = Field(..., min_length=1, max_length=1000)
    file_name: Optional[str] = None  # Optional file name to search for


class ChatResponse(BaseModel):
    """Schema for chatbot response"""
    query: str
    response: str
    metadata: Optional[Dict[str, Any]] = None
    timestamp: datetime


class DAGTriggerRequest(BaseModel):
    """Schema for triggering Airflow DAG"""
    dataset_id: UUID
    config: Optional[Dict[str, Any]] = None


class DAGTriggerResponse(BaseModel):
    """Schema for DAG trigger response"""
    run_id: str
    dag_id: str
    status: str
    message: str


class HealthResponse(BaseModel):
    """Schema for health check response"""
    status: str
    timestamp: datetime
    services: Dict[str, str]


# ==================== Agentic Data Quality Agents Schemas ====================

class AgenticIssue(BaseModel):
    """Schema for a single agentic data quality issue"""
    id: Optional[str] = None  # Unique identifier for the issue
    row_id: Optional[int] = None  # Row index (0-based) or None for dataset-level
    column: str  # Column name where issue was found
    category: str  # Semantic, Logic, Formatting, Imputation, Extraction, Categorical, Units
    issue_type: str  # DateChaos, PhoneNormalization, EntityResolution, etc.
    dirty_value: Any  # Original problematic value
    suggested_value: Any  # Proposed fix
    confidence: float = Field(..., ge=0.0, le=1.0)  # Confidence score 0-1
    explanation: str  # Why it's "agentic" - reasoning behind the fix
    why_agentic: Optional[str] = None  # Additional explanation of agentic reasoning


class AgenticIssueSummary(BaseModel):
    """Schema for summary of agentic issues by category/type"""
    category: str
    issue_type: str
    count: int
    dirty_example: Optional[str] = None  # Example dirty value
    smart_fix_example: Optional[str] = None  # Example suggested fix
    why_agentic: Optional[str] = None  # Why this fix is agentic


class ListAgentRunsResponse(BaseModel):
    """Schema for listing validation runs with agentic data"""
    runs: List[Dict[str, Any]]  # List of runs with dataset, validation_id, timestamp


class ListAgentIssuesResponse(BaseModel):
    """Schema for listing agentic issues"""
    issues: List[AgenticIssue]
    total: int
    limit: int
    offset: int


class AgenticSummaryResponse(BaseModel):
    """Schema for agentic issues summary (matrix view)"""
    dataset: str
    validation_id: Optional[str] = None
    total_rows_scanned: int
    total_issues: int
    rows_affected: int
    rows_affected_percent: float
    summary_by_category: Dict[str, int]  # Count per category
    matrix: List[AgenticIssueSummary]  # Matrix rows for UI
    quota_status: Optional[Dict[str, Any]] = None  # Quota exhaustion status


class ApplyFixesRequest(BaseModel):
    """Schema for applying agentic fixes"""
    issue_ids: List[str]  # IDs of issues to apply
    mode: str = Field(default="preview", pattern="^(preview|export|commit)$")  # preview, export, or commit
    dataset: Optional[str] = None
    validation_id: Optional[str] = None
    unit_preferences: Optional[Dict[str, str]] = None  # Column name -> preferred unit (e.g., {"height": "cm", "weight": "kg"})
    # Optional: allow apply_fixes without stored validation JSON.
    # If provided, backend will use these directly instead of fetching validation results history.
    issues: Optional[List[AgenticIssue]] = None
    source_bucket: Optional[str] = None
    source_key: Optional[str] = None


class ApplyFixesResponse(BaseModel):
    """Schema for apply fixes response"""
    status: str
    message: str
    preview_data: Optional[Dict[str, Any]] = None  # For preview mode
    download_url: Optional[str] = None  # For export mode
    applied_count: Optional[int] = None  # For commit mode
