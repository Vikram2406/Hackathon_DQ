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
