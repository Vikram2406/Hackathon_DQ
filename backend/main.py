"""
FastAPI Backend - Main Application
"""
import sys
import os
# Add parent directory to path to find chatbot module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
import uuid

from database import get_db, init_db
from models import (
    DatasetConfig, QualityMetric, AnomalyRecord, ChatHistory, DAGRun,
    DatasetConfigCreate, DatasetConfigResponse, QualityMetricResponse,
    AnomalyRecordResponse, ChatRequest, ChatResponse,
    DAGTriggerRequest, DAGTriggerResponse, HealthResponse
)
from config import settings
from chatbot import QueryEngine



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

# Initialize chatbot
try:
    query_engine = QueryEngine(api_key=settings.openai_api_key)
except Exception as e:
    print(f"Warning: Could not initialize chatbot: {e}")
    query_engine = None


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


# ==================== S3 File Browser Endpoint ====================

@app.get("/api/s3/list-files", tags=["S3"])
async def list_s3_files(bucket: str, prefix: str = ""):
    """List files in S3 bucket for file browser dropdown"""
    try:
        from connectors.s3_connector import S3Connector
        
        # Create connector with minimal config
        connector = S3Connector({
            'bucket': bucket,
            'key': 'dummy.csv',  # Required by constructor but not used for listing
            'file_format': 'csv'
        })
        
        # List files
        files = connector.list_files(bucket=bucket, prefix=prefix)
        
        return {
            "bucket": bucket,
            "prefix": prefix,
            "files": files,
            "count": len(files)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list S3 files: {str(e)}")


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
        
        # Run validation
        results = run_validation(config)
        
        return {
            "status": "success",
            "message": "Validation completed successfully",
            "results": results,
            "source_id": f"{config['connection_details']['bucket']}/{config['connection_details']['key'].replace('.csv', '').replace('.parquet', '')}"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")


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

@app.post("/api/chat", response_model=ChatResponse, tags=["Chatbot"])
async def chat(request: ChatRequest, db: Session = Depends(get_db)):
    """Process chatbot query"""
    
    if not query_engine:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Chatbot service is not available"
        )
    
    # Get latest metrics for context
    metadata = {}
    if request.dataset_id:
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
    
    # Process query
    try:
        result = query_engine.process_query(
            query=request.query,
            metadata=metadata,
            dataset_name=metadata.get("dataset_name")
        )
        
        # Store in history
        chat_record = ChatHistory(
            dataset_id=request.dataset_id,
            query=request.query,
            response=result['response'],
            metadata={"intent": result.get('intent')}
        )
        db.add(chat_record)
        db.commit()
        
        return ChatResponse(
            query=request.query,
            response=result['response'],
            metadata={"intent": result.get('intent')},
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error processing query: {str(e)}"
        )


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
