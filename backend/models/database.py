"""
Database models for Data Quality Platform - SQLite Compatible
"""
from sqlalchemy import Column, String, Integer, BigInteger, Numeric, DateTime, ForeignKey, ARRAY, JSON, Boolean, Text
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import uuid

Base = declarative_base()


class DatasetConfig(Base):
    """Dataset configuration and schema definition"""
    __tablename__ = "dataset_configs"
    
    id = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String(255), nullable=False, unique=True)
    source_type = Column(String(50), nullable=False)  # s3, snowflake, csv
    connection_details = Column(Text, nullable=False)  # JSON as text
    schema_definition = Column(Text, nullable=False)  # JSON as text
    primary_key = Column(String(255))
    required_columns = Column(Text)  # JSON array as text
    quality_checks = Column(Text, nullable=False)  # JSON as text
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f"<DatasetConfig(id={self.id}, name={self.name}, source={self.source_type})>"


class QualityMetric(Base):
    """Quality check results and measurements"""
    __tablename__ = "quality_metrics"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(String(36), ForeignKey("dataset_configs.id"), nullable=False, index=True)
    run_id = Column(String(255), nullable=False, index=True)
    check_type = Column(String(100), nullable=False)  # null_check, duplicate_check, etc.
    status = Column(String(20), nullable=False)  # PASS, FAIL, WARNING
    value = Column(Numeric)
    threshold = Column(Numeric)
    details = Column(Text)  # JSON as text
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    def __repr__(self):
        return f"<QualityMetric(id={self.id}, check={self.check_type}, status={self.status})>"


class AnomalyRecord(Base):
    """Detected anomalies with AI explanations"""
    __tablename__ = "anomaly_records"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(String(36), ForeignKey("dataset_configs.id"), nullable=False, index=True)
    metric_id = Column(BigInteger, ForeignKey("quality_metrics.id"))
    run_id = Column(String(255), nullable=False)
    severity = Column(String(20), nullable=False)  # LOW, MEDIUM, HIGH, CRITICAL
    ai_explanation = Column(Text)
    root_cause = Column(Text)
    recommended_actions = Column(Text)  # JSON array as text
    risk_level = Column(String(20))
    detected_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    def __repr__(self):
        return f"<AnomalyRecord(id={self.id}, severity={self.severity}, risk={self.risk_level})>"


class ChatHistory(Base):
    """Chatbot conversation logs"""
    __tablename__ = "chat_history"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(String(36), ForeignKey("dataset_configs.id"))
    query = Column(Text, nullable=False)
    response = Column(Text, nullable=False)
    query_metadata = Column(Text)  # JSON as text
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    def __repr__(self):
        return f"<ChatHistory(id={self.id}, query={self.query[:50]})>"


class DAGRun(Base):
    """Track Airflow DAG runs"""
    __tablename__ = "dag_runs"
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    dataset_id = Column(String(36), ForeignKey("dataset_configs.id"), nullable=False, index=True)
    run_id = Column(String(255), nullable=False, unique=True, index=True)
    dag_id = Column(String(255), nullable=False)
    status = Column(String(50))  # running, success, failed
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    error_message = Column(Text)
    
    def __repr__(self):
        return f"<DAGRun(run_id={self.run_id}, status={self.status})>"
