"""
Configuration settings for the application
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Database
    database_url: str = "postgresql+psycopg2://dq_user:dq_password@localhost:5432/data_quality"
    
    # Redis
    redis_url: str = "redis://localhost:6379/0"
    
    # Airflow
    airflow_api_url: str = "http://localhost:8080/api/v1"
    airflow_username: str = "admin"
    airflow_password: str = "admin"
    
    # OpenAI
    openai_api_key: str = ""
    
    # AWS
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_default_region: str = "us-east-1"
    
    # Snowflake
    snowflake_account: Optional[str] = None
    snowflake_user: Optional[str] = None
    snowflake_password: Optional[str] = None
    snowflake_warehouse: Optional[str] = None
    snowflake_database: Optional[str] = None
    snowflake_schema: Optional[str] = None
    
    # Backend
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000
    
    # Application
    environment: str = "development"
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
