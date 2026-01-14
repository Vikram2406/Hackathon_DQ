"""
Connector factory for creating data source connectors
"""
from typing import Dict, Any
from backend.connectors.base import BaseConnector
from backend.connectors.csv_connector import CSVConnector
from backend.connectors.s3_connector import S3Connector
from backend.connectors.snowflake_connector import SnowflakeConnector


def get_connector(source_type: str, config: Dict[str, Any]) -> BaseConnector:
    """
    Factory function to create appropriate connector based on source type
    
    Args:
        source_type: Type of data source ('csv', 's3', 'snowflake')
        config: Connection configuration
    
    Returns:
        Connector instance
    
    Raises:
        ValueError: If source_type is not supported
    """
    connectors = {
        'csv': CSVConnector,
        's3': S3Connector,
        'snowflake': SnowflakeConnector,
    }
    
    connector_class = connectors.get(source_type.lower())
    if not connector_class:
        raise ValueError(f"Unsupported source type: {source_type}. Supported types: {list(connectors.keys())}")
    
    return connector_class(config)


__all__ = [
    'BaseConnector',
    'CSVConnector',
    'S3Connector',
    'SnowflakeConnector',
    'get_connector',
]
