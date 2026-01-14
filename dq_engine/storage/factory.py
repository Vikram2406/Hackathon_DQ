"""
Storage Factory - Returns appropriate storage backend
"""
from typing import Dict
from .base_storage import BaseStorage
from .s3_storage import S3Storage


class StorageFactory:
    """Factory to create appropriate storage backend based on source type"""
    
    _storage_map = {
        's3': S3Storage,
        # Future: 'databricks': DatabricksStorage,
        # Future: 'snowflake': SnowflakeStorage,
    }
    
    @classmethod
    def get_storage(cls, source_type: str) -> BaseStorage:
        """
        Get storage implementation for source type
        
        Args:
            source_type: Type of data source ('s3', 'databricks', 'snowflake')
            
        Returns:
            BaseStorage: Storage implementation
            
        Raises:
            ValueError: If source type not supported
        """
        storage_class = cls._storage_map.get(source_type)
        if not storage_class:
            raise ValueError(f"Unsupported source type: {source_type}")
        
        return storage_class()
    
    @classmethod
    def supported_sources(cls) -> list:
        """Get list of supported source types"""
        return list(cls._storage_map.keys())
