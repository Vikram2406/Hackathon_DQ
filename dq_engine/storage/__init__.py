"""
Storage module for multi-source result persistence
"""
from .base_storage import BaseStorage
from .s3_storage import S3Storage
from .factory import StorageFactory

__all__ = ['BaseStorage', 'S3Storage', 'StorageFactory']
