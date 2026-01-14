"""
Base Storage Interface for Multi-Source Results
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Optional
from datetime import datetime


class BaseStorage(ABC):
    """Abstract base class for storing validation results"""
    
    @abstractmethod
    def save_results(self, results: Dict, source_id: str, metadata: Optional[Dict] = None) -> bool:
        """
        Save validation results to source-native storage
        
        Args:
            results: Validation results dictionary
            source_id: Unique identifier for the data source
            metadata: Optional metadata about the validation run
            
        Returns:
            bool: True if save successful
        """
        pass
    
    @abstractmethod
    def get_latest(self, source_id: str) -> Optional[Dict]:
        """
        Retrieve latest validation results for a source
        
        Args:
            source_id: Unique identifier for the data source
            
        Returns:
            Dict: Latest validation results or None if not found
        """
        pass
    
    @abstractmethod
    def get_history(self, source_id: str, days: int = 7) -> List[Dict]:
        """
        Get historical validation results
        
        Args:
            source_id: Unique identifier for the data source
            days: Number of days of history to retrieve
            
        Returns:
            List[Dict]: Historical validation results
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test if storage backend is accessible
        
        Returns:
            Tuple[bool, str]: (success, message)
        """
        pass
    
    @abstractmethod
    def list_sources(self) -> List[str]:
        """
        List all sources that have validation results
        
        Returns:
            List[str]: List of source identifiers
        """
        pass
