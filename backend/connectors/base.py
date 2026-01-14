"""
Base connector interface for data sources
"""
from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Any, Optional


class BaseConnector(ABC):
    """Abstract base class for data source connectors"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize connector with configuration
        
        Args:
            config: Connection details specific to the data source
        """
        self.config = config
        self.connection = None
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Establish connection to data source
        
        Returns:
            True if connection successful, False otherwise
        """
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """
        Test if connection is valid
        
        Returns:
            True if connection is valid, False otherwise
        """
        pass
    
    @abstractmethod
    def read_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from source
        
        Args:
            limit: Maximum number of rows to read (for sampling)
        
        Returns:
            DataFrame containing the data
        """
        pass
    
    @abstractmethod
    def get_row_count(self) -> int:
        """
        Get total row count without loading data
        
        Returns:
            Total number of rows
        """
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection to data source"""
        pass
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
