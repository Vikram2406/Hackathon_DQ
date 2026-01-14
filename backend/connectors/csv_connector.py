"""
CSV file connector
"""
import pandas as pd
from typing import Optional, Dict, Any
from backend.connectors.base import BaseConnector
import os


class CSVConnector(BaseConnector):
    """Connector for CSV files"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize CSV connector
        
        Args:
            config: Must contain 'file_path' key
        """
        super().__init__(config)
        self.file_path = config.get('file_path')
        if not self.file_path:
            raise ValueError("file_path is required in config")
    
    def connect(self) -> bool:
        """Check if file exists"""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")
        return True
    
    def test_connection(self) -> bool:
        """Test if file is readable"""
        try:
            # Try reading first row
            pd.read_csv(self.file_path, nrows=1)
            return True
        except Exception as e:
            print(f"CSV connection test failed: {e}")
            return False
    
    def read_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from CSV file
        
        Args:
            limit: Maximum number of rows to read
        
        Returns:
            DataFrame containing the data
        """
        try:
            if limit:
                df = pd.read_csv(self.file_path, nrows=limit)
            else:
                df = pd.read_csv(self.file_path)
            
            print(f"Read {len(df)} rows from {self.file_path}")
            return df
        except Exception as e:
            raise Exception(f"Failed to read CSV file: {e}")
    
    def get_row_count(self) -> int:
        """
        Get total row count from CSV
        
        Returns:
            Total number of rows (excluding header)
        """
        try:
            # Efficient row counting without loading entire file
            with open(self.file_path, 'r') as f:
                row_count = sum(1 for line in f) - 1  # Subtract header
            return row_count
        except Exception as e:
            raise Exception(f"Failed to count rows: {e}")
    
    def disconnect(self):
        """No connection to close for CSV files"""
        pass
