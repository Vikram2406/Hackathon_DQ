"""
Snowflake connector
"""
import pandas as pd
from typing import Optional, Dict, Any
from backend.connectors.base import BaseConnector
import os

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import pd_writer
    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


class SnowflakeConnector(BaseConnector):
    """Connector for Snowflake data warehouse"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Snowflake connector
        
        Args:
            config: Must contain connection details
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError("snowflake-connector-python is not installed")
        
        super().__init__(config)
        
        # Connection parameters
        self.account = config.get('account') or os.getenv('SNOWFLAKE_ACCOUNT')
        self.user = config.get('user') or os.getenv('SNOWFLAKE_USER')
        self.password = config.get('password') or os.getenv('SNOWFLAKE_PASSWORD')
        self.warehouse = config.get('warehouse') or os.getenv('SNOWFLAKE_WAREHOUSE')
        self.database = config.get('database') or os.getenv('SNOWFLAKE_DATABASE')
        self.schema = config.get('schema') or os.getenv('SNOWFLAKE_SCHEMA')
        self.table = config.get('table')
        self.query = config.get('query')  # Optional custom query
        
        if not all([self.account, self.user, self.password, self.warehouse, self.database]):
            raise ValueError("Missing required Snowflake connection parameters")
        
        if not self.table and not self.query:
            raise ValueError("Either 'table' or 'query' must be provided")
    
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            self.connection = snowflake.connector.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            return True
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {e}")
    
    def test_connection(self) -> bool:
        """Test if connection is valid"""
        try:
            if not self.connection:
                self.connect()
            
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception as e:
            print(f"Snowflake connection test failed: {e}")
            return False
    
    def read_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from Snowflake
        
        Args:
            limit: Maximum number of rows to read
        
        Returns:
            DataFrame containing the data
        """
        try:
            if not self.connection:
                self.connect()
            
            # Build query
            if self.query:
                query = self.query
            else:
                query = f"SELECT * FROM {self.table}"
            
            if limit:
                query += f" LIMIT {limit}"
            
            # Execute query and fetch as DataFrame
            df = pd.read_sql(query, self.connection)
            
            print(f"Read {len(df)} rows from Snowflake")
            return df
        except Exception as e:
            raise Exception(f"Failed to read from Snowflake: {e}")
    
    def get_row_count(self) -> int:
        """
        Get total row count from Snowflake table
        
        Returns:
            Total number of rows
        """
        try:
            if not self.connection:
                self.connect()
            
            if self.query:
                # For custom queries, use COUNT over subquery
                count_query = f"SELECT COUNT(*) as cnt FROM ({self.query}) as subquery"
            else:
                count_query = f"SELECT COUNT(*) as cnt FROM {self.table}"
            
            cursor = self.connection.cursor()
            cursor.execute(count_query)
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result else 0
        except Exception as e:
            raise Exception(f"Failed to count rows: {e}")
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
