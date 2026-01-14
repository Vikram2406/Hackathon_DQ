"""
AWS S3 connector
"""
import pandas as pd
from typing import Optional, Dict, Any
from backend.connectors.base import BaseConnector
import boto3
from io import StringIO, BytesIO
import os


class S3Connector(BaseConnector):
    """Connector for AWS S3"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize S3 connector
        
        Args:
            config: Must contain 'bucket', 'key', and optionally AWS credentials
        """
        super().__init__(config)
        self.bucket = config.get('bucket')
        self.key = config.get('key')
        self.file_format = config.get('file_format', 'csv')  # csv, parquet, json
        
        if not self.bucket or not self.key:
            raise ValueError("bucket and key are required in config")
        
        # AWS credentials (use from config or environment)
        self.aws_access_key = config.get('aws_access_key_id') or os.getenv('AWS_ACCESS_KEY_ID')
        self.aws_secret_key = config.get('aws_secret_access_key') or os.getenv('AWS_SECRET_ACCESS_KEY')
        self.region = config.get('region') or os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.s3_client = None
    
    def connect(self) -> bool:
        """Establish connection to S3"""
        try:
            if self.aws_access_key and self.aws_secret_key:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=self.aws_access_key,
                    aws_secret_access_key=self.aws_secret_key,
                    region_name=self.region
                )
            else:
                # Use default credentials (IAM role, etc.)
                self.s3_client = boto3.client('s3', region_name=self.region)
            
            return True
        except Exception as e:
            raise Exception(f"Failed to connect to S3: {e}")
    
    def test_connection(self) -> bool:
        """Test if S3 object exists and is accessible"""
        try:
            if not self.s3_client:
                self.connect()
            
            # Try to get object metadata
            self.s3_client.head_object(Bucket=self.bucket, Key=self.key)
            return True
        except Exception as e:
            print(f"S3 connection test failed: {e}")
            return False
    
    def read_data(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Read data from S3
        
        Args:
            limit: Maximum number of rows to read
        
        Returns:
            DataFrame containing the data
        """
        try:
            if not self.s3_client:
                self.connect()
            
            # Get object from S3
            response = self.s3_client.get_object(Bucket=self.bucket, Key=self.key)
            
            # Read based on file format
            if self.file_format == 'csv':
                df = pd.read_csv(BytesIO(response['Body'].read()), nrows=limit)
            elif self.file_format == 'parquet':
                df = pd.read_parquet(BytesIO(response['Body'].read()))
                if limit:
                    df = df.head(limit)
            elif self.file_format == 'json':
                df = pd.read_json(BytesIO(response['Body'].read()))
                if limit:
                    df = df.head(limit)
            else:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            
            print(f"Read {len(df)} rows from s3://{self.bucket}/{self.key}")
            return df
        except Exception as e:
            raise Exception(f"Failed to read from S3: {e}")
    
    def get_row_count(self) -> int:
        """
        Get total row count from S3 object
        
        Note: This loads the entire file, which may be slow for large files
        """
        try:
            df = self.read_data()
            return len(df)
        except Exception as e:
            raise Exception(f"Failed to count rows: {e}")
    
    
    def list_files(self, bucket: str = None, prefix: str = "", max_keys: int = 1000):
        """
        List files in S3 bucket
        
        Args:
            bucket: Bucket name (uses self.bucket if not provided)
            prefix: Prefix to filter files
            max_keys: Maximum number of files to return
        
        Returns:
            List of file keys
        """
        try:
            if not self.s3_client:
                self.connect()
            
            bucket_name = bucket or self.bucket
            if not bucket_name:
                raise ValueError("Bucket name is required")
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip directories (keys ending with /)
                    if not obj['Key'].endswith('/'):
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat()
                        })
            
            return files
        except Exception as e:
            raise Exception(f"Failed to list files in S3: {e}")
    
    def disconnect(self):
        """Close S3 connection"""
        self.s3_client = None
