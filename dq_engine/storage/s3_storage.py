"""
S3 Storage Implementation
"""
import os
import json
import boto3
from typing import Dict, List, Tuple, Optional
from datetime import datetime, timedelta
from .base_storage import BaseStorage


class S3Storage(BaseStorage):
    """Store validation results in S3"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-south-1')
        )
        self.results_bucket = os.getenv('DQ_RESULTS_BUCKET', 'project-cb')
        self.results_prefix = os.getenv('DQ_RESULTS_PREFIX', 'dq-reports/s3/')
    
    def save_results(self, results: Dict, source_id: str, metadata: Optional[Dict] = None) -> bool:
        """Save results to S3: s3://bucket/dq-reports/s3/{source_id}/"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            
            # Add metadata
            full_results = {
                **results,
                'source_type': 's3',
                'source_id': source_id,
                'saved_at': datetime.now().isoformat(),
                'metadata': metadata or {}
            }
            
            # Save timestamped version
            timestamped_key = f"{self.results_prefix}{source_id}/{timestamp}_validation.json"
            self.s3_client.put_object(
                Bucket=self.results_bucket,
                Key=timestamped_key,
                Body=json.dumps(full_results, indent=2),
                ContentType='application/json'
            )
            
            # Save latest version
            latest_key = f"{self.results_prefix}{source_id}/latest.json"
            self.s3_client.put_object(
                Bucket=self.results_bucket,
                Key=latest_key,
                Body=json.dumps(full_results, indent=2),
                ContentType='application/json'
            )
            
            return True
        except Exception as e:
            print(f"Error saving to S3: {e}")
            return False
    
    def get_latest(self, source_id: str) -> Optional[Dict]:
        """Get latest results from S3"""
        try:
            key = f"{self.results_prefix}{source_id}/latest.json"
            response = self.s3_client.get_object(
                Bucket=self.results_bucket,
                Key=key
            )
            data = json.loads(response['Body'].read().decode('utf-8'))
            return data
        except self.s3_client.exceptions.NoSuchKey:
            return None
        except Exception as e:
            print(f"Error reading from S3: {e}")
            return None
    
    def get_history(self, source_id: str, days: int = 7) -> List[Dict]:
        """Get historical results from S3"""
        try:
            prefix = f"{self.results_prefix}{source_id}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.results_bucket,
                Prefix=prefix
            )
            
            results = []
            cutoff_date = datetime.now() - timedelta(days=days)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if obj['Key'].endswith('_validation.json'):
                        if obj['LastModified'].replace(tzinfo=None) >= cutoff_date:
                            result = self.s3_client.get_object(
                                Bucket=self.results_bucket,
                                Key=obj['Key']
                            )
                            data = json.loads(result['Body'].read().decode('utf-8'))
                            results.append(data)
            
            return sorted(results, key=lambda x: x.get('timestamp', ''), reverse=True)
        except Exception as e:
            print(f"Error getting history from S3: {e}")
            return []
    
    def test_connection(self) -> Tuple[bool, str]:
        """Test S3 connection"""
        try:
            self.s3_client.head_bucket(Bucket=self.results_bucket)
            return True, "S3 connection successful"
        except Exception as e:
            return False, f"S3 connection failed: {str(e)}"
    
    def list_sources(self) -> List[str]:
        """List all S3 sources with results"""
        try:
            sources = []
            
            # List all objects with latest.json suffix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.results_bucket,
                Prefix=self.results_prefix
            )
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        # Find all latest.json files
                        if obj['Key'].endswith('/latest.json'):
                            # Extract source_id from path
                            # Format: dq-reports/s3/{source_id}/latest.json
                            key_parts = obj['Key'].replace(self.results_prefix, '').split('/')
                            if len(key_parts) >= 2:
                                # Reconstruct source_id (everything except latest.json)
                                source_id = '/'.join(key_parts[:-1])
                                sources.append(source_id)
            
            return sorted(sources)
        except Exception as e:
            print(f"Error listing sources: {e}")
            return []
