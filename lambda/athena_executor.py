# lambda/athena_executor.py
"""
Athena Query Executor Module
Handles all interactions with Amazon Athena service
"""

import boto3
import time
from datetime import datetime
from typing import Dict, Optional

class AthenaExecutor:
    """Execute and monitor Athena queries."""
    
    def __init__(self, bucket: str, region: str = 'us-east-1'):
        """
        Initialize Athena executor.
        
        Args:
            bucket: S3 bucket for Athena results
            region: AWS region
        """
        self.athena_client = boto3.client('athena', region_name=region)
        self.bucket = bucket
        self.output_location = f's3://{bucket}/athena_results/'
    
    def execute_query(
        self, 
        query: str, 
        database: Optional[str] = None, 
        label: Optional[str] = None,
        max_attempts: int = 150
    ) -> Dict:
        """
        Execute a single Athena query and wait for completion.
        
        Args:
            query: SQL query to execute
            database: Database context (optional)
            label: Label for logging
            max_attempts: Max polling attempts
        
        Returns:
            Dict with status and query_id or error
        """
        if label:
            print(f"\n{'='*60}")
            print(f"Running: {label}")
            print(f"{'='*60}")
        
        print(f"Query:\n{query[:200]}..." if len(query) > 200 else f"Query:\n{query}")
        
        try:
            params = {
                'QueryString': query,
                'ResultConfiguration': {'OutputLocation': self.output_location}
            }
            
            # Only set DB context when not creating/dropping DB itself
            if database and not any(
                keyword in query.upper()
                for keyword in ['CREATE DATABASE', 'DROP DATABASE', 'SHOW DATABASES']
            ):
                params['QueryExecutionContext'] = {'Database': database}
            
            response = self.athena_client.start_query_execution(**params)
            query_id = response['QueryExecutionId']
            
            # Poll for completion
            for attempt in range(max_attempts):
                result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    print("✓ Query succeeded")
                    return {'status': 'success', 'query_id': query_id}
                
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = result['QueryExecution']['Status'].get(
                        'StateChangeReason', 'Unknown error'
                    )
                    print(f"✗ Query failed: {error_msg}")
                    return {'status': 'failed', 'error': error_msg}
                
                time.sleep(2)
            
            print("✗ Query timeout")
            return {'status': 'timeout', 'error': 'Query execution timeout'}
        
        except Exception as e:
            print(f"✗ Query execution error: {str(e)}")
            return {'status': 'error', 'error': str(e)}