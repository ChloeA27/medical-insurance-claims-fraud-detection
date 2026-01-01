# lambda/config.py
"""
Configuration for ETL Pipeline
Load from environment variables or use defaults
"""

import os

# AWS Configuration
AWS_BUCKET = os.getenv('AWS_BUCKET', 'insurance-claim-qian-2025')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
AWS_DATABASE = os.getenv('AWS_DATABASE', 'insurance_claim_db')

# SQL Configuration
SQL_DIR = os.path.join(os.path.dirname(__file__), '..', 'sql')

# Logging
VERBOSE = os.getenv('VERBOSE', 'True').lower() == 'true'

class Config:
    """Configuration object."""
    
    BUCKET = AWS_BUCKET
    REGION = AWS_REGION
    DATABASE = AWS_DATABASE
    SQL_DIR = SQL_DIR
    VERBOSE = VERBOSE
    
    @staticmethod
    def get_sql_file(path: str) -> str:
        """Read SQL file from disk."""
        full_path = os.path.join(Config.SQL_DIR, path)
        if not os.path.exists(full_path):
            raise FileNotFoundError(f"SQL file not found: {full_path}")
        with open(full_path, 'r') as f:
            return f.read()