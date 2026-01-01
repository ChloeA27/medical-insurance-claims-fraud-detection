# lambda/etl_pipeline.py
"""
Medical Claims ETL Pipeline
Main Lambda function for orchestrating data warehouse build
"""

import json
from datetime import datetime
from typing import Dict, List

from config import Config
from athena_executor import AthenaExecutor


class ClaimsETLPipeline:
    """Main ETL pipeline orchestrator."""
    
    def __init__(self):
        """Initialize pipeline."""
        self.executor = AthenaExecutor(bucket=Config.BUCKET, region=Config.REGION)
        self.database = Config.DATABASE
        self.results = {
            "views_created": [],
            "dims_created": [],
            "facts_created": []
        }
    
    def step_views(self) -> List[str]:
        """Create transformation views."""
        print("\n" + "="*80)
        print("STEP: CREATE VIEWS")
        print("="*80)
        
        view_files = [
            ('sql/01-views/v_providers_etl.sql', 'v_providers_etl'),
            ('sql/01-views/v_patients_etl.sql', 'v_patients_etl'),
            ('sql/01-views/v_inpatient_claims_etl.sql', 'v_inpatient_claims_etl'),
            ('sql/01-views/v_outpatient_claims_etl.sql', 'v_outpatient_claims_etl'),
            ('sql/01-views/v_all_claims_etl.sql', 'v_all_claims_etl'),
        ]
        
        created_views = []
        for sql_file, view_name in view_files:
            # Drop if exists
            drop_query = f"DROP VIEW IF EXISTS {view_name}"
            self.executor.execute_query(drop_query, self.database, label=f"Drop view {view_name}")
            
            # Create view
            try:
                query = Config.get_sql_file(sql_file)
                res = self.executor.execute_query(
                    query, 
                    self.database, 
                    label=f"Create view {view_name}"
                )
                if res['status'] != 'success':
                    raise Exception(f"Failed to create {view_name}: {res.get('error')}")
                created_views.append(view_name)
            except Exception as e:
                print(f"ERROR: {str(e)}")
                raise
        
        return created_views
    
    def step_dims(self) -> List[str]:
        """Create dimension tables."""
        print("\n" + "="*80)
        print("STEP: CREATE DIMENSION TABLES")
        print("="*80)
        
        dim_files = [
            'sql/02-dims/dim_date_etl.sql',
            'sql/02-dims/dim_provider_etl.sql',
            'sql/02-dims/dim_patient_etl.sql',
            'sql/02-dims/dim_diagnosis_etl.sql',
            'sql/02-dims/dim_procedure_etl.sql',
        ]
        
        created_dims = []
        for sql_file in dim_files:
            table_name = sql_file.split('/')[-1].replace('.sql', '')
            
            try:
                drop_query = f"DROP TABLE IF EXISTS {table_name}"
                self.executor.execute_query(drop_query, self.database)
                
                query = Config.get_sql_file(sql_file)
                res = self.executor.execute_query(
                    query,
                    self.database,
                    label=f"Create {table_name}"
                )
                
                if res['status'] != 'success':
                    raise Exception(f"Failed: {res.get('error')}")
                created_dims.append(table_name)
            except Exception as e:
                print(f"ERROR: {str(e)}")
                raise
        
        return created_dims
    
    def step_facts(self) -> List[str]:
        """Create fact tables."""
        print("\n" + "="*80)
        print("STEP: CREATE FACT TABLES")
        print("="*80)
        
        fact_files = [
            'sql/03-facts/fact_claims_etl.sql',
            'sql/03-facts/fact_provider_summary_etl.sql',
            'sql/03-facts/fact_patient_claims_summary_etl.sql',
        ]
        
        created_facts = []
        for sql_file in fact_files:
            table_name = sql_file.split('/')[-1].replace('.sql', '')
            
            try:
                drop_query = f"DROP TABLE IF EXISTS {table_name}"
                self.executor.execute_query(drop_query, self.database)
                
                query = Config.get_sql_file(sql_file)
                res = self.executor.execute_query(
                    query,
                    self.database,
                    label=f"Create {table_name}"
                )
                
                if res['status'] != 'success':
                    raise Exception(f"Failed: {res.get('error')}")
                created_facts.append(table_name)
            except Exception as e:
                print(f"ERROR: {str(e)}")
                raise
        
        return created_facts
    
    def step_validate(self) -> bool:
        """Validate data warehouse tables."""
        print("\n" + "="*80)
        print("STEP: VALIDATION")
        print("="*80)
        
        queries = [
            "SELECT 'dim_date_etl' AS table_name, COUNT(*) AS cnt FROM dim_date_etl",
            "SELECT 'dim_provider_etl', COUNT(*) FROM dim_provider_etl",
            "SELECT 'dim_patient_etl', COUNT(*) FROM dim_patient_etl",
            "SELECT 'dim_diagnosis_etl', COUNT(*) FROM dim_diagnosis_etl",
            "SELECT 'dim_procedure_etl', COUNT(*) FROM dim_procedure_etl",
            "SELECT 'fact_claims_etl', COUNT(*) FROM fact_claims_etl",
            "SELECT 'fact_provider_summary_etl', COUNT(*) FROM fact_provider_summary_etl",
            "SELECT 'fact_patient_claims_summary_etl', COUNT(*) FROM fact_patient_claims_summary_etl",
        ]
        
        for query in queries:
            self.executor.execute_query(query, self.database, label="Validation")
        
        return True
    
    def run(self, step: str = 'all') -> Dict:
        """
        Run ETL pipeline.
        
        Args:
            step: Which step to run ('raw', 'views', 'dims', 'facts', 'validate', 'all')
        
        Returns:
            Result dict with status and created tables
        """
        try:
            if step in ['all', 'views']:
                self.results['views_created'] = self.step_views()
            
            if step in ['all', 'dims']:
                self.results['dims_created'] = self.step_dims()
            
            if step in ['all', 'facts']:
                self.results['facts_created'] = self.step_facts()
            
            if step in ['all', 'validate']:
                self.step_validate()
            
            print("\n✅ ETL pipeline completed successfully!")
            return {
                'statusCode': 200,
                'timestamp': str(datetime.now()),
                'step': step,
                **self.results
            }
        
        except Exception as e:
            print(f"\n❌ Pipeline error: {str(e)}")
            return {
                'statusCode': 500,
                'timestamp': str(datetime.now()),
                'error': str(e),
                **self.results
            }


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    Event format:
    {
        "step": "all" | "views" | "dims" | "facts" | "validate"
    }
    """
    print("="*80)
    print("Medical Claims ETL Pipeline (Lambda / Athena)")
    print(f"Started at: {datetime.now()}")
    print("="*80)
    
    step = (event or {}).get('step', 'all')
    print(f"Requested step: {step}")
    
    pipeline = ClaimsETLPipeline()
    result = pipeline.run(step)
    
    return result


if __name__ == '__main__':
    # Local testing
    result = lambda_handler({'step': 'all'}, None)
    print(json.dumps(result, indent=2))