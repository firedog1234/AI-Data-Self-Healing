"""
Airflow DAG for orchestrating ETL pipeline and self-healing tasks
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
import logging

logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_pipeline_self_healing',
    default_args=default_args,
    description='ETL Pipeline with Self-Healing Orchestration',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['etl', 'self-healing', 'data-quality'],
)

def trigger_etl_pipeline(**context):
    """Trigger ETL pipeline via API"""
    try:
        etl_service_url = "http://etl-service:8001"
        response = requests.post(
            f"{etl_service_url}/pipeline/run",
            json={
                "pipeline_name": "student_data_etl",
                "simulate_failures": True
            },
            timeout=300
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"ETL pipeline triggered: {result}")
        
        # Store run_id in XCom for downstream tasks
        context['ti'].xcom_push(key='run_id', value=result['run_id'])
        return result
        
    except Exception as e:
        logger.error(f"Failed to trigger ETL pipeline: {e}")
        raise

def check_pipeline_status(**context):
    """Check pipeline run status"""
    try:
        run_id = context['ti'].xcom_pull(key='run_id', task_ids='trigger_etl')
        if not run_id:
            raise ValueError("No run_id found from trigger_etl task")
        
        etl_service_url = "http://etl-service:8001"
        response = requests.get(
            f"{etl_service_url}/pipeline/runs",
            timeout=30
        )
        response.raise_for_status()
        runs = response.json().get('runs', [])
        
        # Find the current run
        current_run = next((r for r in runs if r['run_id'] == run_id), None)
        if not current_run:
            raise ValueError(f"Run {run_id} not found")
        
        status = current_run['status']
        logger.info(f"Pipeline run {run_id} status: {status}")
        
        if status == 'failed':
            raise ValueError(f"Pipeline run {run_id} failed")
        
        return status
        
    except Exception as e:
        logger.error(f"Failed to check pipeline status: {e}")
        raise

def trigger_monitoring(**context):
    """Trigger monitoring service to process events"""
    try:
        # Monitoring service consumes Kafka events automatically
        # This task just logs and waits a bit for events to be processed
        logger.info("Monitoring service is processing pipeline events...")
        
        run_id = context['ti'].xcom_pull(key='run_id', task_ids='trigger_etl')
        
        # Wait a bit and check for issues
        import time
        time.sleep(10)  # Give monitoring service time to process
        
        monitoring_service_url = "http://monitoring-service:8002"
        response = requests.get(
            f"{monitoring_service_url}/issues",
            timeout=30
        )
        response.raise_for_status()
        issues = response.json().get('issues', [])
        
        # Filter issues for this run
        run_issues = [i for i in issues if i.get('run_id') == run_id]
        logger.info(f"Found {len(run_issues)} issues for run {run_id}")
        
        return len(run_issues)
        
    except Exception as e:
        logger.error(f"Failed to trigger monitoring: {e}")
        # Don't fail the DAG if monitoring fails
        return 0

def check_self_healing(**context):
    """Check if self-healing service has generated fixes"""
    try:
        run_id = context['ti'].xcom_pull(key='run_id', task_ids='trigger_etl')
        
        # Wait for self-healing to process
        import time
        time.sleep(15)
        
        self_healing_service_url = "http://self-healing-service:8004"
        response = requests.get(
            f"{self_healing_service_url}/fixes",
            timeout=30
        )
        response.raise_for_status()
        fixes = response.json().get('fixes', [])
        
        # Filter fixes related to this run's issues
        logger.info(f"Found {len(fixes)} fixes generated")
        
        return len(fixes)
        
    except Exception as e:
        logger.error(f"Failed to check self-healing: {e}")
        return 0

# Define tasks
trigger_etl_task = PythonOperator(
    task_id='trigger_etl',
    python_callable=trigger_etl_pipeline,
    dag=dag,
)

check_status_task = PythonOperator(
    task_id='check_pipeline_status',
    python_callable=check_pipeline_status,
    dag=dag,
)

trigger_monitoring_task = PythonOperator(
    task_id='trigger_monitoring',
    python_callable=trigger_monitoring,
    dag=dag,
)

check_self_healing_task = PythonOperator(
    task_id='check_self_healing',
    python_callable=check_self_healing,
    dag=dag,
)

# Log completion
log_completion_task = BashOperator(
    task_id='log_completion',
    bash_command='echo "ETL Pipeline DAG completed at $(date)"',
    dag=dag,
)

# Define task dependencies
trigger_etl_task >> check_status_task >> trigger_monitoring_task >> check_self_healing_task >> log_completion_task

