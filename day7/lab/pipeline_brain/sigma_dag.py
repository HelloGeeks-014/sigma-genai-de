from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import logging
import json

default_args = {
    'owner': 'data-engineering',
   'retries': 2,
   'retry_delay': timedelta(minutes=5),
    'email_on_failure': True
}

def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    exec_date = context['execution_date']
    error_msg = context['exception']
    logging.error(f"Dag ID: {dag_id}, Task ID: {task_id}, Execution Date: {exec_date}, Error: {error_msg}")

def sla_miss_callback(context):
    dag_id = context['dag'].dag_id
    exec_date = context['execution_date']
    logging.error(f"Dag ID: {dag_id}, Execution Date: {exec_date}, SLA Miss")

def extract_bronze(**context):
    """Ingest raw CSVs to Bronze Parquet"""
    ti = context['task_instance']
    ti.xcom_push(key='run_metadata', value={'start_time': datetime.now()})
    logging.info(f"{ti} - Starting Bronze layer extraction")
    # Add your Bronze layer extraction logic here
    ti.xcom_push(key='run_metadata', value={'end_time': datetime.now()})
    logging.info(f"{ti} - Finished Bronze layer extraction")

def transform_silver(**context):
    """Clean, enrich, deduplicate to Silver"""
    ti = context['task_instance']
    ti.xcom_push(key='run_metadata', value={'start_time': datetime.now()})
    logging.info(f"{ti} - Starting Silver layer transformation")
    # Add your Silver layer transformation logic here
    ti.xcom_push(key='run_metadata', value={'end_time': datetime.now()})
    logging.info(f"{ti} - Finished Silver layer transformation")

def build_gold(**context):
    """Generate the 3 Gold aggregation tables"""
    ti = context['task_instance']
    ti.xcom_push(key='run_metadata', value={'start_time': datetime.now()})
    logging.info(f"{ti} - Starting Gold layer build")
    # Add your Gold layer build logic here
    ti.xcom_push(key='run_metadata', value={'end_time': datetime.now()})
    logging.info(f"{ti} - Finished Gold layer build")

with DAG(
    dag_id='sigma_transaction_pipeline',
    schedule='0 2 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    on_failure_callback=on_failure_callback,
    sla_miss_callback=sla_miss_callback,
    tags=['sigma', 'transactions', 'daily'],
    description="Daily Bronze->Silver->Gold pipeline for Sigma DataTech transactions"
) as dag:

    start = EmptyOperator(task_id='start')

    extract_bronze_task = PythonOperator(
        task_id='extract_bronze',
        python_callable=extract_bronze,
        on_failure_callback=on_failure_callback
    )

    transform_silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
        on_failure_callback=on_failure_callback
    )

    build_gold_task = PythonOperator(
        task_id='build_gold',
        python_callable=build_gold,
        on_failure_callback=on_failure_callback
    )

    end = EmptyOperator(task_id='end')

    start >> extract_bronze_task >> transform_silver_task >> build_gold_task >> end
