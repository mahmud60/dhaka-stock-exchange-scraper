from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the main DAG
dag = DAG(
    'scrape_and_save_share_prices_with_company_data',
    default_args=default_args,
    description='Insert company data first, then scrape DSE share prices daily',
    schedule_interval=timedelta(days=1),  # Runs daily
)

from dse_utils import (
    create_company_table,
    insert_company_data,
    create_share_prices_table,
    save_share_prices_data,
    create_company_basic_table,
    extract_and_save_company_basic_data,
    create_company_market_data_table,
    extract_and_save_company_market_info,
)

# Define tasks
# Task 1: Create the company list table
t1 = PythonOperator(
    task_id='create_company_table',
    python_callable=create_company_table,
    dag=dag,
)

# Task 2: Insert company data (runs yearly)
t2 = PythonOperator(
    task_id='insert_company_data',
    python_callable=insert_company_data,
    dag=dag,
)

# Task 3: Create the share prices table
t3 = PythonOperator(
    task_id='create_share_prices_table',
    python_callable=create_share_prices_table,
    dag=dag,
)

# Task 4: Save share prices data
t4 = PythonOperator(
    task_id='save_share_prices_data',
    python_callable=save_share_prices_data,
    dag=dag,
)

t5 = PythonOperator(
    task_id='create_company_basic_table',
    python_callable=create_company_basic_table,
    dag=dag,
)

t6 = PythonOperator(
    task_id='extract_and_save_company_basic_info',
    python_callable=extract_and_save_company_basic_data,
    dag=dag,
)

t7 = PythonOperator(
    task_id='create_company_market_data_table',
    python_callable=create_company_market_data_table,
    dag=dag,
)

t8 = PythonOperator(
    task_id='extract_and_save_company_market_info',
    python_callable=extract_and_save_company_market_info,
    dag=dag,
)
# Set task dependencies
t1 >> t2 >> [t3,t5,t7]
t3 >> t4  
t5 >> t6  
t7 >> t8

# Ensure insert_company_data runs only once a year
t2.execution_timeout = timedelta(days=365)
