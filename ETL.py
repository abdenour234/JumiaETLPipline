from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importing the ETL functions
from Extract import extract
from Transform import transform
from Load import load

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'etl_dag',
    default_args=default_args,
    description='Jumia ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 11, 5),
    catchup=False,
)

# Define the extract task
def extract_task(**kwargs):
    df = extract()
    kwargs['ti'].xcom_push(key='extract_data', value=df)

extract_operator = PythonOperator(
    task_id='extract_task',
    python_callable=extract_task,
    provide_context=True,
    dag=dag,
)

# Define the transform task
def transform_task(**kwargs):
    ti = kwargs['ti']
    extract_data = ti.xcom_pull(task_ids='extract_task', key='extract_data')
    transformed_data = transform(extract_data)
    ti.xcom_push(key='transform_data', value=transformed_data)

transform_operator = PythonOperator(
    task_id='transform_task',
    python_callable=transform_task,
    provide_context=True,
    dag=dag,
)

# Define the load task
def load_task(**kwargs):
    ti = kwargs['ti']
    transform_data = ti.xcom_pull(task_ids='transform_task', key='transform_data')
    load(transform_data)

load_operator = PythonOperator(
    task_id='load_task',
    python_callable=load_task,
    provide_context=True,
    dag=dag,
)

# Setting the task dependencies
extract_operator >> transform_operator >> load_operator
