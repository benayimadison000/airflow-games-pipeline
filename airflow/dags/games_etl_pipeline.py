from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'ben',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# Define the DAG
with DAG(
    dag_id='games_etl_pipeline',
    default_args=default_args,
    description='Extract, transform and load games data into Postgres',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['games', 'etl'],
) as dag:

    def extract():
        print("Step 1 - Extracting data...")
        print("Extraction complete.")

    def transform():
        print("Step 2 - Transforming data...")
        print("Transformation complete.")

    def load():
        print("Step 3 - Loading data into Postgres...")
        print("Load complete.")

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # Define dependencies — this is the DAG
    extract_task >> transform_task >> load_task