from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weekly_aggregates',
    default_args=default_args,
    description='Compute weekly aggregates from logs',
    schedule_interval='0 7 * * *',  #запуск каждый день в 7:00
    catchup=False
)

compute_task = BashOperator(
    task_id='compute_aggregates',
    bash_command='python /opt/airflow/dags/script.py {{ ds }}',
    dag=dag,
)