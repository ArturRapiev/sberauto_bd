from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime as dt
import sys
import os

path = '/home/artur/projects/my_project_1/de_project'
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

# Импортируем функции из скриптов
from scripts.parquet_processing import run_etl
from scripts.json_processing import process_json

# Определяем DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    'etl_to_postgresql_dag',  # Имя DAG
    default_args=default_args,
    description='ETL процесс проекта',
    schedule_interval=None,
    start_date=dt.datetime(2024, 11, 19),
    catchup=False,
) as dag:

    # Задача для запуска ETL для parquet файлов
    etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl,
        dag=dag,
    )

    # Задача для запуска ETL для JSON
    json_processing_task = PythonOperator(
        task_id='run_json_processing_task',
        python_callable=process_json,
        dag=dag,
    )

    # Определяем порядок выполнения задач
    etl_task >> json_processing_task
