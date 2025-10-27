import logging
import pendulum
from lib.variables import EnvVariables
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import boto3

AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"

def fetch_s3_file(bucket: str, key: str):
    # сюда поместить код из скрипта для скачивания файла
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    ) 
bash_command_tmpl = """echo {{ params.files }}"""


@dag(
    schedule_interval= None, #'0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 's3',  'fetch_data', 'groups', 'dialogs', 'group_log'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation = True  # Остановлен/запущен при появлении. Сразу запущен.
    )

def sprint6_s3_data_reading():

    bucket_files = ['dialog.csv','groups.csv','users.csv']

    fetch_task1 = PythonOperator(
        task_id=f'fetch_groups.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'groups.csv'},
    )

    fetch_task2 = PythonOperator(
        task_id=f'fetch_dialogs.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'dialogs.csv'},
    )

    fetch_task3 = PythonOperator(
        task_id=f'fetch_users.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'users.csv'},
    )
    # S6-Project
    fetch_task4 = PythonOperator(
        task_id=f'group_log.csv',
        python_callable=fetch_s3_file,
        op_kwargs={'bucket': 'sprint6', 'key': 'group_log.csv'},
    )

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': [f'/data/{f}' for f in bucket_files]}
    )    

    fetch_task1 >> fetch_task2 >> fetch_task3 >> fetch_task4 >>  print_10_lines_of_each

sprint6_test_dag = sprint6_s3_data_reading()