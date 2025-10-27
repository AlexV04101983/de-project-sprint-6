import logging
import pendulum
from lib.variables import EnvVariables
from airflow.decorators import dag, task
import boto3

@dag(
    schedule_interval= None, #'0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 's3',  'read_parameters', 'test'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
    )

def sprint6_variables_reading_dag():
    # Объявляем таск, который загружает данные.
    @task()
    def load_variables():
        envvar = EnvVariables("AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY")
        print("1: " + envvar.get_access_key_id())
        print("2: " +  envvar.get_secret_access_key())
    # Инициализируем объявленные таски.
    variables_loader = load_variables()
    @task()
    def extract_s3_data():
        envvar = EnvVariables("AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY")
        #print("1: " + envvar.get_access_key_id())
        #print("2: " +  envvar.get_secret_access_key())
        AWS_ACCESS_KEY_ID = "YCAJEiyNFq4wiOe_eMCMCXmQP"
        AWS_SECRET_ACCESS_KEY = "YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS"

        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id = AWS_ACCESS_KEY_ID,
            aws_secret_access_key=  AWS_SECRET_ACCESS_KEY,
        )

        s3_client.download_file(
            Bucket='sprint6',
            Key='groups.csv',
            Filename='/lessons/dags/data/groups.csv'
        )
    s3_data_extract_task = extract_s3_data()

    variables_loader >> s3_data_extract_task

sprint6_test_dag = sprint6_variables_reading_dag()