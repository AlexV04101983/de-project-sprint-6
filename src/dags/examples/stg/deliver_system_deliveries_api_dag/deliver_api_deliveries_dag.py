import logging
import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder
from examples.stg.deliver_system_deliveries_api_dag.pg_saver import PgSaver
from examples.stg.deliver_system_deliveries_api_dag.deliver_api_deliveries_loader import DeliveriesLoader 
from examples.stg.deliver_system_deliveries_api_dag.delivers_api_deliveries_reader import DeliveriesReader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',                 # каждые 15 минут deliveries
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'api', 'deliveries'],
    is_paused_upon_creation=True
)
def stg_deliver_system_deliveries_api():
    # Подключение к DWH (Airflow Connection с id "PG_WAREHOUSE_CONNECTION")
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Параметры API 
    api_base_url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
    api_nickname = "VSharonov"
    api_cohort   = "39"
    api_key      = "25c27781-8fde-4b30-a22e-524044a7580f" 

    @task()
    def load_deliveries():
        # Инициализация компонентов
        reader = DeliveriesReader()
        saver  = PgSaver()
        loader = DeliveriesLoader(reader, dwh_pg_connect, saver, log)

        # Переопределяем параметры у лоадера значениями 
        loader.api_endpoint = api_base_url
        loader.nickname     = api_nickname
        loader.cohort       = api_cohort
        loader.api_token    = api_key
        loader.headers = {
            'X-Nickname': loader.nickname,
            'X-Cohort': loader.cohort,
            'X-API-KEY': loader.api_token
        }

        # Запуск копирования (постранично, offset хранится в stg.srv_wf_settings)
        loader.run_copy()

    load_task = load_deliveries()
    load_task  # ignore 

deliver_system_deliveries_api_dag = stg_deliver_system_deliveries_api()  # noqa