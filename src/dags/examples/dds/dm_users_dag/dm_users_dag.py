import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository
from examples.dds.dm_users_dag.dm_users_loader import UserLoader
from lib import ConnectionBuilder 




log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 8, 31, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_dm_users_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    settings_repository = DdsEtlSettingsRepository()

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_users_load")
    def load_dm_users(ds=None, **kwargs):
        user_loader = UserLoader(dwh_pg_connect, settings_repository)
        user_loader.load_users() 
    # Инициализируем объявленные таски.
    dm_users = load_dm_users()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_users  # type: ignore


dds_dm_users_dag = dds_dm_users_dag()
