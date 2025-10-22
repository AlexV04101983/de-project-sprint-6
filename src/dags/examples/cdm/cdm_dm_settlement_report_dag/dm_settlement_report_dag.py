import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository
from examples.cdm.cdm_dm_settlement_report_dag.dm_settlement_report_loader import SettlementReportLoader
from lib import ConnectionBuilder 

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 8, 31, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def cdm_dm_settlement_report_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    settings_repository = DdsEtlSettingsRepository()

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_settlement_report_load")
    def load_dm_settlement_report(ds=None, **kwargs):
        Settlement_report_loader = SettlementReportLoader(dwh_pg_connect)
        Settlement_report_loader.load_settlement_report() 
    # Инициализируем объявленные таски.
    dm_settlement_report = load_dm_settlement_report()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    dm_settlement_report  # type: ignore

cdm_dm_settlement_report_dag = cdm_dm_settlement_report_dag()
