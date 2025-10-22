import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository
from examples.cdm.cdm_courier_ledger_dag.cdm_courier_ledger_loader import CourierLedgerLoader
from lib import ConnectionBuilder 

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2025, 8, 31, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def cdm_courier_ledger_report_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    settings_repository = DdsEtlSettingsRepository()

    # Объявляем таск, который загружает данные.
    @task(task_id="cdm_courier_ledger_report_load")
    def cdm_courier_ledger_report(ds=None, **kwargs):
        cdm_courier_ledger_report_loader = CourierLedgerLoader(dwh_pg_connect)
        cdm_courier_ledger_report_loader.load_courier_ledger() 
    # Инициализируем объявленные таски.
    cdm_courier_ledger_report = cdm_courier_ledger_report()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    cdm_courier_ledger_report  # type: ignore

cdm_courier_ledger_report_dag = cdm_courier_ledger_report_dag()
