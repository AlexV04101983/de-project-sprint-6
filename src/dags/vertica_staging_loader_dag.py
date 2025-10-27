from datetime import datetime, timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

@dag(
    schedule_interval = None, #'0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date = pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup = False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint6', 'vertica',  'load_staging', 'users', 'groups', 'dialogs', 'group_log'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation = True  # Остановлен/запущен при появлении. Сразу запущен.
    )

def vertica_staging_loader():

    # Начальная фиктивная задача
    start_pipeline = EmptyOperator(task_id="begin_pipeline")
    # Основная задача для выполнения SQL-запроса в Vertica
    @task()
    def load_dialogs():
        hook = VerticaHook(vertica_conn_id='VERTICA_WAREHOUSE_CONNECTION')
        sql= """COPY VT2510213F5A0D__STAGING.dialogs(message_id, message_ts,message_from, message_to, message, message_group) 
                FROM LOCAL '/data/dialogs.csv' 
                DELIMITER ',';"""

        hook.run(sql)
    load_dialogs_task = load_dialogs()

    @task()
    def load_users():
        hook = VerticaHook(vertica_conn_id='VERTICA_WAREHOUSE_CONNECTION')
        sql = """COPY VT2510213F5A0D__STAGING.users(id, chat_name, registration_dt, country, age) 
                FROM LOCAL '/data/users.csv' 
                DELIMITER ',';"""        

        hook.run(sql)
    load_users_task = load_users()

    @task()
    def load_groups():
        hook = VerticaHook(vertica_conn_id='VERTICA_WAREHOUSE_CONNECTION')
        sql = """
            COPY VT2510213F5A0D__STAGING.groups(id, admin_id, group_name, registration_dt, is_private)
            FROM LOCAL '/data/groups.csv'
            DELIMITER ',';
        """
        hook.run(sql)
    load_groups_task = load_groups()

#   S6-Project
    @task
    def load_group_log():
        hook = VerticaHook(vertica_conn_id='VERTICA_WAREHOUSE_CONNECTION')
        sql = """
            COPY VT2510213F5A0D__STAGING.group_log(group_id, user_id, user_id_from, event, datetime)
            FROM LOCAL '/data/group_log.csv'
            DELIMITER ','
            REJECTED DATA AS TABLE VT2510213F5A0D__STAGING.group_log_rej
        """
        hook.run(sql)
    load_group_log_task = load_group_log()

    # Конечная фиктивная задача
    end_pipeline = EmptyOperator(task_id="end_pipeline")

    # Установка последовательности выполнения задач
    start_pipeline >> [load_dialogs_task, load_users_task, load_groups_task,load_group_log_task] >> end_pipeline

# Запускаем наш DAG
vertica_staging_loader_dag =vertica_staging_loader()