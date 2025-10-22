from logging import Logger
from typing import List, Optional
import json

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
from psycopg.rows import class_row
from datetime import datetime, date, time

class TimestampsJsonObj(BaseModel): 
    id: int
    object_id: str
    object_value: str

class TimestampsDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: str
    date: date

class OrdersRawRepository:
    def load_orders(self, conn: Connection, last_loaded_record_id: int) -> List[TimestampsJsonObj]:
        with conn.cursor(row_factory=class_row(TimestampsJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value 
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s; 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class TimestampDdsRepository:
    def insert_timestamp(self, conn: Connection, timestamp: TimestampsDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps (ts,year ,month, day, time, date)  
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,  
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )

    def get_timestamp(self, conn: Connection, ts: datetime) -> Optional[TimestampsDdsObj]:
        with conn.cursor(row_factory=class_row(TimestampsDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id, id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(ts)s;
                """,
                {"ts": ts},
            )
            obj = cur.fetchone()
        return obj


class TimestampLoader:
    WF_KEY = "timestamps_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_timestamp_id"
    BATCH_LIMIT = 100  # Загружаем 100 записей за раз для получения нужного количества.

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = OrdersRawRepository()
        self.dds = TimestampDdsRepository()
        self.settings_repository = settings_repository
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_timestamps(self, raws: List[TimestampsJsonObj]) -> List[TimestampsDdsObj]:
        res = []
        for r in raws:
            order = json.loads(r.object_value)
            
            # Берем дату из поля date (дата получения финального статуса)
            if 'date' in order and order['date']:
                try:
                    # Парсим дату из строки
                    ts_datetime = datetime.strptime(order['date'], '%Y-%m-%d %H:%M:%S')
                    
                    # Создаем объект timestamp
                    timestamp = TimestampsDdsObj(
                        id=r.id,
                        ts=ts_datetime,
                        year=ts_datetime.year,
                        month=ts_datetime.month,
                        day=ts_datetime.day,
                        date=ts_datetime.date(),
                        # time=ts_datetime.time()
                        time=ts_datetime.time().strftime('%H:%M:%S')  # Преобразуем время в строку
                    )
                    res.append(timestamp)
                except (ValueError, TypeError) as e:
                    print(f"Error parsing date for order {r.object_id}: {e}")
                    continue
        
        return res

    def load_timestamps(self):
        # with self.settings_repository.connection() as conn:
        with self.dwh.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            load_queue = self.raw.load_orders(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)
            timestamps_to_load = self.parse_timestamps(load_queue)
            for u in timestamps_to_load:
                existing = self.dds.get_timestamp(conn, u.ts)
                if not existing:
                    self.dds.insert_timestamp(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))