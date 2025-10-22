from logging import Logger
from typing import List, Optional
import json

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
from psycopg.rows import class_row


class CourierJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str

class CourierDdsObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CourierRawRepository:
    def load_couriers(self, conn: Connection, last_loaded_record_id: int) -> List[CourierJsonObj]:
        with conn.cursor(row_factory=class_row(CourierJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value 
                    FROM stg.deliversystem_couriers
                    WHERE id > %(last_loaded_record_id)s; 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class CourierDdsRepository:
    def insert_courier(self, conn: Connection, courier: CourierDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers (courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,  
                        courier_name = EXCLUDED.courier_name
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name
                },
            )

    def get_courier(self, conn: Connection, courier_id: str) -> Optional[CourierDdsObj]:
        with conn.cursor(row_factory=class_row(CourierDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id, courier_id, courier_name
                    FROM dds.dm_couriers
                    WHERE courier_id = %(courier_id)s;
                """,
                {"courier_id": courier_id},
            )
            obj = cur.fetchone()
        return obj


class CourierLoader:
    WF_KEY = "couriers_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_courier_id"
    BATCH_LIMIT = 100  # Загружаем 100 записей за раз для получения нужного количества.

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = CourierRawRepository()
        self.dds = CourierDdsRepository()
        self.settings_repository = settings_repository
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_couriers(self, raws: List[CourierJsonObj]) -> List[CourierDdsObj]:
        res = []
        for r in raws:
            courier = json.loads(r.object_value)
            t = CourierDdsObj(id = r.id,
                           courier_id = courier['_id'],
                           courier_name = courier['name']
                           )
            res.append(t)
        return res

    def load_couriers(self):
        # with self.settings_repository.connection() as conn:
        with self.dwh.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            load_queue = self.raw.load_couriers(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)
            couriers_to_load = self.parse_couriers(load_queue)
            for u in couriers_to_load:
                existing = self.dds.get_courier(conn, u.courier_id)
                if not existing:
                    self.dds.insert_courier(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                # self.settings_repository.save_setting(conn, wf_setting.workflow_settings)
                # self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))
                