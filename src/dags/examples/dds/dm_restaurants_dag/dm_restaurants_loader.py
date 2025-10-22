from logging import Logger
from typing import List, Optional
import json

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
from psycopg.rows import class_row
from datetime import datetime


class RestaurantJsonObj(BaseModel): 
    id: int
    object_id: str
    object_value: str

class RestaurantDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

class RestaurantRawRepository:
    def load_restaurants(self, conn: Connection, last_loaded_record_id: int) -> List[RestaurantJsonObj]:
        with conn.cursor(row_factory=class_row(RestaurantJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value 
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s; 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class RestaurantDdsRepository:
    def insert_restaurant(self, conn: Connection, restaurant: RestaurantDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,  
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.active_from,
                    "active_to": restaurant.active_to
                },
            )

    def get_restaurant(self, conn: Connection, restaurant_id: str) -> Optional[RestaurantDdsObj]:
        with conn.cursor(row_factory=class_row(RestaurantDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id, restaurant_id, restaurant_name, active_from, active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = %(restaurant_id)s;
                """,
                {"restaurant_id": restaurant_id},
            )
            obj = cur.fetchone()
        return obj


class RestaurantLoader:
    WF_KEY = "restaurants_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_restaurant_id"
    BATCH_LIMIT = 100  # Загружаем 100 записей за раз для получения нужного количества.

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = RestaurantRawRepository()
        self.dds = RestaurantDdsRepository()
        self.settings_repository = settings_repository
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_restaurants(self, raws: List[RestaurantJsonObj]) -> List[RestaurantDdsObj]:
        res = []
        for r in raws:
            restaurant = json.loads(r.object_value)
            t = RestaurantDdsObj(
                           id = r.id,
                           restaurant_id = restaurant['_id'],
                           restaurant_name = restaurant['name'], 
                           active_from = restaurant['update_ts'],
                           active_to=datetime(2099, 12, 31, 0, 0, 0)  
                           )
            res.append(t)
        return res

    def load_restaurants(self):
        # with self.settings_repository.connection() as conn:
        with self.dwh.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            load_queue = self.raw.load_restaurants(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)
            restaurants_to_load = self.parse_restaurants(load_queue)
            for u in restaurants_to_load:
                existing = self.dds.get_restaurant(conn, u.restaurant_id)
                if not existing:
                    self.dds.insert_restaurant(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                # self.settings_repository.save_setting(conn, wf_setting.workflow_settings)
                # self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))
                