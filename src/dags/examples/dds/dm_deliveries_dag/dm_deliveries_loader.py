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
from typing import Union, Any
import json

class DeliveryJsonObj(BaseModel): 
    id: int
    object_id: str
    object_value: Any
    update_ts: datetime
        
    class Config:
        arbitrary_types_allowed = True

class DeliveryDdsObj(BaseModel):
    id: int
    delivery_id: str
    order_id: int
    courier_id: int
    address: str
    delivery_ts: datetime
    rate: Optional[int] = None
    tip_sum: float

class DeliveriesRawRepository:
    def load_deliveries(self, conn: Connection, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value, update_ts
                    FROM stg.deliversystem_deliveries
                    WHERE id > %(last_loaded_record_id)s; 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs

class DeliveriesDdsRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries 
                    (delivery_id, order_id, courier_id, address, delivery_ts, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(order_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "order_id": delivery.order_id,
                    "courier_id": delivery.courier_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryDdsObj]:
        with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, delivery_id, order_id, courier_id, address, delivery_ts, rate, tip_sum
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj


class DeliveriesLoader:
    WF_KEY = "deliveries_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_delivery_id"
    BATCH_LIMIT = 100

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = DeliveriesRawRepository()
        self.dds = DeliveriesDdsRepository()
        self.settings_repository = settings_repository

    def get_order_id(self, conn: Connection, order_key: str) -> Optional[int]:
        """Получаем ID заказа из dm_orders по order_key"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_orders 
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_courier_id(self, conn: Connection, courier_key: str) -> Optional[int]:
        """Получаем ID курьера из dm_couriers по courier_id"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_couriers 
                    WHERE courier_id = %(courier_key)s;
                """,
                {"courier_key": courier_key},
            )
            result = cur.fetchone()
            return result[0] if result else None

    def parse_deliveries(self, raws: List[DeliveryJsonObj], conn: Connection) -> List[DeliveryDdsObj]:
        res = []
        for r in raws:
            # object_value уже является словарем, не нужно json.loads()
            delivery_data = r.object_value  # УБИРАЕМ json.loads()
            
            # Получаем основные данные доставки
            delivery_id = delivery_data.get('delivery_id')
            order_key = delivery_data.get('order_id')
            courier_key = delivery_data.get('courier_id')
            address = delivery_data.get('address')
            delivery_ts_str = delivery_data.get('delivery_ts')
            
            if not all([delivery_id, order_key, courier_key, address, delivery_ts_str]):
                continue
            
            # Получаем ID заказа
            order_id = self.get_order_id(conn, order_key)
            if not order_id:
                print(f"Skipping delivery {delivery_id} - order not found: {order_key}")
                continue
            
            # Получаем ID курьера
            courier_id = self.get_courier_id(conn, courier_key)
            if not courier_id:
                print(f"Skipping delivery {delivery_id} - courier not found: {courier_key}")
                continue
            
            # Парсим дату и время
            try:
                delivery_ts = datetime.strptime(delivery_ts_str, '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                try:
                    delivery_ts = datetime.strptime(delivery_ts_str, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    print(f"Skipping delivery {delivery_id} - invalid delivery_ts: {delivery_ts_str}")
                    continue
            
            # Получаем опциональные поля
            rate = delivery_data.get('rate')
            tip_sum = delivery_data.get('tip_sum', 0)
            
            # Создаем объект доставки
            delivery = DeliveryDdsObj(
                id=0,
                delivery_id=delivery_id,
                order_id=order_id,
                courier_id=courier_id,
                address=address,
                delivery_ts=delivery_ts,
                rate=rate,
                tip_sum=tip_sum
            )
            res.append(delivery)
        
        return res

    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            load_queue = self.raw.load_deliveries(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)
            deliveries_to_load = self.parse_deliveries(load_queue, conn)
            
            for delivery in deliveries_to_load:
                existing = self.dds.get_delivery(conn, delivery.delivery_id)
                if not existing:
                    self.dds.insert_delivery(conn, delivery)
                
                last_loaded = delivery.id
            
            # ИСПРАВЛЕННЫЙ ВЫЗОВ save_setting
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))
            
            print(f"Loaded {len(deliveries_to_load)} deliveries")