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

class OrdersJsonObj(BaseModel): 
    id: int
    object_id: str
    object_value: str

class OrdersDdsObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    courier_id: Optional[int] = None  # Добавляем опциональное поле

class OrdersRawRepository:
    def load_orders(self, conn: Connection, last_loaded_record_id: int) -> List[OrdersJsonObj]:
        with conn.cursor(row_factory=class_row(OrdersJsonObj)) as cur:
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

class OrdersDdsRepository:
    def insert_order(self, conn: Connection, order: OrdersDdsObj) -> None: 
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders (order_key, order_status, user_id, restaurant_id, timestamp_id, courier_id) 
                    VALUES (%(order_key)s, %(order_status)s, %(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(courier_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_key = EXCLUDED.order_key,  
                        order_status = EXCLUDED.order_status,
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        courier_id = EXCLUDED.courier_id
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "courier_id": order.courier_id
                },
            )

    def get_order(self, conn: Connection, order_key: str) -> Optional[OrdersDdsObj]:
        with conn.cursor(row_factory=class_row(OrdersDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id, order_key, order_status, user_id, restaurant_id, timestamp_id, courier_id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_key)s;
                """,
                {"order_key": order_key},
            )
            obj = cur.fetchone()
        return obj
    
class OrderLoader:
    WF_KEY = "orders_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"
    BATCH_LIMIT = 100

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = OrdersRawRepository()
        self.dds = OrdersDdsRepository()
        self.settings_repository = settings_repository

    def get_restaurant_id(self, conn: Connection, restaurant_json_id: str) -> Optional[int]:
        """Получаем ID ресторана из dm_restaurants по JSON ID"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_restaurants 
                    WHERE restaurant_id = %(restaurant_json_id)s;
                """,
                {"restaurant_json_id": restaurant_json_id},
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_timestamp_id(self, conn: Connection, order_ts: datetime) -> Optional[int]:
        """Получаем ID timestamp из dm_timestamps по дате заказа"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_timestamps 
                    WHERE ts = %(order_ts)s;
                """,
                {"order_ts": order_ts},
            )
            result = cur.fetchone()
            return result[0] if result else None

        
    def get_user_id(self, conn: Connection, user_json_id: str) -> Optional[int]:
        """Получаем ID пользователя из dm_users по JSON ID"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_users 
                    WHERE user_id = %(user_json_id)s;
                """,
                {"user_json_id": user_json_id},
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_courier_id(self, conn: Connection, courier_json_id: str) -> Optional[int]: 
        """Получаем ID курьера из dm_couriers по JSON ID"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_couriers 
                    WHERE courier_id = %(courier_json_id)s;
                """,
                {"courier_json_id": courier_json_id},
            )
            result = cur.fetchone()
            return result[0] if result else None
        


    def parse_orders(self, raws: List[OrdersJsonObj], conn: Connection) -> List[OrdersDdsObj]:
        res = []
        for r in raws:
            order_data = json.loads(r.object_value)
            
            # Получаем основные данные заказа
            order_key = order_data.get('_id')
            order_status = order_data.get('final_status')
            
            if not order_key or not order_status:
                continue
            
            # Получаем ID ресторана
            restaurant_json_id = order_data.get('restaurant', {}).get('id')
            restaurant_id = self.get_restaurant_id(conn, restaurant_json_id) if restaurant_json_id else None
            
            # Получаем ID timestamp
            order_date_str = order_data.get('date')
            timestamp_id = None
            if order_date_str:
                try:
                    order_ts = datetime.strptime(order_date_str, '%Y-%m-%d %H:%M:%S')
                    timestamp_id = self.get_timestamp_id(conn, order_ts)
                except ValueError:
                    pass
            
            # Получаем ID пользователя
            user_json_id = order_data.get('user', {}).get('id')
            user_id = self.get_user_id(conn, user_json_id) if user_json_id else None

            # Получаем ID курьера
            courier_json_id = order_data.get('courier', {}).get('id')
            courier_id = self.get_user_id(conn, courier_json_id) if courier_json_id else None
            
            # Пропускаем если нет всех внешних ключей
            if not all([restaurant_id, timestamp_id, user_id]):
                print(f"Skipping order {order_key} - missing foreign keys")
                continue
            
            # Создаем объект заказа
            order = OrdersDdsObj(
                id=0,  # Автогенерируется
                order_key=order_key,
                order_status=order_status,
                restaurant_id=restaurant_id,
                timestamp_id=timestamp_id,
                user_id=user_id,
                courier_id=courier_id
            )
            res.append(order)
        
        return res

    def load_orders(self):
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
            orders_to_load = self.parse_orders(load_queue, conn)
            for u in orders_to_load:
                existing = self.dds.get_order(conn, u.order_key)
                if not existing:
                    self.dds.insert_order(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))