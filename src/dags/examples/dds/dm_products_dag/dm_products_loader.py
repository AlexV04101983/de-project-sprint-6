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

class ProductsJsonObj(BaseModel): 
    id: int
    object_id: str
    object_value: str

class ProductsDdsObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int


class OrdersRawRepository:
    def load_orders(self, conn: Connection, last_loaded_record_id: int) -> List[ProductsJsonObj]:
        with conn.cursor(row_factory=class_row(ProductsJsonObj)) as cur:
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


class ProductDdsRepository:
    def insert_product(self, conn: Connection, product: ProductsDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)  
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,  
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_to
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to
                },
            )

    def get_product(self, conn: Connection, product_id: datetime) -> Optional[ProductsDdsObj]:
        with conn.cursor(row_factory=class_row(ProductsDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id, restaurant_id, product_id, product_name, product_price, active_from, active_to
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
        return obj
    


class ProductLoader:
    WF_KEY = "products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_product_id"
    BATCH_LIMIT = 100  # Загружаем 100 записей за раз для получения нужного количества.

    def get_restaurant_id_by_json_id(self, conn: Connection, restaurant_json_id: str) -> Optional[int]:
        """Получаем integer ID ресторана из dm_restaurants по его JSON ID"""
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

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = OrdersRawRepository()
        self.dds = ProductDdsRepository()
        self.settings_repository = settings_repository
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_products(self, raws: List[ProductsJsonObj], conn: Connection) -> List[ProductsDdsObj]:
        res = []
        for r in raws:
            order = json.loads(r.object_value)
            
            # Получаем restaurant_id из JSON (это строка)
            restaurant_json_id = order.get('restaurant', {}).get('id')
            if not restaurant_json_id:
                continue
                
            # Получаем настоящий restaurant_id (integer) из dm_restaurants
            restaurant_dds_id = self.get_restaurant_id_by_json_id(conn, restaurant_json_id)
            if not restaurant_dds_id:
                continue  # Пропускаем если ресторан не найден
                
            update_ts = order.get('update_ts')
            if isinstance(update_ts, str):
                try:
                    active_from = datetime.strptime(update_ts, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    active_from = datetime.now()
            else:
                active_from = update_ts or datetime.now()
            
            # Обрабатываем каждый продукт в order_items
            order_items = order.get('order_items', [])
            for item in order_items:
                product_id = item.get('id')
                product_name = item.get('name')
                product_price = item.get('price')  # Добавляем цену
                
                if product_id and product_name and product_price is not None:
                    t = ProductsDdsObj(
                        id=0,  # Автогенерируемое значение, можно поставить 0
                        restaurant_id=restaurant_dds_id,  # Теперь integer ID из dm_restaurants
                        product_id=product_id,
                        product_name=product_name,
                        product_price=product_price,  # Добавляем цену
                        active_from=active_from,
                        active_to=datetime(2099, 12, 31, 0, 0, 0)  
                    )
                    res.append(t)
        
        return res

    def load_products(self):
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
            products_to_load = self.parse_products(load_queue, conn)
            for u in products_to_load:
                existing = self.dds.get_product(conn, u.product_id)
                if not existing:
                    self.dds.insert_product(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))