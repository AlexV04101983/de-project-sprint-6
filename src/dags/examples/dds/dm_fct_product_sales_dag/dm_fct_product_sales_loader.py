from logging import Logger
from typing import List, Optional, Dict, Any
import json

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
from psycopg.rows import class_row
from datetime import datetime
from decimal import Decimal


class BonusEventJsonObj(BaseModel): 
    id: int
    event_ts: datetime
    event_type: str
    event_value: str

class OrderItemJsonObj(BaseModel):
    id: str
    name: str
    price: float
    quantity: int

class ProductSaleDdsObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: Decimal
    total_sum: Decimal
    bonus_payment: Decimal
    bonus_grant: Decimal

class BonusEventsRawRepository:
    def load_bonus_events(self, conn: Connection, last_loaded_record_id: int) -> List[BonusEventJsonObj]:
        with conn.cursor(row_factory=class_row(BonusEventJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value 
                    FROM stg.bonussystem_events
                    WHERE id > %(last_loaded_record_id)s 
                    AND event_type = 'bonus_transaction';
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs

class OrdersRawRepository:
    def load_orders(self, conn: Connection) -> List[Dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT object_value 
                    FROM stg.ordersystem_orders;
                """
            )
            objs = cur.fetchall()
            return [str2json(obj[0]) for obj in objs]

class ProductSalesDdsRepository:
    def insert_product_sale(self, conn: Connection, sale: ProductSaleDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales 
                    (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant
                """,
                {
                    "product_id": sale.product_id,
                    "order_id": sale.order_id,
                    "count": sale.count,
                    "price": sale.price,
                    "total_sum": sale.total_sum,
                    "bonus_payment": sale.bonus_payment,
                    "bonus_grant": sale.bonus_grant
                },
            )

    def get_product_sale(self, conn: Connection, product_id: int, order_id: int) -> Optional[ProductSaleDdsObj]:
        with conn.cursor(row_factory=class_row(ProductSaleDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant
                    FROM dds.fct_product_sales
                    WHERE product_id = %(product_id)s AND order_id = %(order_id)s;
                """,
                {"product_id": product_id, "order_id": order_id},
            )
            obj = cur.fetchone()
        return obj

class ProductSalesLoader:
    WF_KEY = "product_sales_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_product_sale_id"
    BATCH_LIMIT = 100

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.bonus_events_raw = BonusEventsRawRepository()
        self.orders_raw = OrdersRawRepository()
        self.dds = ProductSalesDdsRepository()
        self.settings_repository = settings_repository

    def get_product_id(self, conn: Connection, product_json_id: str, order_ts: datetime) -> Optional[int]:
        """Получаем ID продукта из dm_products по JSON ID и дате заказа"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_products 
                    WHERE product_id = %(product_json_id)s 
                    AND active_from <= %(order_ts)s 
                    AND active_to > %(order_ts)s;
                """,
                {"product_json_id": product_json_id, "order_ts": order_ts},
            )
            result = cur.fetchone()
            return result[0] if result else None

    def get_order_id(self, conn: Connection, order_json_id: str) -> Optional[int]:
        """Получаем ID заказа из dm_orders по JSON ID"""
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id 
                    FROM dds.dm_orders 
                    WHERE order_key = %(order_json_id)s;
                """,
                {"order_json_id": order_json_id},
            )
            result = cur.fetchone()
            return result[0] if result else None

    def parse_bonus_transactions(self, events: List[BonusEventJsonObj]) -> Dict[str, Dict[str, Decimal]]:
        """Парсим бонусные транзакции из новой структуры"""
        bonus_data = {}
        
        for event in events:
            if event.event_type == 'bonus_transaction':
                try:
                    transaction = json.loads(event.event_value)
                    order_id = transaction.get('order_id')
                    
                    # Обрабатываем массив product_payments
                    product_payments = transaction.get('product_payments', [])
                    for payment in product_payments:
                        product_id = payment.get('product_id')
                        bonus_payment = Decimal(payment.get('bonus_payment', 0))
                        bonus_grant = Decimal(payment.get('bonus_grant', 0))
                        
                        if order_id and product_id:
                            key = f"{order_id}_{product_id}"
                            if key not in bonus_data:
                                bonus_data[key] = {'payment': Decimal(0), 'grant': Decimal(0)}
                            
                            bonus_data[key]['payment'] += bonus_payment
                            bonus_data[key]['grant'] += bonus_grant
                            
                except (json.JSONDecodeError, ValueError, TypeError) as e:
                    print(f"Error parsing bonus transaction: {e}")
                    continue
        
        return bonus_data

    def parse_product_sales(self, conn: Connection) -> List[ProductSaleDdsObj]:
        """Парсим продажи продуктов из заказов и обогащаем бонусными данными"""
        res = []
        
        # Загружаем бонусные транзакции
        bonus_events = self.bonus_events_raw.load_bonus_events(conn, -1)
        bonus_data = self.parse_bonus_transactions(bonus_events)
        
        # Загружаем заказы
        orders = self.orders_raw.load_orders(conn)
        
        for order in orders:
            order_json_id = order.get('_id')
            order_ts_str = order.get('date')
            
            if not order_json_id or not order_ts_str:
                continue
            
            try:
                order_ts = datetime.strptime(order_ts_str, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
            
            # Получаем ID заказа
            order_id = self.get_order_id(conn, order_json_id)
            if not order_id:
                continue
            
            # Обрабатываем продукты в заказе
            order_items = order.get('order_items', [])
            for item in order_items:
                product_json_id = item.get('id')
                quantity = item.get('quantity', 0)
                price = Decimal(item.get('price', 0))
                
                if not product_json_id or quantity <= 0:
                    continue
                
                # Получаем ID продукта
                product_id = self.get_product_id(conn, product_json_id, order_ts)
                if not product_id:
                    continue
                
                # Получаем бонусные данные
                bonus_key = f"{order_json_id}_{product_json_id}"
                bonus_info = bonus_data.get(bonus_key, {'payment': Decimal(0), 'grant': Decimal(0)})

                # ФИЛЬТР: Пропускаем записи где оба бонуса = 0
                if bonus_info['payment'] == 0 and bonus_info['grant'] == 0:
                    print(f"Пропускаем продукт {product_json_id} в заказе {order_json_id} - нет бонусных данных")
                    continue
                
                # Создаем объект продажи
                sale = ProductSaleDdsObj(
                    id=0,
                    product_id=product_id,
                    order_id=order_id,
                    count=quantity,
                    price=price,
                    total_sum=price * quantity,
                    bonus_payment=bonus_info['payment'],
                    bonus_grant=bonus_info['grant']
                )
                res.append(sale)
        
        return res

    def load_product_sales(self):
        with self.dwh.connection() as conn:
            # Читаем состояние загрузки
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            # Парсим и загружаем продажи
            sales_to_load = self.parse_product_sales(conn)
            
            # Вставляем продажи
            for sale in sales_to_load:
                existing = self.dds.get_product_sale(conn, sale.product_id, sale.order_id)
                if not existing:
                    self.dds.insert_product_sale(conn, sale)
                
                last_loaded = sale.id
            
            # Сохраняем прогресс - ТАК ЖЕ КАК В РАБОЧЕМ ЛОАДЕРЕ
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded
            self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))
            
            print(f"Loaded {len(sales_to_load)} product sales. Last loaded ID: {last_loaded}")