from logging import Logger
from typing import List, Optional
from decimal import Decimal

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel
from datetime import datetime, date


class CourierLedgerObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: Decimal
    rate_avg: Decimal
    order_processing_fee: Decimal
    courier_order_sum: Decimal
    courier_tips_sum: Decimal
    courier_reward_sum: Decimal


class CourierLedgerLoader:
    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg

    def calculate_courier_order_sum(self, order_sum: Decimal, rate_avg: Decimal) -> Decimal:
        """Рассчитываем сумму выплаты курьеру по правилам"""
        if rate_avg < 4:
            percent = Decimal('0.05')
            min_amount = Decimal('100')
        elif 4 <= rate_avg < 4.5:
            percent = Decimal('0.07')
            min_amount = Decimal('150')
        elif 4.5 <= rate_avg < 4.9:
            percent = Decimal('0.08')
            min_amount = Decimal('175')
        else:  # rate_avg >= 4.9
            percent = Decimal('0.10')
            min_amount = Decimal('200')
        
        calculated = order_sum * percent
        return max(calculated, min_amount)

    def load_courier_ledger(self):
        """Заполняем витрину данными о выплатах курьерам"""
        with self.dwh.connection() as conn:
            with conn.cursor() as cur:
                # Очищаем витрину перед заполнением (или используем UPSERT)
                cur.execute("TRUNCATE TABLE cdm.dm_courier_ledger")
                
                # Основной запрос для заполнения витрины
                cur.execute("""
                    WITH courier_stats AS (
                        SELECT
                            c.id as courier_id,
                            c.courier_name,
                            EXTRACT(YEAR FROM t.ts)::INTEGER as settlement_year,
                            EXTRACT(MONTH FROM t.ts)::INTEGER as settlement_month,
                            COUNT(DISTINCT o.id) as orders_count,
                            SUM(ps.total_sum) as orders_total_sum,
                            AVG(d.rate)::NUMERIC(4,2) as rate_avg,
                            SUM(d.tip_sum) as courier_tips_sum
                        FROM dds.dm_orders o
                        JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                        JOIN dds.fct_product_sales ps ON o.id = ps.order_id
                        JOIN dds.dm_deliveries d ON o.id = d.order_id
                        JOIN dds.dm_couriers c ON o.courier_id = c.id
                        WHERE o.order_status = 'CLOSED'
                        GROUP BY c.id, c.courier_name, EXTRACT(YEAR FROM t.ts), EXTRACT(MONTH FROM t.ts)
                    )
                    INSERT INTO cdm.dm_courier_ledger 
                    (courier_id, courier_name, settlement_year, settlement_month, 
                     orders_count, orders_total_sum, rate_avg, order_processing_fee,
                     courier_order_sum, courier_tips_sum, courier_reward_sum)
                    SELECT
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        orders_total_sum * 0.25 as order_processing_fee,
                        orders_total_sum * 
                            CASE 
                                WHEN rate_avg < 4 THEN 0.05
                                WHEN rate_avg < 4.5 THEN 0.07
                                WHEN rate_avg < 4.9 THEN 0.08
                                ELSE 0.10
                            END as courier_order_sum_calculated,
                        courier_tips_sum,
                        (orders_total_sum * 
                            CASE 
                                WHEN rate_avg < 4 THEN 0.05
                                WHEN rate_avg < 4.5 THEN 0.07
                                WHEN rate_avg < 4.9 THEN 0.08
                                ELSE 0.10
                            END) + (courier_tips_sum * 0.95) as courier_reward_sum
                    FROM courier_stats
                """)
                
                # Обновляем минимальные суммы выплат
                cur.execute("""
                    UPDATE cdm.dm_courier_ledger
                    SET courier_order_sum = 
                        CASE 
                            WHEN rate_avg < 4 AND courier_order_sum < 100 THEN 100
                            WHEN rate_avg >= 4 AND rate_avg < 4.5 AND courier_order_sum < 150 THEN 150
                            WHEN rate_avg >= 4.5 AND rate_avg < 4.9 AND courier_order_sum < 175 THEN 175
                            WHEN rate_avg >= 4.9 AND courier_order_sum < 200 THEN 200
                            ELSE courier_order_sum
                        END,
                    courier_reward_sum = 
                        (CASE 
                            WHEN rate_avg < 4 AND courier_order_sum < 100 THEN 100
                            WHEN rate_avg >= 4 AND rate_avg < 4.5 AND courier_order_sum < 150 THEN 150
                            WHEN rate_avg >= 4.5 AND rate_avg < 4.9 AND courier_order_sum < 175 THEN 175
                            WHEN rate_avg >= 4.9 AND courier_order_sum < 200 THEN 200
                            ELSE courier_order_sum
                        END) + (courier_tips_sum * 0.95)
                """)
                
                print("Courier ledger loaded successfully")