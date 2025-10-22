from logging import Logger
from typing import List, Optional
from decimal import Decimal

from lib import PgConnect
from psycopg import Connection
from pydantic import BaseModel
from datetime import datetime, date


class SettlementReportObj(BaseModel):
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: Decimal
    orders_bonus_payment_sum: Decimal
    orders_bonus_granted_sum: Decimal
    order_processing_fee: Decimal
    restaurant_reward_sum: Decimal


class SettlementReportLoader:
    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg

    def load_settlement_report(self):
        """Заполняем витрину данными - идемпотентно с ON CONFLICT"""
        with self.dwh.connection() as conn:
            with conn.cursor() as cur:
                # Один запрос для всего - идемпотентный и эффективный
                cur.execute(
                    """
                    INSERT INTO cdm.dm_settlement_report 
                    (restaurant_id, restaurant_name, settlement_date, orders_count, 
                     orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
                     order_processing_fee, restaurant_reward_sum)
                    WITH order_stats AS (
                        SELECT 
                            dr.id as restaurant_id,
                            dr.restaurant_name,
                            DATE(t.ts) as settlement_date,
                            COUNT(DISTINCT o.id) as orders_count,
                            SUM(ps.total_sum) as orders_total_sum,
                            SUM(ps.bonus_payment) as orders_bonus_payment_sum,
                            SUM(ps.bonus_grant) as orders_bonus_granted_sum,
                            SUM(ps.total_sum) * 0.25 as order_processing_fee,
                            SUM(ps.total_sum) - SUM(ps.bonus_payment) - (SUM(ps.total_sum) * 0.25) as restaurant_reward_sum
                        FROM dds.fct_product_sales ps
                        INNER JOIN dds.dm_orders o ON ps.order_id = o.id
                        INNER JOIN dds.dm_restaurants dr ON o.restaurant_id = dr.id
                        INNER JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                        WHERE o.order_status = 'CLOSED'
                        GROUP BY dr.id, dr.restaurant_name, DATE(t.ts)
                    )
                    SELECT 
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                    FROM order_stats
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
                    """
                )
                