CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id SERIAL PRIMARY KEY,
    courier_id INTEGER NOT NULL,
    courier_name VARCHAR NOT NULL,
    settlement_year INTEGER NOT NULL,
    settlement_month INTEGER NOT NULL,
    orders_count INTEGER NOT NULL,
    orders_total_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    rate_avg NUMERIC(4, 2) NOT NULL DEFAULT 0,
    order_processing_fee NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_order_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_tips_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    courier_reward_sum NUMERIC(14, 2) NOT NULL DEFAULT 0,
    
    -- Constraints для валидации данных
    CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (settlement_year >= 2022 AND settlement_year < 2500),
    CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (settlement_month >= 1 AND settlement_month <= 12),
    CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0),
    CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum >= 0),
    CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (rate_avg >= 0 AND rate_avg <= 5),
    CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee >= 0),
    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK (courier_order_sum >= 0),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK (courier_tips_sum >= 0),
    CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK (courier_reward_sum >= 0),
        -- Уникальность по курьеру и периоду
    CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);

COMMENT ON TABLE cdm.dm_courier_ledger IS 'Витрина для расчета вознаграждения курьеров';
COMMENT ON COLUMN cdm.dm_courier_ledger.id IS 'Идентификатор записи';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_id IS 'ID курьера, которому перечисляем';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_name IS 'Ф. И. О. курьера';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_year IS 'Год отчёта';
COMMENT ON COLUMN cdm.dm_courier_ledger.settlement_month IS 'Месяц отчёта, где 1 — январь и 12 — декабрь';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_count IS 'Количество заказов за период (месяц)';
COMMENT ON COLUMN cdm.dm_courier_ledger.orders_total_sum IS 'Общая стоимость заказов';
COMMENT ON COLUMN cdm.dm_courier_ledger.rate_avg IS 'Средний рейтинг курьера по оценкам пользователей';
COMMENT ON COLUMN cdm.dm_courier_ledger.order_processing_fee IS 'Сумма, удержанная компанией за обработку заказов (orders_total_sum * 0.25)';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_order_sum IS 'Сумма, которую необходимо перечислить курьеру за доставленные заказы';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_tips_sum IS 'Сумма чаевых, которые пользователи оставили курьеру';
COMMENT ON COLUMN cdm.dm_courier_ledger.courier_reward_sum IS 'Итоговая сумма к перечислению курьеру (courier_order_sum + courier_tips_sum * 0.95)';