CREATE TABLE IF NOT EXISTS dds.dm_deliveries (
    id SERIAL PRIMARY KEY,
    delivery_id VARCHAR NOT NULL UNIQUE,  -- ID из системы доставки
    order_id INTEGER NOT NULL REFERENCES dds.dm_orders(id),
    courier_id INTEGER NOT NULL REFERENCES dds.dm_couriers(id),
    address TEXT NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    rate INTEGER CHECK (rate >= 1 AND rate <= 5),
    tip_sum NUMERIC(14, 2) NOT NULL DEFAULT 0
);
COMMENT ON TABLE dds.dm_deliveries IS 'Таблица доставок';
COMMENT ON COLUMN dds.dm_deliveries.delivery_id IS 'ID доставки из системы доставки';
COMMENT ON COLUMN dds.dm_deliveries.order_id IS 'Ссылка на заказ';
COMMENT ON COLUMN dds.dm_deliveries.courier_id IS 'Ссылка на курьера';
COMMENT ON COLUMN dds.dm_deliveries.address IS 'Адрес доставки';
COMMENT ON COLUMN dds.dm_deliveries.delivery_ts IS 'Время доставки';
COMMENT ON COLUMN dds.dm_deliveries.rate IS 'Рейтинг доставки (1-5)';
COMMENT ON COLUMN dds.dm_deliveries.tip_sum IS 'Сумма чаевых';