CREATE TABLE IF NOT EXISTS dds.dm_couriers (
    id SERIAL PRIMARY KEY,
    courier_id VARCHAR NOT NULL UNIQUE,  -- ID из системы доставки
    courier_name VARCHAR NOT NULL
);

COMMENT ON TABLE dds.dm_couriers IS 'Версионная таблица курьеров';
COMMENT ON COLUMN dds.dm_couriers.courier_id IS 'ID курьера из системы доставки';
COMMENT ON COLUMN dds.dm_couriers.courier_name IS 'ФИО курьера';