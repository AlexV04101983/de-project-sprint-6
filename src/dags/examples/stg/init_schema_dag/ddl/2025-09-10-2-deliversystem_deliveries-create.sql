CREATE TABLE IF NOT EXISTS stg.deliversystem_deliveries (
    id SERIAL PRIMARY KEY,
    object_id VARCHAR NOT NULL,          -- delivery_id из JSON
    object_value JSONB NOT NULL,         -- Весь JSON объект
    update_ts TIMESTAMP NOT NULL,        -- Время обновления записи (delivery_ts из JSON)
    load_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT deliversystem_deliveries_object_id_unique UNIQUE (object_id)
);