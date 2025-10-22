CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL
);