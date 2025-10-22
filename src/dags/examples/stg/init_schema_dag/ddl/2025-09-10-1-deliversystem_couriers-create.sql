CREATE TABLE IF NOT EXISTS stg.deliversystem_couriers (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
	object_id varchar NOT NULL,
	object_value text NOT NULL
);