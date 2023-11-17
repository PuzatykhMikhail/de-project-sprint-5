CREATE TABLE IF NOT EXISTS stg.couriers(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT couriers_pk PRIMARY KEY (id),
    CONSTRAINT couriers_object_id_unique UNIQUE (object_id)
);