   CREATE TABLE IF NOT EXISTS stg.deliveries(
    id serial NOT NULL,
    object_id varchar NOT NULL,
    object_value varchar NOT NULL,
    CONSTRAINT deliveries_pk PRIMARY KEY (id),
    CONSTRAINT deliveries_object_id_uique UNIQUE (object_id)
 );