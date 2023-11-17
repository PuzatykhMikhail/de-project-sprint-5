CREATE TABLE IF NOT EXISTS dds.dm_couriers(
	id serial,
	courier_id varchar NOT null,
	name varchar NOT null,
    CONSTRAINT id_pk PRIMARY KEY (id),
    CONSTRAINT courier_id_unique UNIQUE (courier_id)
);