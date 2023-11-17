CREATE TABLE IF NOT EXISTS dds.dm_deliveries(
    id serial,
	delivery_id	varchar NOT NULL,
	courier_id	integer NOT NULL,
	order_id integer NOT NULL,
	order_ts timestamp NOT NULL,
	delivery_ts timestamp NOT NULL,
	address varchar NOT NULL,
	rate  integer NOT NULL,
	tip_sum numeric (14, 2) NOT NULL,
	total_sum numeric (14, 2) NOT NULL,
    CONSTRAINT dm_deliveries_id_pk PRIMARY KEY (id),
    CONSTRAINT dm_deliveries_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT dm_deliveries_courier_id_fk FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);