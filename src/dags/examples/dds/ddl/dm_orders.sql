CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id serial NOT NULL,
	user_id int NOT NULL,
	restaurant_id int NOT NULL,
	timestamp_id int NOT NULL,
	order_key varchar NOT NULL,
	order_status varchar NOT NULL,
	CONSTRAINT dm_orders_pkey PRIMARY KEY (id),
	CONSTRAINT order_key_unique UNIQUE (order_key),
	CONSTRAINT dm_orders_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_id_fk FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id),
    CONSTRAINT dm_orders_user_id_fk FOREIGN KEY (user_id) REFERENCES dds.dm_users(id)
);