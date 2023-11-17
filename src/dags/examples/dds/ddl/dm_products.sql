CREATE TABLE IF NOT EXISTS dds.dm_products (
	id serial NOT NULL,
	restaurant_id int NOT NULL,
	product_id varchar NOT NULL,
	product_name varchar NOT NULL,
	product_price numeric(14, 2) NOT NULL DEFAULT 0,
	active_from timestamp NOT NULL,
	active_to timestamp NOT NULL,
	CONSTRAINT dm_products_pkey PRIMARY KEY (id),
	CONSTRAINT dm_products_price_check CHECK ((product_price >= (0)::numeric)),
	CONSTRAINT dm_products_restaurant_id_fk FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id)
);