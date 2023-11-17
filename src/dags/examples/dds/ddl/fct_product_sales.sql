CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
	id serial NOT NULL,
	product_id int NOT NULL,
	order_id int NOT NULL,
	count int NOT NULL DEFAULT 0,
	price numeric(14, 2) NOT NULL DEFAULT 0,
	total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_payment numeric(14, 2) NOT NULL DEFAULT 0,
	bonus_grant numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT fct_product_sales_bonus_grant_check CHECK ((bonus_grant >= (0)::numeric)),
	CONSTRAINT fct_product_sales_bonus_payment_check CHECK ((bonus_payment >= (0)::numeric)),
	CONSTRAINT fct_product_sales_count_check CHECK ((count >= 0)),
	CONSTRAINT fct_product_sales_pkey PRIMARY KEY (id),
	CONSTRAINT fct_product_sales_price_check CHECK ((price >= (0)::numeric)),
	CONSTRAINT fct_product_sales_total_sum_check CHECK ((total_sum >= (0)::numeric)),
	CONSTRAINT order_id_product_id_unique UNIQUE (order_id, product_id),
	CONSTRAINT fct_product_sales_order_id_fk FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_product_sales_product_id_fk FOREIGN KEY (product_id) REFERENCES dds.dm_products(id)
);