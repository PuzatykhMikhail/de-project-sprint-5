CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger(
	id	serial,
	courier_id integer,
	courier_name varchar,
	settlement_year integer not null,
    settlement_month integer not null,
	orders_count integer,
	orders_total_sum numeric(14, 2),
	rate_avg numeric,
	order_processing_fee numeric,
	courier_order_sum numeric(14, 2),
	courier_tips_sum numeric(14, 2),
	courier_reward_sum numeric(14, 2),
    CONSTRAINT dm_courier_ledger_pk primary key (id),
	CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);