1. id — идентификатор записи.
courier_id — ID курьера, которому перечисляем.
courier_name — Ф. И. О. курьера.
settlement_year — год отчёта.
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
orders_count — количество заказов за период (месяц).
orders_total_sum — общая стоимость заказов.
rate_avg — средний рейтинг курьера по оценкам пользователей.
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).
2. dm_order, dm_delivery, dm_courier, dm_timestamp
3. методы - couriers,deliveries.