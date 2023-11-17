from lib import PgConnect
from logging import Logger


class BaseRepository:
    def __init__(self, pg: PgConnect, sql_update:str) -> None:
        self._sql_update = sql_update
        self._db = pg

    def load_courier_ledger(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(self._sql_update)

class CourierLedgerLoad:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
	sql_rep  = '''

					INSERT INTO cdm.dm_courier_ledger (
       						 courier_id, 
        					 courier_name, 
        					 settlement_year, 
        					 settlement_month, 
        					 orders_count, 
        				 	 orders_total_sum, 
        					 rate_avg, 
        					 order_processing_fee, 
        					 courier_order_sum, 
        					 courier_tips_sum, 
        					 courier_reward_sum
						)
					WITH rate AS (
						SELECT courier_id, order_id,  AVG(rate) over(PARTITION BY courier_id) avg_rate
						FROM dds.dm_deliveries
						),

					tmp AS (
							SELECT
								fct.courier_id,
								dc."name" AS courier_name,
								EXTRACT('YEAR' FROM fct.order_ts) AS settlement_year,
								EXTRACT('MONTH' FROM fct.order_ts) AS settlement_month,
								COUNT(fct.order_id) AS orders_count,
								SUM(fct.total_sum) AS orders_total_sum,
								AVG(fct.rate) AS rate_avg,
								SUM(fct.total_sum) * 0.25 AS order_processing_fee,
								sum(CASE			    
			        						WHEN rate.avg_rate < 4 AND rate.avg_rate * 0.05 <=  100 THEN 100
			       							WHEN rate.avg_rate < 4 AND rate.avg_rate * 0.05 >  100 THEN fct.total_sum * 0.05
			        						WHEN rate.avg_rate >= 4 AND rate.avg_rate < 4.5 AND fct.total_sum * 0.07 <=  150 THEN 150 
			        						WHEN rate.avg_rate >= 4 AND rate.avg_rate < 4.5 AND fct.total_sum * 0.07 >  150 THEN fct.total_sum * 0.07
			        						WHEN rate.avg_rate >= 4.5 AND rate.avg_rate < 4.9 AND fct.total_sum * 0.07 <=  175 THEN 175 
			        						WHEN rate.avg_rate >= 4.5 AND rate.avg_rate < 4.9 AND fct.total_sum * 0.07 >  175 THEN fct.total_sum * 0.08
			        						WHEN rate.avg_rate >= 4.9 AND fct.total_sum * 0.1 <=  200 THEN 200 
			        						WHEN rate.avg_rate >= 4.9 AND fct.total_sum * 0.1 >  200 THEN fct.total_sum * 0.1
			    						END) AS courier_order_sum,
								SUM(fct.tip_sum) AS courier_tips_sum
							FROM 		de.dds.dm_deliveries AS fct
							JOIN        rate 
	            						ON rate.order_id = fct.order_id
							LEFT JOIN	de.dds.dm_couriers AS dc 
								ON dc.id = fct.courier_id 
							GROUP BY 	1, 2, 3, 4
							)
							SELECT 
								courier_id,
								courier_name,
								settlement_year,
								settlement_month,
								orders_count,
								orders_total_sum,
								rate_avg,
								order_processing_fee,
								courier_order_sum,
								courier_tips_sum,
								courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
							FROM 		tmp
								ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
								SET
    									orders_count = EXCLUDED.orders_count,
    									orders_total_sum = EXCLUDED.orders_total_sum,
    									rate_avg = EXCLUDED.rate_avg,
    									order_processing_fee = EXCLUDED.order_processing_fee,
    									courier_order_sum = EXCLUDED.courier_order_sum,
    									courier_tips_sum = EXCLUDED.courier_tips_sum,
    									courier_reward_sum = EXCLUDED.courier_reward_sum;

				''' 
        self.repository = BaseRepository(pg, sql_rep)
        self.log = log

    def load_courier_ledger(self):
        self.repository.load_courier_ledger()