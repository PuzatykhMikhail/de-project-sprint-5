from lib import PgConnect
from logging import Logger


class BaseRepository:
    def __init__(self, pg: PgConnect, sql_update:str) -> None:
        self._sql_update = sql_update
        self._db = pg

    def load_delivery(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(self._sql_update)

class DeliveryLoad:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
	sql_for_delivery = """

				insert into dds.dm_deliveries (
   					 delivery_id, 
    					 courier_id, 
    					 order_id, 
   					 order_ts, 
    					 delivery_ts, 
    					 address, 
   					 rate, 
    					 tip_sum, 
   					 total_sum
				)
				WITH temp AS  
				(
					SELECT  
            					replace(replace(object_value, '"', ''), '''','"')::JSON as val
					FROM  	stg.system_deliveries 
				)
				SELECT  
           				 val->>'delivery_id' as delivery_id,
            				 dc.id as courier_id,
          			         dor.id as order_id,
            			         (val->>'order_ts')::timestamptz as order_ts,
            				 (val->>'delivery_ts')::timestamptz as delivery_ts,
            				 val->>'address' as address,
            				 (val->>'rate')::integer as rate,
           				 (val->>'tip_sum')::DECIMAL  as tip_sum,
           				 (val->>'sum')::DECIMAL as total_sum
				FROM  		temp
				LEFT  JOIN  	dds.dm_couriers dc ON val->>'courier_id' = dc.courier_id
				LEFT  JOIN  	dds.dm_orders dor ON val->>'order_id' = dor.order_key
				WHERE val->>'delivery_id' NOT IN (SELECT DISTINCT delivery_id FROM dds.dm_deliveries);

		 	   """
        self.repository = BaseRepository(pg, sql_for_delivery)
        self.log = log

    def fct_delivery_load(self):
        self.repository.load_delivery()