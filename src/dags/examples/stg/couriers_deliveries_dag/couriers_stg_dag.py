import logging
import pendulum
from airflow.decorators import dag, task

from lib import ConnectionBuilder
from examples.stg.couriers_deliveries_dag.deliveries_loader import DeliveryLoader
from examples.stg.couriers_deliveries_dag.couriers_loader import CourierLoader

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 11, 18, tz="UTC"),
    catchup=False,
    tags=['project_sp_5', 'stg', 'schema'],
    is_paused_upon_creation=True
)

def stg_courier_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="deliveries_load")
    def deliveries_load():
        loader = DeliveryLoader(pg_dest=dwh_pg_connect, log=log)
        loader.load_delivery()

    @task(task_id="couriers_load")
    def couriers_load():
        loader = CourierLoader(pg_dest=dwh_pg_connect, log=log)
        loader.load_courier()

    deliveries = deliveries_load()
    couriers = couriers_load()

    deliveries
    couriers

stg_project_dag = stg_courier_dag()