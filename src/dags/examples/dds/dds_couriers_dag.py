import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from lib import ConnectionBuilder

from examples.dds.dds_courier_loader import CourierLoader
from examples.dds.fct_delivery_loader import DeliveryLoad

log = logging.getLogger(__name__)

with DAG(
    dag_id='dds_project_5_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 11, 18, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'project', 'ddl'],
    is_paused_upon_creation=True
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        courier_loader = CourierLoader(dwh_pg_connect, log)
        courier_loader.load_couriers()

    @task(task_id="dm_delivery_load")
    def fact_delivery_load():
        delivery_loader = DeliveryLoad(dwh_pg_connect, log)
        delivery_loader.fct_delivery_load()

    dm_couriers = load_dm_couriers()
    dm_deliveries = fact_delivery_load()


    dm_couriers >> dm_deliveries