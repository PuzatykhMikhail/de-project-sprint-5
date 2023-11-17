import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from lib import ConnectionBuilder

from examples.cdm.courier_report import CourierLedgerLoad

log = logging.getLogger(__name__)

with DAG(
    dag_id='cdm_project_5_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 11, 18, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm', 'project', 'report'],
    is_paused_upon_creation=True
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="couriers_report")
    def report_couriers():
        courier_loader = CourierLedgerLoad(dwh_pg_connect, log)
        courier_loader.load_courier_ledger()


    courier_loader_ledger_report = report_couriers()

    courier_loader_ledger_report