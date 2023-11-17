import logging
import pendulum
from airflow import DAG
from airflow.decorators import task
from lib import ConnectionBuilder

from examples.dds.dds_user_loader import UserLoader
from examples.dds.dds_restaurant_loader import RestaurantLoader
from examples.dds.dds_timestamp_loader import TimestampLoader
from examples.dds.dds_product_loader import ProductLoader
from examples.dds.dds_order_loader import OrderLoader
from examples.dds.fct_products_sales_loader import FctProductsLoader


log = logging.getLogger(__name__)

with DAG(
    dag_id='dds_dag',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2023, 11, 18, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'project', 'ddl'],
    is_paused_upon_creation=True
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_users_load")
    def load_dm_users():
        user_loader = UserLoader(dwh_pg_connect)
        user_loader.load_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        rest_loader = RestaurantLoader(dwh_pg_connect)
        rest_loader.load_restaurants()


    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        ts_loader = TimestampLoader(dwh_pg_connect)
        ts_loader.load_timestamps()


    @task(task_id="dm_products_load")
    def load_dm_products():
        prod_loader = ProductLoader(dwh_pg_connect)
        prod_loader.load_products()


    @task(task_id="dm_orders_load")
    def load_dm_orders():
        order_loader = OrderLoader(dwh_pg_connect)
        order_loader.load_orders()


    @task(task_id="fct_order_products_load")
    def load_fct_order_products():
        fct_loader = FctProductsLoader(dwh_pg_connect)
        fct_loader.load_product_facts()


    dm_users = load_dm_users()
    dm_restaurants = load_dm_restaurants()
    dm_timestamps = load_dm_timestamps()
    dm_products = load_dm_products()
    dm_orders = load_dm_orders()
    fct_order_products = load_fct_order_products()

    dm_restaurants >> dm_products
    dm_restaurants >> dm_orders
    dm_timestamps >> dm_orders
    dm_users >> dm_orders
    dm_products >> fct_order_products
    dm_orders >> fct_order_products