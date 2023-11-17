from logging import Logger
import logging
from typing import Any
import time
import requests


from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection

HEADERS = {
        "X-API-KEY": '25c27781-8fde-4b30-a22e-524044a7580f',
        "X-Nickname": 'mikhail_puzatykh',
        "X-Cohort": '17'
    }
API_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=%s&sort_direction=%s&limit=%d&offset=%d"


class DeliveryOriginRepository:
    def list_deliveries(self, sort_field: str = "date",
                        sort_direction: str = "asc",
                        offset: int = 0, 
                        limit: int = 500):
        api_url = API_URL % (sort_field, sort_direction, limit, offset)
        s = requests.Session()
        for i in range(5):
            try:
                response = s.get(api_url, headers=HEADERS)
                response.raise_for_status()
            except requests.exceptions.ConnectionError as err:
                logging.error(err)
                time.sleep(10)
            if response.status_code == 200:
                list_response = list(response.json())
                logging.info('Recieved for load objects: ', len(list_response))
                break
            elif i == 4:
                raise TimeoutError("TimeoutError fail to get deliveries.")
        return list_response


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, object_id: str, value: Any) -> None:
        value_str = json2str(value)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.deliveries(object_id, object_value)
                    VALUES (%(object_id)s, %(value)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value
                """,
                {
                    "object_id": object_id,
                    "value": value_str
                }
            )


class DeliveryLoader:
    WF_KEY = "deliveries_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.reader = DeliveryOriginRepository()
        self.saver = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_delivery(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY,
                                        workflow_settings={self.LAST_LOADED_ID_KEY: 0})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.reader.list_deliveries(offset=last_loaded)
            self.log.info(f"Found {len(load_queue)} objects deliveries to load.")
            if len(load_queue) == 0:
                self.log.info(f"there is no object to load. Last id: {last_loaded}")
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded - 50
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn,
                                                      wf_setting.workflow_key,
                                                      wf_setting_json)

                self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
                return
            for obj in load_queue:
                self.saver.insert_delivery(conn, str(obj["order_id"]), str(obj))

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = len(load_queue) + last_loaded
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")