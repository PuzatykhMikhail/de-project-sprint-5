from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierObj(BaseModel):
    id: int
    courier_id: str
    name: str


class CourierReader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, threshold: int, limit: int) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        object_id AS courier_id,
                        replace(replace(
                        object_value, '"', ''), '''','"')::JSON->>'name' as name
                    FROM stg.system_couriers
                    WHERE id > %(threshold)s
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            obj = cur.fetchall()
        return obj


class CourierSaver:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, name)
                    VALUES (%(courier_id)s, %(name)s)
                """,
                {
                    "courier_id": courier.courier_id,
                    "name": courier.name
                },
            )


class CourierLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.reader = CourierReader(pg)
        self.saver = CourierSaver()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        with self.pg.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.reader.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} rows to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return
            for courier in load_queue:
                self.saver.insert_courier(conn, courier)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            self.settings_repository.save_setting(conn, wf_setting)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")