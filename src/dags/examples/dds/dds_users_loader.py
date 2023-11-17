from logging import Logger
from typing import List, Optional
import json

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class UserJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class UserDdsObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str



class UserRawRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_raw_users(self, last_loaded_record_id: int) -> List[UserJsonObj]:
        with self._db.client().cursor(row_factory=class_row(UserJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class UserDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_user(self, user: UserDdsObj) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.dm_users(user_id, user_name, user_login)
                        VALUES (%(user_id)s, %(user_name)s, %(user_login)s);
                    """,
                    {
                        "user_id": user.user_id,
                        "user_name": user.user_name,
                        "user_login": user.user_login
                    },
                )
                conn.commit()

    def get_user(self, user_id: str) -> Optional[UserDdsObj]:
        with self._db.client().cursor(row_factory=class_row(UserDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        user_id,
                        user_name,
                        user_login
                    FROM dds.dm_users
                    WHERE user_id = %(user_id)s;
                """,
                {"user_id": user_id},
            )
            obj = cur.fetchone()
        return obj


class UserLoader:
    WF_KEY = "users_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_user_id"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.pg = pg
        self.raw = UserRawRepository(pg)
        self.dds = UserDdsRepository(pg)
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def parse_users(self, raws: List[UserJsonObj]) -> List[UserDdsObj]:
        res = []
        for r in raws:
            user_json = json.loads(r.object_value)
            t = UserDdsObj(id=r.id,
                           user_id=user_json['_id'],
                           user_name=user_json['name'],
                           user_login=user_json['login'],
                           )

            res.append(t)
        return res

    def load_users(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting= EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_users(last_loaded_id)
	    load_queue.sort(key=lambda x: x.id)
            users_to_load = self.parse_users(load_queue)
            for u in users_to_load:
                #self.dds.insert_user(conn, u)
                self.dds.insert_user(u)
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(u.id, wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)