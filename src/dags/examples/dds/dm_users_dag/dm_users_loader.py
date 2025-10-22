from logging import Logger
from typing import List, Optional
import json

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from pydantic import BaseModel
from psycopg.rows import class_row


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
    def load_users(self, conn: Connection, last_loaded_record_id: int) -> List[UserJsonObj]:
        with conn.cursor(row_factory=class_row(UserJsonObj)) as cur:
            cur.execute(
                """
                    SELECT id, object_id, object_value 
                    FROM stg.ordersystem_users
                    WHERE id > %(last_loaded_record_id)s; 
                """, {
                    "last_loaded_record_id": last_loaded_record_id
                }
            )
            objs = cur.fetchall()
        return objs


class UserDdsRepository:
    def insert_user(self, conn: Connection, user: UserDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users (user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_name = EXCLUDED.user_name,  
                        user_login = EXCLUDED.user_login
                """,
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )

    def get_user(self, conn: Connection, user_id: str) -> Optional[UserDdsObj]:
        with conn.cursor(row_factory=class_row(UserDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id, user_id, user_name, user_login
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
    BATCH_LIMIT = 100  # Загружаем 100 записей за раз для получения нужного количества.

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw = UserRawRepository()
        self.dds = UserDdsRepository()
        self.settings_repository = settings_repository
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_users(self, raws: List[UserJsonObj]) -> List[UserDdsObj]:
        res = []
        for r in raws:
            user = json.loads(r.object_value)
            t = UserDdsObj(id = r.id,
                           user_id = user['_id'],
                           user_name = user['name'], 
                           user_login = user['login']
                           )
            res.append(t)
        return res

    def load_users(self):
        # with self.settings_repository.connection() as conn:
        with self.dwh.connection() as conn:
            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            load_queue = self.raw.load_users(conn, last_loaded)
            load_queue.sort(key=lambda x: x.id)
            users_to_load = self.parse_users(load_queue)
            for u in users_to_load:
                existing = self.dds.get_user(conn, u.user_id)
                if not existing:
                    self.dds.insert_user(conn, u)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                # self.settings_repository.save_setting(conn, wf_setting.workflow_settings)
                # self.settings_repository.save_setting(conn, self.WF_KEY, wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, self.WF_KEY, json2str(wf_setting.workflow_settings))
                