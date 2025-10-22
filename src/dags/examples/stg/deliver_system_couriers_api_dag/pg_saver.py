from datetime import datetime
from typing import Any, List, Dict

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:
    def save_batch(self, conn: Connection, items: List[Dict[str, Any]]) -> None:
        rows = [(it["_id"], json2str(it)) for it in items]
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO stg.deliversystem_couriers(object_id, object_value)
                VALUES (%s, %s)
                ON CONFLICT (id) DO UPDATE
                  SET object_value = EXCLUDED.object_value;
                """,
                rows
            )
