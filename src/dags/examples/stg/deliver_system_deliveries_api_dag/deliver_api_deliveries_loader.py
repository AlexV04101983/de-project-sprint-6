from datetime import datetime
from logging import Logger
from typing import List, Dict, Any

from examples.stg import EtlSetting, StgEtlSettingsRepository
from examples.stg.deliver_system_deliveries_api_dag.pg_saver import PgSaver
from examples.stg.deliver_system_deliveries_api_dag.delivers_api_deliveries_reader import DeliveriesReader
from lib import PgConnect
from lib.dict_util import json2str


class DeliveriesLoader:
    api_endpoint = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    api_token = '25c27781-8fde-4b30-a22e-524044a7580f'
    nickname = 'VSharonov'
    cohort = '39'

    headers = {
        'X-Nickname': nickname,
        'X-Cohort': cohort,
        'X-API-KEY': api_token
    }

    WF_KEY = "example_api_deliveries_origin_to_stg_workflow"
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"
    PAGE_LIMIT = 50
    LOG_THRESHOLD = 500

    def __init__(self, collection_loader: DeliveriesReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger

    def run_copy(self) -> int:
        total = 0
        with self.pg_dest.connection() as conn:
            wf = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf:
                wf = EtlSetting(id=0, workflow_key=self.WF_KEY,
                                workflow_settings={self.LAST_LOADED_OFFSET_KEY: 0})

            offset = int(wf.workflow_settings.get(self.LAST_LOADED_OFFSET_KEY, 0))
            self.log.info(f"Start loading deliveries: offset={offset}, limit={self.PAGE_LIMIT}")

            while True:
                items: List[Dict[str, Any]] = self.collection_loader.get_page(
                    headers=self.headers,
                    api_endpoint=self.api_endpoint,
                    offset=offset,
                    limit=self.PAGE_LIMIT
                )
                if not items:
                    # дошли до конца — сбрасываем курсор
                    wf.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = 0
                    self.settings_repository.save_setting(conn, wf.workflow_key, json2str(wf.workflow_settings))
                    self.log.info("No more pages. Offset reset to 0. Done.")
                    break
                for item in items: 
                    object_id = item["delivery_id"]
                    update_ts = datetime.fromisoformat(item["delivery_ts"].replace('Z', '+00:00'))

                self.pg_saver.save_batch(conn, object_id, update_ts, item)

                cnt = len(items)
                total += cnt
                offset += cnt
                wf.workflow_settings[self.LAST_LOADED_OFFSET_KEY] = offset
                self.settings_repository.save_setting(conn, wf.workflow_key, json2str(wf.workflow_settings))

                if total % self.LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {total} deliveries...")

            self.log.info(f"Finished. Total loaded/updated: {total}")
            return total
