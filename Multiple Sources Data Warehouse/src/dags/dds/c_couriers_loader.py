from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CCourier(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class CouriersStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        """Repository for handling operations related to stg.deliverysystem_couriers table."""
        self._db = pg

    def list_couriers(self, courier_threshold: int, limit: int) -> List[CCourier]:
        """Retrieve a list of couriers from stg.deliverysystem_couriers table."""
        with self._db.client().cursor(row_factory=class_row(CCourier)) as cur:
            cur.execute(
                """
                    SELECT 
                        dc.id AS id,
                        dc.object_id AS courier_id,
                        dc.object_value AS courier_name
                    FROM 
                        stg.deliverysystem_couriers dc 
                    WHERE dc.id > %(threshold)s
                    ORDER BY dc.id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": courier_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CCourierDestRepository:

    def insert_courier(self, conn: Connection, courier: CCourier) -> None:
        """Insert or update a courier in the dds.c_couriers table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.c_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name,
                },
            )


class CCurierLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        """Loader for transferring data from stg to dds for couriers."""
        self.pg_dest = pg_dest
        self.stg = CouriersStgRepository(pg_origin)
        self.dds = CCourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        """Load couriers data from stg to dds."""
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for courier in load_queue:
                self.dds.insert_courier(conn, courier)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
