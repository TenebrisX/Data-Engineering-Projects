from logging import Logger
from typing import List
from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

class CDeliveryObj(BaseModel):
    id: int
    delivery_id: str
    order_key: str
    order_ts: datetime
    courier_id: int
    delivery_ts: datetime
    tip_sum: float
    rate: int
    sum: float

class DeliveriesStgRepository:
    def __init__(self, pg: PgConnect) -> None:
        """Repository for handling operations related to stg.deliverysystem_deliveries table."""
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[CDeliveryObj]:
        """Retrieve a list of deliveries from stg.deliverysystem_deliveries table."""
        with self._db.client().cursor(row_factory=class_row(CDeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT
                        dd.id AS id,
                        dd.object_id AS delivery_id,
                        dd.object_value::jsonb->>'order_id' AS order_key,
                        (dd.object_value::jsonb->>'order_ts')::timestamp AS order_ts,
                        cc.id AS courier_id,
                        dd.delivery_ts AS delivery_ts,
                        dd.object_value::jsonb->>'tip_sum' AS tip_sum,
                        dd.object_value::jsonb->>'rate' AS rate,
                        dd.object_value::jsonb->>'sum' AS "sum"
                    FROM
                        stg.deliverysystem_deliveries dd
                    JOIN dds.c_couriers cc ON cc.courier_id = dd.object_value::jsonb->>'courier_id'
                    WHERE dd.id > %(threshold)s
                    ORDER BY dd.id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs

class CDeliveriesDestRepository:
    def insert_deliveries(self, conn: Connection, delivery: CDeliveryObj) -> None:
        """Insert or update a delivery in the dds.c_deliveries table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.c_deliveries(delivery_id, order_key, order_ts, courier_id, delivery_ts, tip_sum, rate, sum)
                    VALUES (%(delivery_id)s, %(order_key)s, %(order_ts)s, %(courier_id)s, %(delivery_ts)s, %(tip_sum)s, %(rate)s, %(sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        order_key = EXCLUDED.order_key,
                        order_ts = EXCLUDED.order_ts,
                        courier_id = EXCLUDED.courier_id,
                        delivery_ts = EXCLUDED.delivery_ts,
                        tip_sum = EXCLUDED.tip_sum,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum;
                """, {
                    "delivery_id": delivery.delivery_id,
                    "order_key": delivery.order_key,
                    "order_ts": delivery.order_ts,
                    "courier_id": delivery.courier_id,
                    "delivery_ts": delivery.delivery_ts,
                    "tip_sum": delivery.tip_sum,
                    "rate": delivery.rate,
                    "sum": delivery.sum
                },
            )

class CDeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        """Loader for transferring data from stg to dds for deliveries."""
        self.pg_dest = pg_dest
        self.stg = DeliveriesStgRepository(pg_origin)
        self.dds = CDeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        """Load deliveries data from stg to dds."""
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for delivery in load_queue:
                self.dds.insert_deliveries(conn, delivery)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
