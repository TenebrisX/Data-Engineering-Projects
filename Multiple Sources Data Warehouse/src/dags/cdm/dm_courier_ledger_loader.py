from logging import Logger
from typing import List

from cmd_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel



class CourierLedgerObj(BaseModel):
    courier_id: int
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    total_payment: float


class CourierLedgerDdsRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reports(self, report_threshold: int, limit: int) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            
            with open("src\dags\cdm\sql\courier_ledger.sql", "r") as file:
                query = file.read()
            cur.execute(query, {"threshold": report_threshold, "limit": limit})
            objs = cur.fetchall()
        return objs


class CourierLedgerDestRepository:

    def insert_report(self, conn: Connection, report: CourierLedgerObj) -> None:
        with conn.cursor() as cur:
           cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger
                    (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, total_payment)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s, %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, %(courier_order_sum)s, %(courier_tips_sum)s, %(total_payment)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name,
                        settlement_year = EXCLUDED.settlement_year,
                        settlement_month = EXCLUDED.settlement_month,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        total_payment = EXCLUDED.total_payment;
                """,
                {
                    "courier_id": report.courier_id,
                    "courier_name": report.courier_name,
                    "settlement_year": report.settlement_year,
                    "settlement_month": report.settlement_month,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "rate_avg": report.rate_avg,
                    "order_processing_fee": report.order_processing_fee,
                    "courier_order_sum": report.courier_order_sum,
                    "courier_tips_sum": report.courier_tips_sum,
                    "total_payment": report.total_payment,
                },
            )

class CourierLedgerLoader:
    WF_KEY = "courier_ledger_dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        """Loader for transferring data from dds to cdm for the report."""
        self.pg_dest = pg_dest
        self.dds = CourierLedgerDdsRepository(pg_origin)
        self.cdm = CourierLedgerDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
        """Load report data from dds to cdm."""
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.dds.list_reports(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} objects to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for report in load_queue:
                self.cdm.insert_report(conn, report)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.courier_id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
