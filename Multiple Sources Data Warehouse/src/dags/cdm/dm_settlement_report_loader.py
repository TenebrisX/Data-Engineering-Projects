"""Settlement report data loader from DDS to CDM layer.

This module handles the ETL process for loading settlement report data from the DDS layer
to the CDM layer, including restaurant performance metrics and financial calculations.
"""

from logging import Logger
from typing import List

from cmd_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import date, timedelta


class SettlementReportObj(BaseModel):
    """Data model for settlement report objects."""
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float


class SettlementReportDdsRepository:
    """Repository for reading settlement report data from DDS layer."""
    
    def __init__(self, pg: PgConnect) -> None:
        """Initialize the DDS repository.
        
        Args:
            pg: PostgreSQL connection object.
        """
        self._db = pg

    def list_reports(self, report_threshold: date, limit: int) -> List[SettlementReportObj]:
        """Retrieve settlement reports from DDS layer.
        
        Args:
            report_threshold: Date threshold for filtering reports.
            limit: Maximum number of reports to retrieve.
            
        Returns:
            List of SettlementReportObj instances.
        """
        with self._db.client().cursor(row_factory=class_row(SettlementReportObj)) as cur:
            cur.execute(
                """
                SELECT
                    dr.id AS restaurant_id,
                    dr.restaurant_name AS restaurant_name,
                    DATE(dt.date) AS settlement_date,
                    COUNT(distinct do2.id) AS orders_count,
                    SUM(fps.total_sum) AS orders_total_sum,
                    SUM(fps.bonus_payment) AS orders_bonus_payment_sum,
                    SUM(fps.bonus_grant) AS orders_bonus_granted_sum,
                    SUM(fps.total_sum) * 0.25 AS order_processing_fee,
                    SUM(fps.total_sum) - SUM(fps.bonus_payment) - (SUM(fps.total_sum) * 0.25) AS restaurant_reward_sum
                FROM
                    dds.dm_orders do2
                JOIN
                    dds.dm_timestamps dt ON dt.id = do2.timestamp_id
                JOIN
                    dds.fct_product_sales fps ON fps.order_id = do2.id
                JOIN
                    dds.dm_restaurants dr ON dr.id = do2.restaurant_id
                WHERE
                    do2.order_status = 'CLOSED'
                    AND dt.ts >= DATE_TRUNC('day', CURRENT_DATE) - INTERVAL '1 month'
                    AND dt.date::date > %(threshold)s::date
                GROUP BY
                    dt.date::date, restaurant_name, dr.id
                ORDER BY
                    dt.date ASC, settlement_date, restaurant_name
                LIMIT %(limit)s;
                """, {
                    "threshold": report_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class SettlementReportDestRepository:
    """Repository for writing settlement report data to CDM layer."""
    
    def insert_report(self, conn: Connection, report: SettlementReportObj) -> None:
        """Insert settlement report into CDM layer.
        
        Args:
            conn: Database connection.
            report: SettlementReportObj to insert.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cdm.dm_settlement_report
                (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                VALUES (%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s)
                ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                SET
                    restaurant_name = EXCLUDED.restaurant_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": report.restaurant_id,
                    "restaurant_name": report.restaurant_name,
                    "settlement_date": report.settlement_date,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "orders_bonus_payment_sum": report.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": report.orders_bonus_granted_sum,
                    "order_processing_fee": report.order_processing_fee,
                    "restaurant_reward_sum": report.restaurant_reward_sum
                },
            )


class SettlementReportLoader:
    """Loader for transferring settlement report data from DDS to CDM layer."""
    
    WF_KEY = "settlement_report_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_date"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        """Initialize the settlement report loader.
        
        Args:
            pg_origin: Source database connection.
            pg_dest: Destination database connection.
            log: Logger instance.
        """
        self.pg_dest = pg_dest
        self.dds = SettlementReportDdsRepository(pg_origin)
        self.cdm = SettlementReportDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_reports(self):
        """Load settlement reports from DDS to CDM layer."""
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: date.today() - timedelta(days=30)})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"Last loaded date: {last_loaded}")

            if isinstance(last_loaded, date):
                last_loaded = last_loaded.isoformat()

            load_queue = self.dds.list_reports(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} reports to load.")

            if not load_queue:
                self.log.info("Quitting.")
                return

            self.log.info(f"Load queue length: {len(load_queue)}")

            for report in load_queue:
                self.cdm.insert_report(conn, report)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.settlement_date.isoformat() for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")