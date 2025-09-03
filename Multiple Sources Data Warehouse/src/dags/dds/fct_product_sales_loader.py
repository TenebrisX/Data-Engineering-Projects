from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class FCTObj(BaseModel):
    """Pydantic model for representing FCT (Fact) objects."""
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float


class FCTRepository:
    """Repository class for interacting with staging FCT data."""

    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_facts(self, fcts_threshold: int, limit: int) -> List[FCTObj]:
        """Retrieve a list of FCT (Fact) objects from the staging area."""
        with self._db.client().cursor(row_factory=class_row(FCTObj)) as cur:
            cur.execute(
                """
                    -- The query fetches relevant data from bonus and orders subqueries.
                    -- It then joins the two subqueries based on product_id and order_id,
                    -- filtering by the specified threshold and limiting the result set.
                    WITH bonus AS (
                        SELECT DISTINCT
                            jsonb_array_elements_text(event_value::jsonb->'product_payments')::jsonb->>'product_id' as product_id,
                            event_value::jsonb->>'order_id' as order_id,
                            (jsonb_array_elements_text(event_value::jsonb->'product_payments')::jsonb->>'quantity')::int as "count",
                            (jsonb_array_elements_text(event_value::jsonb->'product_payments')::jsonb->>'bonus_payment')::numeric(19,5) as bonus_payment,
                            (jsonb_array_elements_text(event_value::jsonb->'product_payments')::jsonb->>'bonus_grant')::numeric(19,5) as bonus_grant
                        FROM stg.bonussystem_events be
                        WHERE event_type = 'bonus_transaction'
                    ),

                    orders AS (
                        SELECT 
                            do2.id,
                            dp.id as product_id,
                            do2.id as order_id,
                            dp.product_price as price,
                            dp.product_id as product_key,
                            do2.order_key as order_key
                        FROM 
                            dds.dm_products dp
                            JOIN dds.dm_restaurants dr ON dr.id = dp.restaurant_id
                            JOIN dds.dm_orders do2 ON dr.id = do2.restaurant_id
                            JOIN dds.dm_timestamps dt ON dt.id = do2.timestamp_id
                    )

                    SELECT 
                        o.id,
                        o.product_id,
                        o.order_id,
                        b.count,
                        o.price::numeric(19,5),
                        (o.price * b.count)::numeric(19,5) as total_sum,
                        b.bonus_payment,
                        b.bonus_grant
                    FROM 
                        bonus b
                        JOIN orders o ON b.product_id = o.product_key AND b.order_id = o.order_key
                    WHERE o.id > %(threshold)s
                    ORDER BY o.id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": fcts_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class FCTDestRepository:
    """Repository class for interacting with the destination FCT table."""

    def insert_facts(self, conn: Connection, fct: FCTObj) -> None:
        """Insert or update FCT (Fact) data in the destination table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                -- The query inserts FCT data into the destination table if it does not already exist.
                INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                SELECT
                    %(product_id)s,
                    %(order_id)s,
                    %(count)s,
                    %(price)s,
                    %(total_sum)s,
                    %(bonus_payment)s,
                    %(bonus_grant)s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM dds.fct_product_sales pr
                    INNER JOIN dds.dm_orders o ON pr.order_id = o.id
                    INNER JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                    INNER JOIN dds.dm_products dp ON pr.product_id = dp.id
                    WHERE
                        dp.id = %(product_id)s AND
                        o.id = %(order_id)s
                );
                    
                """,
                {
                    "product_id": fct.product_id,
                    "order_id": fct.order_id,
                    "count": fct.count,
                    "price": fct.price,
                    "total_sum": fct.total_sum,
                    "bonus_payment": fct.bonus_payment,
                    "bonus_grant": fct.bonus_grant
                },
            )


class FCTLoader:
    """Loader class for loading FCT (Fact) data into the destination table."""

    WF_KEY = "fct_product_sales_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = FCTRepository(pg_origin)
        self.dds = FCTDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_facts(self):
        """Load FCT (Fact) data into the destination table."""
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_facts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} facts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for product in load_queue:
                self.dds.insert_facts(conn, product)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
