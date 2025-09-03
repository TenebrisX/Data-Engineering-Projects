from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DMProductsObj(BaseModel):
    """Pydantic model for representing product objects."""
    id: int
    product_id: str
    product_name: str
    product_price: float
    restaurant_id: int
    active_from: str


class ProductsStgRepository:
    """Repository class for interacting with staging products data."""
    
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, timestamps_threshold: int, limit: int) -> List[DMProductsObj]:
        """Retrieve a list of products from the staging area."""
        with self._db.client().cursor(row_factory=class_row(DMProductsObj)) as cur:
            cur.execute(
                """
                    SELECT DISTINCT ON (product_id) id, product_id, product_name, product_price, active_from, restaurant_id
                    FROM (
                        SELECT oo.id as id,
                            (jsonb_array_elements(oo.object_value::jsonb->'order_items')->>'id') as product_id,
                            (jsonb_array_elements(oo.object_value::jsonb->'order_items')->>'name') as product_name, 
                            (jsonb_array_elements(oo.object_value::jsonb->'order_items')->>'price') as product_price,
                            oo.object_value::jsonb->>'update_ts' as active_from,
                            dmr.id as restaurant_id
                        FROM stg.ordersystem_orders oo
                        JOIN dds.dm_restaurants dmr ON dmr.restaurant_id = oo.object_value::json->'restaurant'->>'id'
                        WHERE oo.id > %(threshold)s
                        ORDER BY oo.id ASC
                        LIMIT %(limit)s
                    ) t
                """, {
                    "threshold": timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DMProductsDestRepository:
    """Repository class for interacting with the destination products table."""
    
    def insert_products(self, conn: Connection, product: DMProductsObj) -> None:
        """Insert or update product data in the destination table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s::timestamp, '2099-12-31 00:00:00.000'::timestamp)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        product_id = EXCLUDED.product_id,
                        product_name = EXCLUDED.product_name,
                        product_price = EXCLUDED.product_price,
                        active_from = EXCLUDED.active_from,
                        active_to = EXCLUDED.active_from;
                """,
                {
                    "restaurant_id": product.restaurant_id,
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from
                },
            )


class DMProductsLoader:
    """Loader class for loading products data into the destination table."""
    
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductsStgRepository(pg_origin)
        self.dds = DMProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
        """Load products data into the destination table."""
        # Open a transaction. It will be committed if the code in the 'with' block succeeds.
        # If an error occurs, changes will be rolled back (transaction rollback).
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for product in load_queue:
                self.dds.insert_products(conn, product)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings) 
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
