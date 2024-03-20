from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DMOrderObj(BaseModel):
    """Pydantic model for representing order objects."""
    id: int
    user_id: int
    restaurant_id: int
    timestamp_id: int
    delivery_id: int
    courier_id: int
    order_key: str
    order_status: str


class OrdersStgRepository:
    """Repository class for interacting with staging orders data."""
    
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[DMOrderObj]:
        """Retrieve a list of orders from the staging area."""
        with self._db.client().cursor(row_factory=class_row(DMOrderObj)) as cur:
            cur.execute(
                """
                    SELECT oo.id as id, 
                           du.id as user_id, 
                           dmr.id as restaurant_id,
                           dt.id as timestamp_id,
                           cd.id as delivery_id,
                           cc.id as courier_id,
                           oo.object_value::json->>'_id' as order_key,
                           oo.object_value::json->>'final_status' as order_status 
                    FROM stg.ordersystem_orders oo 
                    JOIN dds.dm_restaurants dmr ON dmr.restaurant_id = oo.object_value::json->'restaurant'->>'id'
                    JOIN dds.dm_timestamps dt ON dt.ts = (oo.object_value::json->>'update_ts')::timestamp
                    JOIN dds.dm_users du ON du.user_id = oo.object_value::json->'user'->>'id'
                    JOIN dds.c_deliveries cd ON cd.order_key = oo.object_id
                    JOIN dds.c_couriers cc ON cd.courier_id = cc.id 
                    WHERE oo.id > %(threshold)s
                    ORDER BY oo.id ASC
                    LIMIT %(limit)s
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DMOrdersDestRepository:
    """Repository class for interacting with the destination orders table."""
    
    def insert_orders(self, conn: Connection, order: DMOrderObj) -> None:
        """Insert or update order data in the destination table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, delivery_id, courier_id, order_key, order_status)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(delivery_id)s, %(courier_id)s, %(order_key)s, %(order_status)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status;
                """,
                {
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "delivery_id": order.delivery_id,
                    "courier_id": order.courier_id,
                    "order_key": order.order_key,
                    "order_status": order.order_status
                },
            )


class DMOrdersLoader:
    """Loader class for loading orders data into the destination table."""
    
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = OrdersStgRepository(pg_origin)
        self.dds = DMOrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        """Load orders data into the destination table."""
        # Open a transaction. It will be committed if the code in the 'with' block succeeds.
        # If an error occurs, changes will be rolled back (transaction rollback).
        with self.pg_dest.connection() as conn:

            # Read the loading state
            # If the setting does not exist, create it.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Read the next batch of objects.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for order in load_queue:
                self.dds.insert_orders(conn, order)

            # Save progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back as a whole.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
