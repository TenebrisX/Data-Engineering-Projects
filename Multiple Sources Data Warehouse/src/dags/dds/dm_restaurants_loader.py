from logging import Logger
from typing import List
from datetime import datetime

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DMRestaurantsObj(BaseModel):
    """Pydantic model for representing restaurant objects."""
    id: int
    restaurant_id: str
    restaurant_name: str
    date: datetime


class RestaurantsStgRepository:
    """Repository class for interacting with staging restaurants data."""

    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_threshold: int, limit: int) -> List[DMRestaurantsObj]:
        """Retrieve a list of restaurants from the staging area."""
        with self._db.client().cursor(row_factory=class_row(DMRestaurantsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        (object_value::jsonb->>'_id') as restaurant_id,
                        (object_value::jsonb->>'name') as restaurant_name,
                        (object_value::jsonb->>'update_ts')::timestamp as date
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(threshold)s -- Skip objects that have already been loaded.
                    ORDER BY id ASC -- Sorting by id is necessary since id is used as a cursor.
                    LIMIT %(limit)s; -- Process only one batch of objects.
                """, {
                    "threshold": restaurant_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DMRestaurantsDestRepository:
    """Repository class for interacting with the destination restaurants table."""

    def insert_restaurant(self, conn: Connection, restaurant: DMRestaurantsObj) -> None:
        """Insert or update restaurant data in the destination table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s::timestamp, '2099-12-31 00:00:00.000'::timestamp)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name,
                        active_from = EXCLUDED.active_from;
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name,
                    "active_from": restaurant.date
                },
            )


class DMRestaurantLoader:
    """Loader class for loading restaurants data into the destination table."""

    WF_KEY = "example_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = RestaurantsStgRepository(pg_origin)
        self.dds = DMRestaurantsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        """Load restaurants data into the destination table."""
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
            load_queue = self.stg.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for restaurant in load_queue:
                self.dds.insert_restaurant(conn, restaurant)

            # Save progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back as a whole.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
