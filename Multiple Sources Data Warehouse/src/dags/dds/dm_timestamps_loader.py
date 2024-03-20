from logging import Logger
from typing import List
from datetime import datetime

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DMTimestampsObj(BaseModel):
    """Pydantic model for representing timestamps objects."""
    id: int
    date: datetime


class TimestampsStgRepository:
    """Repository class for interacting with staging timestamps data."""

    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self, timestamps_threshold: int, limit: int) -> List[DMTimestampsObj]:
        """Retrieve a list of timestamps from the staging area."""
        with self._db.client().cursor(row_factory=class_row(DMTimestampsObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id,
                        (object_value::jsonb->>'update_ts')::timestamp as date
                    FROM stg.ordersystem_orders
                    WHERE id > %(threshold)s -- Skip objects that have already been loaded.
                    ORDER BY id ASC -- Sorting by id is necessary since id is used as a cursor.
                    LIMIT %(limit)s; -- Process only one batch of objects.
                """, {
                    "threshold": timestamps_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DMTimestampsDestRepository:
    """Repository class for interacting with the destination timestamps table."""

    def insert_timestamps(self, conn: Connection, timestamp: DMTimestampsObj) -> None:
        """Insert or update timestamp data in the destination table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s::timestamp, extract(year from %(year)s::date), extract(month from %(month)s::date), extract(day from %(day)s::date), %(time)s::time, %(date)s::date)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        ts = EXCLUDED.ts,
                        year = EXCLUDED.year,
                        month = EXCLUDED.month,
                        day = EXCLUDED.day,
                        time = EXCLUDED.time,
                        date = EXCLUDED.date;
                """,
                {
                    "ts": timestamp.date,
                    "year": timestamp.date,
                    "month": timestamp.date,
                    "day": timestamp.date,
                    "time": timestamp.date,
                    "date": timestamp.date
                },
            )


class DMTimestampLoader:
    """Loader class for loading timestamps data into the destination table."""

    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = TimestampsStgRepository(pg_origin)
        self.dds = DMTimestampsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
        """Load timestamps data into the destination table."""
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
            load_queue = self.stg.list_timestamps(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} timestamps to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for timestamp in load_queue:
                self.dds.insert_timestamps(conn, timestamp)

            # Save progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back as a whole.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
