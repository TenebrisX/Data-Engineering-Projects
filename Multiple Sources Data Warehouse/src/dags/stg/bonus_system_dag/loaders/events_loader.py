from typing import List

from stg import EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime

# Import abstract class
from stg.bonus_system_dag.abstraction.bonus_system_abstract_loader import BonusAbstractLoader


class EventObj(BaseModel):
    """
    Pydantic BaseModel representing the structure of the 'outbox' table records.
    """
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository:
    """
    Repository for interacting with the 'outbox' table in the origin database.
    """
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, event_threshold: int, limit: int) -> List[EventObj]:
        """
        Retrieve a list of events from the 'outbox' table.

        Args:
            event_threshold (int): Threshold ID to filter events.

            limit (int): limit.

        Returns:
            List[EventObj]: List of EventObj instances.
        """
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s -- Skip objects that have already been loaded.
                    ORDER BY id ASC
                    LIMIT %(limit)s;
                """, {
                    "threshold": event_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class EventDestRepository:
    """
    Repository for inserting events into the destination database.
    """
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        """
        Insert or update an event in the 'stg.bonussystem_events' table.

        Args:
            conn (Connection): PostgreSQL database connection.
            event (EventObj): EventObj instance to be inserted or updated.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


class EventLoader(BonusAbstractLoader):
    """
    Class for loading events from the origin to the destination database.
    """
    WF_KEY = "bonus_system_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000

    def create_origin_repository(self, pg: PgConnect):
        """
        Create an instance of the EventsOriginRepository.

        Args:
            pg (PgConnect): PostgreSQL connection for the origin repository.

        Returns:
            EventsOriginRepository: Instance of the EventsOriginRepository.
        """
        return EventsOriginRepository(pg)

    def create_destination_repository(self):
        """
        Create an instance of the EventDestRepository.

        Returns:
            EventDestRepository: Instance of the EventDestRepository.
        """
        return EventDestRepository()

    def load(self):
        """
        Load events from the origin to the destination database.
        """
        # Open a transaction block.
        # The transaction will be committed if the code in the 'with' block runs successfully.
        # If an error occurs, changes will be rolled back.
        with self.pg_dest.connection() as conn:

            # Read the loading state.
            # If the setting doesn't exist, create it.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Read the next batch of objects.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_events(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for event in load_queue:
                self.destination.insert_event(conn, event)

            # Save the progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back together.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
