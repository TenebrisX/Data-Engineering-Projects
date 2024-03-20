from logging import Logger
from typing import List

from dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
import lib.dict_util as du
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DMUserObj(BaseModel):
    """Pydantic model for representing user objects."""
    id: int
    user_id: str
    user_name: str
    user_login: str


class UsersStgRepository:
    """Repository class for interacting with staging users data."""

    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, user_threshold: int, limit: int) -> List[DMUserObj]:
        """Retrieve a list of users from the staging area."""
        with self._db.client().cursor(row_factory=class_row(DMUserObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        id as id,
                        object_value::jsonb->>'_id' as user_id,
                        object_value::jsonb->>'name' as user_name,
                        object_value::jsonb->>'login' as user_login
                    FROM stg.ordersystem_users ou 
                    WHERE id > %(threshold)s -- Skip objects that have already been loaded.
                    ORDER BY id ASC -- Sorting by id is necessary since id is used as a cursor.
                    LIMIT %(limit)s; -- Process only one batch of objects.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DMUserDestRepository:
    """Repository class for interacting with the destination users table."""

    def insert_user(self, conn: Connection, user: DMUserObj) -> None:
        """Insert or update user data in the destination table."""
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dds.dm_users(user_id, user_name, user_login)
                VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                ON CONFLICT (id) DO UPDATE
                SET
                    user_id = EXCLUDED.user_id,
                    user_name = EXCLUDED.user_name,
                    user_login = EXCLUDED.user_login;
                    
                """,
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )


class DMUserLoader:
    """Loader class for loading users data into the destination table."""

    WF_KEY = "example_users_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = UsersStgRepository(pg_origin)
        self.dds = DMUserDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_users(self):
        """Load users data into the destination table."""
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
            load_queue = self.stg.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for user in load_queue:
                self.dds.insert_user(conn, user)

            # Save progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back as a whole.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = du.json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
