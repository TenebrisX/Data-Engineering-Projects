from typing import List

from stg import EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

# Import abstract class
from stg.bonus_system_dag.abstraction.bonus_system_abstract_loader import BonusAbstractLoader


class UserObj(BaseModel):
    """
    Pydantic BaseModel representing the structure of the 'users' table records.
    """
    id: int
    order_user_id: str


class UsersOriginRepository:
    """
    Repository for interacting with the 'users' table in the origin database.
    """
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self, user_threshold: int, limit: int) -> List[UserObj]:
        """
        Retrieve a list of users from the 'users' table.

        Args:
            user_threshold (int): Threshold ID to filter users.
            limit (int): Maximum number of users to retrieve.

        Returns:
            List[UserObj]: List of UserObj instances.
        """
        with self._db.client().cursor(row_factory=class_row(UserObj)) as cur:
            cur.execute(
                """
                    SELECT id, order_user_id
                    FROM users
                    WHERE id > %(threshold)s -- Skip objects that have already been loaded.
                    ORDER BY id ASC -- Mandatory sorting by id, as id is used as a cursor.
                    LIMIT %(limit)s; -- Process only one batch of objects.
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class UserDestRepository:
    """
    Repository for inserting users into the destination database.
    """

    def insert_user(self, conn: Connection, user: UserObj) -> None:
        """
        Insert or update a user in the 'stg.bonussystem_users' table.

        Args:
            conn (Connection): PostgreSQL database connection.
            user (UserObj): UserObj instance to be inserted or updated.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_users(id, order_user_id)
                    VALUES (%(id)s, %(order_user_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_user_id = EXCLUDED.order_user_id;
                """,
                {
                    "id": user.id,
                    "order_user_id": user.order_user_id
                },
            )


class UserLoader(BonusAbstractLoader):
    """
    Class for loading users from the origin to the destination database.
    """
    WF_KEY = "bonus_system_users_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def create_origin_repository(self, pg: PgConnect):
        """
        Create an instance of the UsersOriginRepository.

        Args:
            pg (PgConnect): PostgreSQL connection for the origin repository.

        Returns:
            UsersOriginRepository: Instance of the UsersOriginRepository.
        """
        return UsersOriginRepository(pg)

    def create_destination_repository(self):
        """
        Create an instance of the UserDestRepository.

        Returns:
            UserDestRepository: Instance of the UserDestRepository.
        """
        return UserDestRepository()

    def load(self):
        """
        Load users from the origin to the destination database.
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
            load_queue = self.origin.list_users(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for user in load_queue:
                self.destination.insert_user(conn, user)

            # Save the progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back together.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
