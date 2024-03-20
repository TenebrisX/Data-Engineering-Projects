from typing import List

from stg import EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


# Import abstract class
from stg.bonus_system_dag.abstraction.bonus_system_abstract_loader import BonusAbstractLoader


class RankObj(BaseModel):
    """
    Pydantic BaseModel representing the structure of the 'outbox' table records.
    """
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


class RanksOriginRepository:
    """
    Repository for interacting with the 'ranks' table in the origin database.
    """
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ranks(self, rank_threshold: int, limit: int) -> List[RankObj]:
        """
        Retrieve a list of ranks from the 'ranks' table.

        Args:
            rank_threshold (int): Threshold ID to filter ranks.
            limit (int): Maximum number of ranks to retrieve.

        Returns:
            List[RankObj]: List of RankObj instances.
        """
        with self._db.client().cursor(row_factory=class_row(RankObj)) as cur:
            cur.execute(
                """
                    SELECT id, name, bonus_percent, min_payment_threshold
                    FROM ranks
                    WHERE id > %(threshold)s
                    ORDER BY id ASC 
                    LIMIT %(limit)s; 
                """, {
                    "threshold": rank_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RankDestRepository:
    """
    Repository for inserting ranks into the destination database.
    """

    def insert_rank(self, conn: Connection, rank: RankObj) -> None:
        """
        Insert or update a rank in the 'stg.bonussystem_ranks' table.

        Args:
            conn (Connection): PostgreSQL database connection.
            rank (RankObj): RankObj instance to be inserted or updated.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                    VALUES (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        name = EXCLUDED.name,
                        bonus_percent = EXCLUDED.bonus_percent,
                        min_payment_threshold = EXCLUDED.min_payment_threshold;
                """,
                {
                    "id": rank.id,
                    "name": rank.name,
                    "bonus_percent": rank.bonus_percent,
                    "min_payment_threshold": rank.min_payment_threshold
                },
            )


class RankLoader(BonusAbstractLoader):
    """
    Class for loading ranks from the origin to the destination database.
    """
    WF_KEY = "bonus_system_ranks_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 3

    def create_origin_repository(self, pg: PgConnect):
        """
        Create an instance of the RanksOriginRepository.

        Args:
            pg (PgConnect): PostgreSQL connection for the origin repository.

        Returns:
            RanksOriginRepository: Instance of the RanksOriginRepository.
        """
        return RanksOriginRepository(pg)

    def create_destination_repository(self):
        """
        Create an instance of the RankDestRepository.

        Returns:
            RankDestRepository: Instance of the RankDestRepository.
        """
        return RankDestRepository()

    def load(self):
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
            load_queue = self.origin.list_ranks(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ranks to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Save objects to the dwh database.
            for rank in load_queue:
                self.destination.insert_rank(conn, rank)

            # Save the progress.
            # We use the same connection, so the setting will be saved along with the objects,
            # or all changes will be rolled back together.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Convert to a string to store in the database.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
