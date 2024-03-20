from abc import ABC, abstractmethod
from datetime import datetime
from logging import Logger
from lib import PgConnect
from stg import EtlSetting, StgEtlSettingsRepository
from lib.pg_saver import PgSaver
from stg.order_system_dag.abstraction.mongo_abstract_reader import MongoReader

class MongoLoader(ABC):
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    WF_KEY = ""  # Workflow key for identifying the workflow
    LAST_LOADED_TS_KEY = ""  # Key for storing the last loaded timestamp

    def __init__(self, collection_loader: MongoReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        """
        Constructor for MongoLoader.

        Args:
            collection_loader (MongoReader): The reader for the MongoDB collection.
            pg_dest (PgConnect): The PostgreSQL connection.
            pg_saver (PgSaver): The saver for saving data to PostgreSQL.
            logger (Logger): The logger for logging messages.
        """
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger
        self.doc_name = collection_loader.get_collection_name()

    def run_copy(self) -> int:
        """
        Run the data copy process.

        Returns:
            int: The number of documents copied.
        """
        with self.pg_dest.connection() as conn:
            wf_setting = self._get_or_create_workflow_setting(conn)

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_documents(last_loaded_ts, self._SESSION_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from {self.doc_name} collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            self._process_and_save_documents(conn, load_queue)

            self._update_workflow_setting(conn, wf_setting, load_queue)

            return len(load_queue)

    @abstractmethod
    def _get_or_create_workflow_setting(self, conn: PgConnect) -> EtlSetting:
        """
        Abstract method to get or create the workflow setting.

        Args:
            conn (PgConnect): The PostgreSQL connection.

        Returns:
            EtlSetting: The workflow setting.
        """
        pass

    @abstractmethod
    def _process_and_save_documents(self, conn: PgConnect, load_queue: list) -> None:
        """
        Abstract method to process and save documents.

        Args:
            conn (PgConnect): The PostgreSQL connection.
            load_queue (list): The list of documents to process and save.
        """
        pass

    @abstractmethod
    def _update_workflow_setting(self, conn: PgConnect, wf_setting: EtlSetting, load_queue: list) -> None:
        """
        Abstract method to update the workflow setting.

        Args:
            conn (PgConnect): The PostgreSQL connection.
            wf_setting (EtlSetting): The workflow setting.
            load_queue (list): The list of documents processed.
        """
        pass
