from logging import Logger

from lib.pg_connect import PgConnect
from lib.dict_util import json2str
from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from stg.delivery_system_dag.readers.deliveries_reader import DeliveryApiReader
from stg.delivery_system_dag.loaders.dest.deliveries_dest import DeliveryDest


class DeliveryLoader:
    WF_KEY = "deliveries_from_api_to_stg"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_LIMIT = 50
    MAX_ITERATIONS = 5

    def __init__(
        self,
        pg_dest: PgConnect,
        delivery_api_client: DeliveryApiReader,
        delivery_repository: DeliveryDest,
        logger: Logger,
    ) -> None:
        """
        Initialize the DeliveryLoader.

        Args:
            pg_dest (PgConnect): PostgreSQL connection object for the destination.
            delivery_api_client (DeliveryApiReader): Reader for fetching delivery data from the API.
            delivery_repository (DeliveryDest): Destination for loading delivery data.
            logger (Logger): Logger for logging messages.
        """
        self.pg_dest = pg_dest
        self.delivery_api_client = delivery_api_client
        self.delivery_repository = delivery_repository
        self.logger = logger
        self.settings_repository = StgEtlSettingsRepository()

    def load_deliveries(self) -> None:
        """
        Load delivery data from the API to the destination.

        Fetches deliveries in batches, processes them, and updates the last loaded timestamp in settings.
        """
        with self.pg_dest.connection() as conn:
            # Retrieve the workflow setting for last loaded timestamp
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)

            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_TS_KEY: "2022-01-01 00:00:00"},
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            self.logger.info(f"Starting to load from last checkpoint: {last_loaded_ts_str} ...")

            for _ in range(self.MAX_ITERATIONS):
                # Get a batch of deliveries from the API
                load_queue = self.delivery_api_client.get_deliveries(self.BATCH_LIMIT, 0, last_loaded_ts_str)
                self.logger.info(f"Found {len(load_queue)} objects to load.")

                if not load_queue:
                    self.logger.info("Quitting")
                    break

                # Process each delivery in the batch
                for delivery in load_queue:
                    try:
                        self.delivery_repository.insert_entry(conn, delivery)
                    except Exception as e:
                        self.logger.error(f"Error inserting delivery {delivery.delivery_id}: {e}", exc_info=True)

                # Update the last loaded timestamp with the maximum timestamp in the current batch
                last_loaded_ts = max([t.delivery_ts for t in load_queue])
                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts

            # Save the updated settings to the database
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.logger.info(
                f"Finished work. Last checkpoint: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}"
            )
