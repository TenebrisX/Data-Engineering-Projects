from logging import Logger

from lib.pg_connect import PgConnect
from lib.dict_util import json2str

from stg.stg_settings_repository import EtlSetting, StgEtlSettingsRepository
from stg.delivery_system_dag.readers.restaurant_reader import RestaurantApiReader
from stg.delivery_system_dag.loaders.dest.restaurants_dest import RestaurantDest



class RestaurantLoader:
    WF_KEY = "restaurants_from_api_to_stg"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100
    
    def __init__(
        self,
        pg_dest: PgConnect,
        restaurant_api_reader: RestaurantApiReader,
        restaurant_repository: RestaurantDest,
        logger: Logger,
    ) -> None:
        """
        Initialize the RestaurantLoader.

        Args:
            pg_dest (PgConnect): PostgreSQL connection object for the destination.
            restaurant_api_reader (RestaurantApiReader): Reader for fetching restaurant data from the API.
            restaurant_repository (RestaurantDest): Destination for loading restaurant data.
            logger (Logger): Logger for logging messages.
        """
        self.pg_dest = pg_dest
        self.restaurant_api_reader = restaurant_api_reader
        self.restaurant_repository = restaurant_repository
        self.logger = logger

    def load_restaurants(self) -> None:
        """
        Load restaurant data from the API to the destination.

        Fetches restaurants in batches, processes them, and updates the last loaded ID in settings.
        """
        self.settings_repository = StgEtlSettingsRepository()
        limit = 50
        max_iterations = 5

        with self.pg_dest.connection() as conn:
            # Retrieve the workflow setting for the last loaded ID
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})
     
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.logger.info(f"Starting to load from last checkpoint: {last_loaded_id} ...")

            for _ in range(max_iterations):
                # Get a batch of restaurants from the API
                load_queue = self.restaurant_api_reader.get_restaurants(limit, last_loaded_id)

                if not load_queue:
                    self.logger.info("Quitting.")
                    break

                # Process each restaurant in the batch
                for restaurant in load_queue:
                    try:
                        self.restaurant_repository.insert_entry(conn, restaurant)
                    except Exception as e:
                        self.logger.error(f"Error inserting restaurant {restaurant._id}: {e}")

                # Update the last loaded ID with the maximum ID in the current batch
                last_loaded_id = max([t._id for t in load_queue])
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = last_loaded_id

            # Save the updated settings to the database
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.logger.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
