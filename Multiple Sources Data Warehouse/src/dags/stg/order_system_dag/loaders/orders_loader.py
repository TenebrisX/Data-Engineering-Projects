from datetime import datetime
from stg import EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from stg.order_system_dag.abstraction.mongo_abstract_loader import MongoLoader

class OrdersLoader(MongoLoader):
    """
    Concrete class for loading orders data from MongoDB to PostgreSQL.
    """

    WF_KEY = "ordersystem_orders_origin_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    
    def _get_or_create_workflow_setting(self, conn: PgConnect) -> EtlSetting:
        """
        Get or create the workflow setting for orders loading.

        Args:
            conn (PgConnect): The PostgreSQL connection.

        Returns:
            EtlSetting: The workflow setting.
        """
        wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
        if not wf_setting:
            wf_setting = EtlSetting(
                id=0,
                workflow_key=self.WF_KEY,
                workflow_settings={
                    self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                }
            )
        return wf_setting

    def _process_and_save_documents(self, conn: PgConnect, load_queue: list) -> None:
        """
        Process and save the orders documents.

        Args:
            conn (PgConnect): The PostgreSQL connection.
            load_queue (list): List of orders documents.
        """
        i = 0
        for d in load_queue:
            self.pg_saver.save_object(conn, str(d["_id"]), d["update_ts"], d, "ordersystem_" + self.doc_name, "stg")
            i += 1
            if i % self._LOG_THRESHOLD == 0:
                self.log.info(f"processed {i} documents of {len(load_queue)} while syncing {self.doc_name}.")

    def _update_workflow_setting(self, conn: PgConnect, wf_setting: EtlSetting, load_queue: list) -> None:
        """
        Update the workflow setting after loading.

        Args:
            conn (PgConnect): The PostgreSQL connection.
            wf_setting (EtlSetting): The workflow setting.
            load_queue (list): List of orders documents.
        """
        wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([t["update_ts"] for t in load_queue])
        wf_setting_json = json2str(wf_setting.workflow_settings)
        self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
        self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")
