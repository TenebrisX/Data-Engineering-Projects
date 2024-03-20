from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

class EtlSetting(BaseModel):
    """Pydantic model for ETL settings."""
    id: int
    workflow_key: str
    workflow_settings: Dict

class DdsEtlSettingsRepository:
    """Repository class for interacting with ETL settings in the database."""
    
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        """
        Retrieve ETL setting from the database.

        Args:
            conn (Connection): PostgreSQL database connection.
            etl_key (str): Key to identify the ETL workflow.

        Returns:
            Optional[EtlSetting]: EtlSetting instance if found, otherwise None.
        """
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM dds.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        """
        Save or update ETL setting in the database.

        Args:
            conn (Connection): PostgreSQL database connection.
            workflow_key (str): Key to identify the ETL workflow.
            workflow_settings (str): JSON string representing the workflow settings.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )
