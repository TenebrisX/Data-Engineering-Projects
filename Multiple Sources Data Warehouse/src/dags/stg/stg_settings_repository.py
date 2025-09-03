"""STG layer ETL workflow settings repository.

This module provides repository classes for managing ETL workflow settings
and tracking processing state in the staging layer.
"""

from typing import Dict, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class EtlSetting(BaseModel):
    """Data model for ETL workflow settings."""
    id: int
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    """Repository for managing STG layer ETL workflow settings."""
    
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        """Retrieve ETL workflow setting from the database.

        Args:
            conn: Database connection.
            etl_key: Workflow key identifier.

        Returns:
            EtlSetting instance if found, None otherwise.
        """
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(workflow_key)s;
                """,
                {"workflow_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(self, conn: Connection, workflow_key: str, workflow_settings: str) -> None:
        """Save or update ETL workflow setting in the database.

        Args:
            conn: Database connection.
            workflow_key: Workflow key identifier.
            workflow_settings: JSON string of workflow settings.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {
                    "etl_key": workflow_key,
                    "etl_setting": workflow_settings
                },
            )