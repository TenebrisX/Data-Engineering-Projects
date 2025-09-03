"""PostgreSQL data saver utility for staging operations.

This module provides utilities for saving objects to PostgreSQL staging tables
with automatic serialization and conflict resolution.
"""

from datetime import datetime
from typing import Any
from psycopg import Connection
from lib.dict_util import json2str
import logging

class PgSaver:
    """Utility class for saving objects to PostgreSQL staging tables."""

    def save_object(self, conn: Connection, obj_id: str, update_ts: datetime, val: Any, table_name: str, schema_name: str) -> None:
        """Save an object to the PostgreSQL database with upsert functionality.

        Args:
            conn: PostgreSQL database connection.
            obj_id: Object ID for identification.
            update_ts: Timestamp of the object's last update.
            val: Object value to be serialized and stored.
            table_name: Target database table name.
            schema_name: Target database schema name.

        Raises:
            psycopg.Error: Database operation errors.
        """
        str_val = json2str(val)
        
        with conn.cursor() as cur:
            cur.execute(
              """
                INSERT INTO {schema_name}.{table_name}(object_id, object_value, update_ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
                """.format(schema_name=schema_name, table_name=table_name),
                        (obj_id, str_val, update_ts)
            )
        
        logging.info(f"Object with ID {obj_id} saved successfully to {schema_name}.{table_name}")