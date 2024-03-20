from datetime import datetime
from typing import Any
from psycopg import Connection
from lib.dict_util import json2str
import logging

class PgSaver:

    def save_object(self, conn: Connection, obj_id: str, update_ts: datetime, val: Any, table_name: str, schema_name: str) -> None:
        """
        Save an object to the PostgreSQL database.

        Parameters:
            conn (Connection): PostgreSQL database connection.
            obj_id (str): Object ID.
            update_ts (datetime): Timestamp of the object's last update.
            val (Any): Object value.
            doc_name (str): Document name for the database table.

        Returns:
            None
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
