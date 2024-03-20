from typing import Optional
from pydantic import BaseModel
from datetime import datetime
from psycopg import Connection
from lib.dict_util import json2str

from lib.pg_connect import PgConnect


class Delivery(BaseModel):
    delivery_id: str
    object_value: str
    delivery_ts: datetime
    update_ts: datetime


class DeliveryDest:
    def __init__(self, pg_connect: PgConnect) -> None:
        """
        Initialize DeliveryDest with a PostgreSQL connection.

        Args:
            pg_connect (PgConnect): PostgreSQL connection object.
        """
        self.pg_connect = pg_connect

    def insert_entry(self, conn: Connection, obj: Delivery) -> None:
        """
        Insert or update a delivery entry in the stg.deliverysystem_deliveries table.

        Args:
            conn (Connection): PostgreSQL connection object.
            obj (Delivery): Delivery object to be inserted or updated.
        """
        with conn.cursor() as cur:
            # SQL query to insert or update delivery entry
            cur.execute(
                """
                INSERT INTO stg.deliverysystem_deliveries
                (object_id, object_value, delivery_ts, update_ts)
                VALUES (%(object_id)s, %(object_value)s, %(delivery_ts)s, %(update_ts)s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    delivery_ts = EXCLUDED.delivery_ts,
                    update_ts = EXCLUDED.update_ts;
                """,
                {
                    "object_id": obj.delivery_id,
                    "object_value": json2str(obj),
                    "delivery_ts": obj.delivery_ts,
                    "update_ts": datetime.now(),
                },
            )

    def get_max_delivery_ts(self, conn: Connection) -> Optional[Delivery]:
        """
        Retrieve the maximum delivery timestamp from the stg.deliverysystem_deliveries table.

        Args:
            conn (Connection): PostgreSQL connection object.

        Returns:
            Optional[Delivery]: Delivery object with the maximum delivery timestamp or None if no records.
        """
        with conn.cursor(row_factory=Delivery) as cur:
            # SQL query to get the maximum delivery timestamp
            cur.execute(
                """
                SELECT max(delivery_ts) last_ts
                FROM stg.deliverysystem_deliveries
                """
            )
            return cur.fetchone()
