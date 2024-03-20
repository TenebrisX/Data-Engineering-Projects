from psycopg import Connection
from pydantic import BaseModel
from datetime import datetime
from lib.pg_connect import PgConnect


class Restaurant(BaseModel):
    _id: str
    name: str


class RestaurantDest:
    def __init__(self, pg_connect: PgConnect) -> None:
        """
        Initialize RestaurantDest with a PostgreSQL connection.

        Args:
            pg_connect (PgConnect): PostgreSQL connection object.
        """
        self.pg_connect = pg_connect

    def insert_entry(self, conn: Connection, obj: Restaurant) -> None:
        """
        Insert or update a restaurant entry in the stg.deliverysystem_restaurants table.

        Args:
            conn (Connection): PostgreSQL connection object.
            obj (Restaurant): Restaurant object to be inserted or updated.
        """
        with conn.cursor() as cur:
            # SQL query to insert or update restaurant entry
            cur.execute(
                """
                INSERT INTO stg.deliverysystem_restaurants(object_id, object_value, update_ts)
                VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                ON CONFLICT (object_id) DO UPDATE
                SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
                """,
                {
                    "object_id": obj._id,
                    "object_value": obj.name,
                    "update_ts": datetime.now(),
                },
            )
