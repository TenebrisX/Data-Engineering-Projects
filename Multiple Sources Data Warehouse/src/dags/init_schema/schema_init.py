import os
from logging import Logger
from pathlib import Path

from lib import PgConnect

class SchemaDdl:
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        """
        Initialize SchemaDdl instance.

        Args:
            pg (PgConnect): PostgreSQL connection object.
            log (Logger): Logger object for logging.
        """
        self._db = pg
        self.log = log

    def init_schema(self, path_to_scripts: str) -> None:
        """
        Initialize the database schema by applying SQL scripts.

        Args:
            path_to_scripts (str): Path to the directory containing SQL scripts.
        """
        # List all files in the specified directory.
        files = os.listdir(path_to_scripts)
        # Create a list of file paths and sort them by name.
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} files to apply changes.")

        i = 1
        # Iterate through the sorted file paths.
        for fp in file_paths:
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            # Read the content of the SQL script.
            script = fp.read_text()

            # Execute the SQL script using a database connection.
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

            self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")
            i += 1
