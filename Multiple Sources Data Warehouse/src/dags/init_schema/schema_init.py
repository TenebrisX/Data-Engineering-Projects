"""Database schema initialization utilities.

This module provides utilities for initializing database schemas by applying
SQL DDL scripts in the correct order for data warehouse layers.
"""

import os
from logging import Logger
from pathlib import Path

from lib import PgConnect

class SchemaDdl:
    """Database schema DDL executor for initializing data warehouse schemas."""
    
    def __init__(self, pg: PgConnect, log: Logger) -> None:
        """Initialize SchemaDdl instance.

        Args:
            pg: PostgreSQL connection object.
            log: Logger object for logging operations.
        """
        self._db = pg
        self.log = log

    def init_schema(self, path_to_scripts: str) -> None:
        """Initialize database schema by applying SQL scripts from directory.

        Reads all SQL files from the specified directory, sorts them by filename,
        and executes them in order to create the database schema structure.

        Args:
            path_to_scripts: Path to directory containing SQL DDL scripts.

        Raises:
            FileNotFoundError: If the script directory doesn't exist.
            psycopg.Error: Database execution errors.
        """
        files = os.listdir(path_to_scripts)
        file_paths = [Path(path_to_scripts, f) for f in files]
        file_paths.sort(key=lambda x: x.name)

        self.log.info(f"Found {len(file_paths)} files to apply changes.")

        i = 1
        for fp in file_paths:
            self.log.info(f"Iteration {i}. Applying file {fp.name}")
            script = fp.read_text()

            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(script)

            self.log.info(f"Iteration {i}. File {fp.name} executed successfully.")
            i += 1