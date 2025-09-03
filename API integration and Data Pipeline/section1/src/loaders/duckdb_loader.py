from pathlib import Path
from typing import Iterable
import duckdb
import pandas as pd

from src.utils.logging import init_logger

logger = init_logger(__name__)

class DuckDBLoader:
    """Utility class for loading and inspecting data in a DuckDB data warehouse.

    This class provides methods to:
    - Connect to a DuckDB database.
    - Initialize the schema using an external SQL file.
    - Load Parquet files into temporary views.
    - Count rows in tables.
    - Preview table contents.
    - Log health summaries for specified tables.
    """

    def __init__(self, db_path: Path):
        """Initialize the DuckDB loader.

        Args:
            db_path (Path): Path to the DuckDB database file.
        """
        self.db_path = Path(db_path)

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Establish a connection to the DuckDB database.

        Returns:
            duckdb.DuckDBPyConnection: Active connection to DuckDB.
        """
        return duckdb.connect(str(self.db_path))

    def init_schema(self, schema_sql_path: Path) -> None:
        """Initialize the database schema using an external SQL file.

        Args:
            schema_sql_path (Path): Path to the `.sql` file defining the schema.
        """
        con = self.connect()
        try:
            logger.info("initializing_dw_schema", extra={"db_path": str(self.db_path), "schema_sql": str(schema_sql_path)})
            con.execute(Path(schema_sql_path).read_text(encoding="utf-8"))
        finally:
            con.close()

    def load_stage_parquet(self, con: duckdb.DuckDBPyConnection, name: str, parquet_path: Path) -> None:
        """Create a temporary view in DuckDB from a Parquet file.

        Args:
            con (duckdb.DuckDBPyConnection): Active DuckDB connection.
            name (str): Name of the temporary view to create.
            parquet_path (Path): Path to the Parquet file.

        Logs:
            - View name
            - Row count
            - Sample of first 3 rows
        """
        logger.info("loading_stage_parquet", extra={"name": name, "parquet_path": str(parquet_path)})
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS SELECT * FROM read_parquet('{parquet_path.as_posix()}');")
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        sample = con.execute(f"SELECT * FROM {name} LIMIT 3").fetch_df()
        logger.info("stage_view_ready", extra={"view": name, "row_count": int(cnt), "sample": sample.to_dict(orient="records")})


    # NOTE: This should migrate to tests later 
    def table_count(self, con: duckdb.DuckDBPyConnection, table: str) -> int:
        """Return the row count for a specified table.

        Args:
            con (duckdb.DuckDBPyConnection): Active DuckDB connection.
            table (str): Table name.

        Returns:
            int: Number of rows in the table.
        """
        return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def table_preview(self, con: duckdb.DuckDBPyConnection, table: str, limit: int = 5) -> pd.DataFrame:
        """Fetch a limited number of rows from a table for preview.

        Args:
            con (duckdb.DuckDBPyConnection): Active DuckDB connection.
            table (str): Table name.
            limit (int, optional): Number of rows to preview. Defaults to 5.

        Returns:
            pd.DataFrame: DataFrame containing the previewed rows.
        """
        return con.execute(f"SELECT * FROM {table} LIMIT {limit}").fetch_df()

    def log_table_health(self, con: duckdb.DuckDBPyConnection, tables: Iterable[str]) -> None:
        """Log row count and preview for a list of tables, or log errors if access fails.

        Args:
            con (duckdb.DuckDBPyConnection): Active DuckDB connection.
            tables (Iterable[str]): List of table names to inspect.
        """
        for t in tables:
            try:
                cnt = self.table_count(con, t)
                head = self.table_preview(con, t).to_dict(orient="records")
                logger.info("table_health", extra={"table": t, "row_count": cnt, "head": head})
            except Exception as e:
                logger.error("table_health_error", extra={"table": t, "error": str(e)})
                raise
