from pydantic import Field
from pydantic_settings import BaseSettings
from pathlib import Path


class Settings(BaseSettings):
    """
    Application configuration settings loaded from environment variables.

    This class uses Pydantic's `BaseSettings` to automatically parse environment
    variables prefixed with `APP_` into strongly typed attributes. It supports 
    both sample and REST API modes for Polygon.io data ingestion.

    Attributes:
        polygon_mode (str): Mode of operation. Must be either "sample" (for bundled JSON data)
            or "rest" (for real API calls to Polygon.io).
        polygon_api_key (str | None): API key for accessing the Polygon.io REST API.
            Required when `polygon_mode` is "rest".
        polygon_timespan (str): Time resolution for the Polygon API (e.g., "day", "minute").
        data_dir (Path): Base directory for input data.
        out_dir (Path): Output directory for transformed data and artifacts.
        duckdb_path (Path): Path to the DuckDB file used as a local data warehouse.

    Config:
        env_prefix (str): Environment variable prefix (`APP_`) for loading values.

    Example:
        Set environment variables like:

        ```bash
        export APP_POLYGON_MODE=rest
        export APP_POLYGON_API_KEY=your_api_key_here
        ```

        Then instantiate:

        ```python
        settings = Settings()
        print(settings.polygon_mode)
        ```
    """
    polygon_mode: str = Field(default="sample", pattern="^(sample|rest)$")
    polygon_api_key: str | None = None
    polygon_timespan: str = "day"
    data_dir: Path = Path("/data") # could be s3:// in prod
    out_dir: Path = Path("data/out") # could be s3:// in prod
    duckdb_path: Path = Path("data/stock_data.duckdb")

    class Config:
        env_prefix = "APP_"


settings = Settings()
