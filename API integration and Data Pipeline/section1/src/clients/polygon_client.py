import json
from pathlib import Path
from typing import Iterable, Dict, Any

import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)

from src.utils.logging import init_logger
from src.config import settings

logger = init_logger(__name__)

class PolygonClient:
    """
    Fetch OHLCV aggregates from the Polygon Stocks API or bundled sample files.

    Depending on the configured mode (`sample` or `rest`), the client either reads
    local JSON files containing historical aggregates or performs HTTP requests to
    the Polygon.io API to retrieve the same data.

    Attributes:
        mode (str): Data source mode. Can be "sample" (local JSON files) or "rest" (Polygon API).
        api_key (str | None): API key for accessing the Polygon REST API.
    """

    def __init__(self, mode: str | None = None, api_key: str | None = None):
        """
        Initialize the PolygonClient.

        Args:
            mode (str | None): Optional override for mode. Defaults to value in `settings.polygon_mode`.
            api_key (str | None): Optional override for the Polygon API key. Defaults to value in `settings.polygon_api_key`.
        """
        self.mode = mode or settings.polygon_mode
        self.api_key = api_key or settings.polygon_api_key

    def _sample_path(self, ticker: str, start: str, end: str) -> Path:
        """
        Construct the local file path for a sample aggregate JSON file.

        Args:
            ticker (str): Stock ticker symbol.
            start (str): Start date (e.g. "2024-01-01").
            end (str): End date (e.g. "2024-01-31").

        Returns:
            Path: Path to the sample JSON file in the data directory.
        """
        fname = f"polygon_aggs_{ticker.lower()}_{start}_{end}.json"
        return settings.data_dir / "samples" / fname

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=1, max=30),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def _rest_aggs(self, ticker: str, start: str, end: str, timespan: str) -> Dict[str, Any]:
        """
        Fetch OHLCV aggregate data from the Polygon.io REST API.

        This method retries on network failures using exponential backoff.

        Args:
            ticker (str): Stock ticker symbol.
            start (str): Start date in YYYY-MM-DD format.
            end (str): End date in YYYY-MM-DD format.
            timespan (str): Aggregation timespan (e.g., "day", "minute").

        Returns:
            dict: JSON response from the Polygon API containing aggregate data.

        Raises:
            requests.RequestException: If the request fails after retries.
        """
        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/{timespan}/"
            f"{start}/{end}?adjusted=true&sort=asc&limit=50000&apiKey={self.api_key}"
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def list_aggs(self, ticker: str, start: str, end: str, timespan: str = "day") -> Iterable[Dict[str, Any]]:
        """
        List OHLCV aggregate rows for the given ticker and date range.

        Depending on the configured mode, either reads from local sample JSON
        or fetches data from the Polygon REST API.

        Args:
            ticker (str): Stock ticker symbol.
            start (str): Start date in YYYY-MM-DD format.
            end (str): End date in YYYY-MM-DD format.
            timespan (str): Aggregation timespan (default is "day").

        Yields:
            dict: A dictionary representing one OHLCV row with keys like
                  "t" (timestamp), "o", "h", "l", "c", "v", "vw", etc.
        """
        if self.mode == "sample":
            path = self._sample_path(ticker, start, end)
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for row in data.get("results", []):
                yield row
        else:
            data = self._rest_aggs(ticker, start, end, timespan)
            for row in data.get("results", []):
                yield row
