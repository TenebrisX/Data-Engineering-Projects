from typing import Dict, Any
import requests
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type
)
from src.utils.logging import init_logger

logger = init_logger(__name__)

class FrankfurterClient:
    """
    A minimal client for the [frankfurter.dev](https://www.frankfurter.app/) currency exchange API.

    This client supports querying:
    - Time series exchange rates between a base and multiple target currencies.
    - A full list of supported currencies and their names.

    No API key is required. Requests are retried on network failures using exponential backoff.
    """

    BASE = "https://api.frankfurter.dev/v1"

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=1, max=30),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def time_series(
        self,
        start: str,
        end: str,
        base: str = "EUR",
        symbols: list[str] | None = None
    ) -> Dict[str, Any]:
        """
        Fetch historical exchange rate time series from Frankfurter API.

        Args:
            start (str): Start date in ISO format (YYYY-MM-DD).
            end (str): End date in ISO format (YYYY-MM-DD).
            base (str): Base currency code (default is "EUR").
            symbols (list[str] | None): List of target currency codes to compare against the base.

        Returns:
            Dict[str, Any]: Parsed JSON response with daily exchange rates.

        Raises:
            requests.RequestException: On failure to connect or bad HTTP response.
        """
        params = {"base": base}
        if symbols:
            params["symbols"] = ",".join(symbols)
        url = f"{self.BASE}/{start}..{end}"
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(min=1, max=30),
        retry=retry_if_exception_type(requests.RequestException)
    )
    def currencies(self) -> Dict[str, str]:
        """
        Retrieve a dictionary of supported currencies from Frankfurter API.

        Returns:
            Dict[str, str]: A mapping of currency codes to their full names.

        Raises:
            requests.RequestException: On failure to connect or bad HTTP response.
        """
        url = f"{self.BASE}/currencies"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
