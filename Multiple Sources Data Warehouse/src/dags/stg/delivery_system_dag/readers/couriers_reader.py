from typing import List
from dataclasses import dataclass
from logging import Logger
from urllib.parse import urljoin
import requests

@dataclass
class Courier:
    _id: str
    name: str

class CourierApiReader:
    BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

    def __init__(self, logger: Logger) -> None:
        """
        Initialize the CourierApiReader.

        Args:
            logger (Logger): Logger for logging messages.
        """
        self.logger = logger

    def get_couriers(self, limit: int, offset: int) -> List[Courier]:
        """
        Fetch couriers from the API.

        Args:
            limit (int): Number of couriers to fetch.
            offset (int): Offset for pagination.

        Returns:
            List[Courier]: List of Courier objects fetched from the API.
        """
        endpoint = urljoin(self.BASE_URL, "/couriers")
        
        params = {
            "sort_field": "id",
            "sort_direction": "asc",
            "limit": limit,
            "offset": offset,
        }

        headers = {
            "X-Nickname": "Bar Kotlyarov",
            "X-Cohort": "21",
            "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
        }

        try:
            response = requests.get(url=endpoint, params=params, headers=headers)
            response.raise_for_status()

            # Convert JSON response to list of Courier objects
            return [Courier(**courier) for courier in response.json()]
        except requests.RequestException as e:
            self.logger.error(f"Error during API request: {e}")
            
            # Re-raise the exception to propagate it up
            raise
