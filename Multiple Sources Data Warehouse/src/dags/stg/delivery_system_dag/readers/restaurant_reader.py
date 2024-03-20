from typing import List
from dataclasses import dataclass
from logging import Logger
from urllib.parse import urljoin
import requests

@dataclass
class Restaurant:
    _id: str
    name: str

class RestaurantApiReader:
    BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

    def __init__(self, logger: Logger) -> None:
        """
        Initialize the RestaurantApiReader.

        Args:
            logger (Logger): Logger for logging messages.
        """
        self.logger = logger

    def get_restaurants(self, limit: int, offset: int) -> List[Restaurant]:
        """
        Fetch restaurants from the API.

        Args:
            limit (int): Number of restaurants to fetch.
            offset (int): Offset for pagination.

        Returns:
            List[Restaurant]: List of Restaurant objects fetched from the API.
        """
        endpoint = urljoin(self.BASE_URL, "/restaurants")
        
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
            
            # Convert JSON response to list of Restaurant objects
            return [Restaurant(**restaurant) for restaurant in response.json()]
        except requests.RequestException as e:
            self.logger.error(f"Error during API request: {e}")
            
            # Re-raise the exception to propagate it up
            raise
