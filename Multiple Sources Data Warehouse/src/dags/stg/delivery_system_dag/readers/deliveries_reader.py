from typing import List
from dataclasses import dataclass
from logging import Logger
from urllib.parse import urljoin
from datetime import datetime, timedelta
from dateutil.parser import parse
import requests

@dataclass
class Delivery:
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    tip_sum: float
    sum: float

class DeliveryApiReader:
    BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

    def __init__(self, logger: Logger) -> None:
        """
        Initialize the DeliveryApiReader.

        Args:
            logger (Logger): Logger for logging messages.
        """
        self.logger = logger

    def get_deliveries(self, limit: int, offset: int, last_loaded_ts_str: str) -> List[Delivery]:
        """
        Fetch deliveries from the API.

        Args:
            limit (int): Number of deliveries to fetch.
            offset (int): Offset for pagination.
            last_loaded_ts_str (str): Last loaded timestamp as a string.

        Returns:
            List[Delivery]: List of Delivery objects fetched from the API.
        """
        endpoint = urljoin(self.BASE_URL, "/deliveries")
        
        # Calculate the 'from' timestamp based on the last loaded timestamp or a week ago
        date_from = max(parse(last_loaded_ts_str), (datetime.now() - timedelta(days=7)))
        
        params = {
            "sort_field": "date",
            "sort_direction": "asc",
            "from": date_from.strftime('%Y-%m-%d %H:%M:%S'),
            "to": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
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
            
            # Convert JSON response to list of Delivery objects
            return [Delivery(**delivery) for delivery in response.json()]
        except requests.RequestException as e:
            self.logger.error(f"Error during API request: {e}")
            
            # Re-raise the exception to propagate it up
            raise
