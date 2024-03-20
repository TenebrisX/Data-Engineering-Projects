from typing import Dict
from stg.order_system_dag.abstraction.mongo_abstract_reader import MongoReader

class OrdersReader(MongoReader):
    """
    Concrete class for reading orders data from MongoDB.
    """

    def get_collection_name(self) -> str:
        """
        Get the name of the MongoDB collection for orders.

        Returns:
            str: The collection name.
        """
        return "orders"

    def transform_document(self, document: Dict) -> Dict:
        """
        Transform the orders document if needed.

        Args:
            document (Dict): The raw orders document.

        Returns:
            Dict: The transformed orders document.
        """
        return document
