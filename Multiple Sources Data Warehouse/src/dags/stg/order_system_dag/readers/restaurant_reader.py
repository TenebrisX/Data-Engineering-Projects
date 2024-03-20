from typing import Dict
from stg.order_system_dag.abstraction.mongo_abstract_reader import MongoReader

class RestaurantReader(MongoReader):
    """
    Concrete class for reading restaurant data from MongoDB.
    """

    def get_collection_name(self) -> str:
        """
        Get the name of the collection containing restaurant data.

        Returns:
            str: The name of the MongoDB collection for restaurants.
        """
        return "restaurants"

    def transform_document(self, document: Dict) -> Dict:
        """
        Transform the restaurant document if needed.

        Args:
            document (Dict): The original restaurant document.

        Returns:
            Dict: The transformed restaurant document.
        """
        return document

