from typing import Dict
from stg.order_system_dag.abstraction.mongo_abstract_reader import MongoReader

class UsersReader(MongoReader):
    """
    Concrete class for reading user data from MongoDB.
    """

    def get_collection_name(self) -> str:
        """
        Get the name of the collection containing user data.

        Returns:
            str: The name of the MongoDB collection for users.
        """
        return "users"

    def transform_document(self, document: Dict) -> Dict:
        """
        Transform the user document if needed.

        Args:
            document (Dict): The original user document.

        Returns:
            Dict: The transformed user document.
        """
        return document