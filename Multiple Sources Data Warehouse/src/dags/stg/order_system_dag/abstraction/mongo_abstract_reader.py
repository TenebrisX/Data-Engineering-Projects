from datetime import datetime
from typing import Dict, List
from abc import ABC, abstractmethod
from lib import MongoConnect

class MongoReader(ABC):
    """
    Abstract base class for reading data from MongoDB.
    """

    def __init__(self, mc: MongoConnect) -> None:
        """
        Constructor for MongoReader.

        Args:
            mc (MongoConnect): The MongoDB connection.
        """
        self.dbs = mc.client()

    @abstractmethod
    def get_collection_name(self) -> str:
        """
        Abstract method to get the name of the MongoDB collection.

        Returns:
            str: The name of the collection.
        """
        pass

    @abstractmethod
    def transform_document(self, document: Dict) -> Dict:
        """
        Abstract method to transform a document.

        Args:
            document (Dict): The document to transform.

        Returns:
            Dict: The transformed document.
        """
        pass

    def get_documents(self, load_threshold: datetime, limit: int) -> List[Dict]:
        """
        Get documents from MongoDB based on load threshold and limit.

        Args:
            load_threshold (datetime): The threshold for loading documents.
            limit (int): The maximum number of documents to load.

        Returns:
            List[Dict]: The list of documents.
        """
        filter = {'update_ts': {'$gt': load_threshold}}
        sort = [('update_ts', 1)]
        docs = list(self.dbs.get_collection(self.get_collection_name()).find(filter=filter, sort=sort, limit=limit))
        return [self.transform_document(doc) for doc in docs]
