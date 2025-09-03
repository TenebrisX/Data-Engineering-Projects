"""MongoDB connection utilities for data warehouse operations.

This module provides connection management utilities for MongoDB databases,
including SSL certificate handling and connection string generation.
"""

from urllib.parse import quote_plus as quote

from pymongo.mongo_client import MongoClient


class MongoConnect:
    """MongoDB connection manager with SSL certificate and replica set support."""
    
    def __init__(self,
                 cert_path: str,
                 user: str,
                 pw: str,
                 host: str,
                 rs: str,
                 auth_db: str,
                 main_db: str
                 ) -> None:
        """Initialize MongoDB connection parameters.
        
        Args:
            cert_path: Path to SSL certificate file.
            user: MongoDB username.
            pw: MongoDB password.
            host: MongoDB host address.
            rs: Replica set name.
            auth_db: Authentication database name.
            main_db: Main database name.
        """
        self.user = user
        self.pw = pw
        self.host = host
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self) -> str:
        """Generate MongoDB connection URL string.
        
        Returns:
            Formatted MongoDB connection string.
        """
        return 'mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}'.format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=self.host,
            rs=self.replica_set,
            auth_src=self.auth_db)

    def client(self):
        """Create a MongoDB client connection.
        
        Returns:
            pymongo.database.Database: MongoDB database object.
        """
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]