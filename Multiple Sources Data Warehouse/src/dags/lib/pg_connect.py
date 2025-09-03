"""PostgreSQL connection utilities for data warehouse operations.

This module provides connection management utilities for PostgreSQL databases,
including connection pooling and configuration management.
"""

from contextlib import contextmanager
from typing import Generator

import psycopg
from airflow.hooks.base import BaseHook


class PgConnect:
    """PostgreSQL connection manager with configuration and connection pooling."""
    
    def __init__(self, host: str, port: str, db_name: str, user: str, pw: str, sslmode: str = "require") -> None:
        """Initialize PostgreSQL connection parameters.
        
        Args:
            host: Database host address.
            port: Database port number.
            db_name: Database name.
            user: Database user.
            pw: Database password.
            sslmode: SSL connection mode. Defaults to "require".
        """
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        """Generate PostgreSQL connection URL string.
        
        Returns:
            Formatted connection string.
        """
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode)

    def client(self):
        """Create a PostgreSQL client connection.
        
        Returns:
            psycopg.Connection: Database connection object.
        """
        return psycopg.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        """Context manager for database connections with automatic transaction handling.
        
        Yields:
            psycopg.Connection: Database connection with automatic commit/rollback.
            
        Raises:
            Exception: Any database operation exception.
        """
        conn = psycopg.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class ConnectionBuilder:
    """Factory class for building database connections from Airflow configurations."""

    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        """Build PostgreSQL connection from Airflow connection configuration.
        
        Args:
            conn_id: Airflow connection ID.
            
        Returns:
            PgConnect: Configured PostgreSQL connection object.
        """
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(str(conn.host),
                       str(conn.port),
                       str(conn.schema),
                       str(conn.login),
                       str(conn.password),
                       sslmode)

        return pg