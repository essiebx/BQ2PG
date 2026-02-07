"""Database connection pooling for performance."""

import logging
from typing import Dict, Any
from psycopg2 import pool

logger = logging.getLogger(__name__)


class ConnectionPool:
    """Manages PostgreSQL connection pooling."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_connections: int = 5,
        max_connections: int = 20,
    ):
        """Initialize connection pool.

        Args:
            host: Database host.
            port: Database port.
            database: Database name.
            user: Database user.
            password: Database password.
            min_connections: Minimum pool size.
            max_connections: Maximum pool size.
        """
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_connections = min_connections
        self.max_connections = max_connections

        try:
            self.pool = pool.SimpleConnectionPool(
                min_connections,
                max_connections,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
            logger.info(
                f"Connection pool initialized: "
                f"min={min_connections}, max={max_connections}"
            )
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool.

        Returns:
            Database connection.

        Raises:
            OperationalError: If no connections available.
        """
        try:
            return self.pool.getconn()
        except Exception as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise

    def return_connection(self, connection):
        """Return a connection to the pool.

        Args:
            connection: Connection to return.
        """
        try:
            self.pool.putconn(connection)
        except Exception as e:
            logger.error(f"Failed to return connection to pool: {e}")

    def close_all(self) -> None:
        """Close all connections in the pool."""
        try:
            self.pool.closeall()
            logger.info("Closed all connections in pool")
        except Exception as e:
            logger.error(f"Failed to close pool: {e}")

    def get_pool_status(self) -> Dict[str, Any]:
        """Get pool status information.

        Returns:
            Dictionary with pool statistics.
        """
        try:
            return {
                "min_connections": self.min_connections,
                "max_connections": self.max_connections,
                "closed_connections": self.pool.closed,
            }
        except Exception as e:
            logger.error(f"Failed to get pool status: {e}")
            return {}
