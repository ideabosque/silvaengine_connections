"""
Neo4j Connection Pool Implementation

Neo4j graph database connection pool implementation based on neo4j-python-driver, supporting hot-plugging.
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, TypeVar

from neo4j import Driver, GraphDatabase, Session, Transaction
from neo4j.exceptions import AuthError, ClientError, Neo4jError, ServiceUnavailable

from ..config import ConnectionConfig
from ..connection import BaseConnection
from ..connection_pool import BaseConnectionPool
from ..exceptions import (
    AuthenticationError,
    ConfigValidationError,
    ConnectionError,
    ConnectionTimeoutError,
    HealthCheckError,
    PoolError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Driver)


class Neo4jConnection(BaseConnection[Driver]):
    """
    Neo4j Connection Wrapper Class

    Uses Neo4j Python Driver as the underlying connection object, providing connection management and health check functionality.

    Attributes:
        _driver: Neo4j driver instance
        _uri: Connection URI
        _auth: Authentication info tuple (username, password)
        _config: Driver configuration parameters
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize Neo4j Connection

        Args:
            config: Connection configuration dictionary, containing the following key fields:
                - uri: Neo4j connection URI (bolt://host:port or neo4j://host:port)
                - host: Database host address (alternative to uri)
                - port: Database port (default: 7687)
                - username: Username
                - password: Password
                - database: Default database name (optional)
                - encrypted: Whether to use encrypted connection (default: True)
                - trust: Certificate trust policy (optional)
                - connection_timeout: Connection timeout in seconds
                - max_connection_lifetime: Maximum connection lifetime in seconds
        """
        super().__init__(config)
        self._driver: Optional[Driver] = None
        self._uri = self._build_uri()
        self._auth = self._build_auth()
        self._driver_config = self._build_driver_config()
        self._default_database = config.get("database")

    def _build_uri(self) -> str:
        """
        Build Neo4j Connection URI

        Returns:
            Neo4j connection URI string

        Raises:
            ConfigValidationError: When required configuration is missing
        """
        # Prioritize directly provided uri
        if "uri" in self._config:
            return self._config["uri"]

        # Otherwise build from host and port
        host = self._config.get("host")
        port = self._config.get("port", 7687)
        scheme = self._config.get("scheme", "bolt")

        if not host:
            raise ConfigValidationError(
                "Neo4j configuration missing required field: uri or host",
                config_key="neo4j.connection",
            )

        return f"{scheme}://{host}:{port}"

    def _build_auth(self) -> tuple:
        """
        Build Authentication Information

        Returns:
            (username, password) tuple

        Raises:
            ConfigValidationError: When authentication information is missing
        """
        username = self._config.get("username")
        password = self._config.get("password")

        if not username or not password:
            raise ConfigValidationError(
                "Neo4j configuration missing authentication info: username, password",
                config_key="neo4j.connection.auth",
            )

        return (username, password)

    def _build_driver_config(self) -> Dict[str, Any]:
        """
        Build Driver Configuration

        Returns:
            Driver configuration dictionary
        """
        config = {}

        # Encryption settings
        if "encrypted" in self._config:
            config["encrypted"] = self._config["encrypted"]

        # Trust policy
        if "trust" in self._config:
            config["trust"] = self._config["trust"]

        # Connection timeout
        if "connection_timeout" in self._config:
            config["connection_acquisition_timeout"] = self._config[
                "connection_timeout"
            ]

        # Maximum connection lifetime
        if "max_connection_lifetime" in self._config:
            config["max_connection_lifetime"] = self._config["max_connection_lifetime"]

        # Connection pool settings
        if "max_connection_pool_size" in self._config:
            config["max_connection_pool_size"] = self._config[
                "max_connection_pool_size"
            ]

        if "connection_pool_min_size" in self._config:
            config["connection_pool_min_size"] = self._config[
                "connection_pool_min_size"
            ]

        # Other driver parameters
        if "driver_kwargs" in self._config:
            config.update(self._config["driver_kwargs"])

        return config

    def connect(self) -> Driver:
        """
        Establish Database Connection

        Returns:
            Neo4j Driver instance

        Raises:
            ConnectionError: When connection fails
            AuthenticationError: When authentication fails
        """
        try:
            self._driver = GraphDatabase.driver(
                self._uri, auth=self._auth, **self._driver_config
            )

            # Verify connection
            self._driver.verify_connectivity()

            self._is_closed = False
            logger.debug(
                f"Neo4j connection established [connection_id={self._connection_id}, uri={self._uri}]"
            )
            return self._driver

        except AuthError as e:
            raise AuthenticationError(
                f"Neo4j authentication failed: {str(e)}",
                connection_type="neo4j",
                original_error=e,
            )
        except ServiceUnavailable as e:
            raise ConnectionError(
                f"Neo4j service unavailable: {str(e)}",
                connection_type="neo4j",
                original_error=e,
            )
        except Neo4jError as e:
            raise ConnectionError(
                f"Neo4j connection failed: {str(e)}",
                connection_type="neo4j",
                original_error=e,
            )

    def close(self) -> None:
        """
        Close Database Connection
        """
        if self._driver and not self._is_closed:
            try:
                self._driver.close()
                self._is_closed = True
                logger.debug(f"Neo4j connection closed [connection_id={self._connection_id}]")
            except Neo4jError as e:
                logger.warning(
                    f"Error closing Neo4j connection [connection_id={self._connection_id}]: {e}"
                )

    def is_healthy(self) -> bool:
        """
        Check Connection Health Status

        Returns:
            True if connection is healthy, False if connection is abnormal
        """
        if not self._driver or self._is_closed:
            return False

        try:
            # Try to verify connection
            self._driver.verify_connectivity()
            return True
        except Exception:
            return False

    def session(self, database: Optional[str] = None, **kwargs) -> Session:
        """
        Get Session

        Args:
            database: Database name, defaults to configured database
            **kwargs: Other session parameters

        Returns:
            Neo4j session object

        Raises:
            ConnectionError: When connection is not established
        """
        if not self._driver or self._is_closed:
            raise ConnectionError(
                "Connection not established or already closed", connection_type="neo4j"
            )

        db = database or self._default_database

        return self._driver.session(database=db, **kwargs)

    def run_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Execute Cypher Query

        Args:
            query: Cypher query statement
            parameters: Query parameters
            database: Target database

        Returns:
            Query result list

        Raises:
            ConnectionError: When execution fails
        """
        if not self._driver or self._is_closed:
            raise ConnectionError(
                "Connection not established or already closed", connection_type="neo4j"
            )

        try:
            with self.session(database=database) as session:
                result = session.run(query, parameters or {})
                return [record.data() for record in result]
        except Neo4jError as e:
            raise ConnectionError(
                f"Cypher query execution failed: {str(e)}",
                connection_type="neo4j",
                original_error=e,
            )

    def run_transaction(
        self, transaction_function, database: Optional[str] = None
    ) -> Any:
        """
        Execute Transaction

        Args:
            transaction_function: Transaction function, receives tx parameter
            database: Target database

        Returns:
            Transaction execution result
        """
        if not self._driver or self._is_closed:
            raise ConnectionError(
                "Connection not established or already closed", connection_type="neo4j"
            )

        with self.session(database=database) as session:
            return session.execute_write(transaction_function)

    @property
    def driver(self) -> Optional[Driver]:
        """Get Neo4j driver instance"""
        return self._driver

    @property
    def uri(self) -> str:
        """Get connection URI"""
        return self._uri


class Neo4jConnectionPool(BaseConnectionPool[Neo4jConnection]):
    """
    Neo4j Connection Pool Implementation

    Neo4j-specific connection pool implementation based on the base connection pool, providing connection management and monitoring functionality.

    Attributes:
        _name: Connection pool name
        _config: Connection configuration
        _health_check_interval: Health check interval (seconds)
        _last_health_check: Last health check time
    """

    def __init__(
        self,
        name: str,
        config: ConnectionConfig,
        min_size: int = 2,
        max_size: int = 10,
        max_idle_time: float = 300.0,
        max_lifetime: float = 3600.0,
        health_check_interval: float = 60.0,
    ) -> None:
        """
        Initialize Neo4j Connection Pool

        Args:
            name: Connection pool unique identifier name
            config: Connection configuration object
            min_size: Minimum number of connections
            max_size: Maximum number of connections
            max_idle_time: Maximum idle time (seconds)
            max_lifetime: Maximum connection lifetime (seconds)
            health_check_interval: Health check interval (seconds)
        """
        # Build connection configuration dictionary
        conn_config = {
            "uri": config.settings.get("uri"),
            "host": config.settings.get("host"),
            "port": config.settings.get("port", 7687),
            "scheme": config.settings.get("scheme", "bolt"),
            "username": config.settings.get("username"),
            "password": config.settings.get("password"),
            "database": config.settings.get("database"),
            "encrypted": config.settings.get("encrypted", True),
            "trust": config.settings.get("trust"),
            "connection_timeout": config.settings.get("connection_timeout", 30),
            "max_connection_lifetime": config.settings.get(
                "max_connection_lifetime", 3600
            ),
            "max_connection_pool_size": max_size,
            "connection_pool_min_size": min_size,
            "driver_kwargs": config.settings.get("driver_kwargs", {}),
        }

        super().__init__(
            name=name,
            connection_class=Neo4jConnection,
            min_size=min_size,
            max_size=max_size,
            max_idle_time=max_idle_time,
            max_lifetime=max_lifetime,
        )

        self._config = conn_config
        self._health_check_interval = health_check_interval
        self._last_health_check = 0.0
        self._last_health_status: Dict[str, Any] = {}
        self._default_database = config.settings.get("database")

    def get_pool_type(self) -> str:
        """Get pool type identifier."""
        return "neo4j"

    def _create_connection(self) -> Neo4jConnection:
        """
        Create New Neo4j Connection

        Returns:
            Newly created Neo4j connection instance

        Raises:
            PoolError: When connection creation fails
        """
        try:
            connection = Neo4jConnection(self._config)
            connection.connect()
            return connection
        except ConnectionError:
            raise
        except Exception as e:
            raise PoolError(
                f"Failed to create Neo4j connection: {str(e)}",
                pool_name=self._name,
                original_error=e,
            )

    def _validate_connection(self, connection: Neo4jConnection) -> bool:
        """
        Validate Connection Validity

        Args:
            connection: Connection to be validated

        Returns:
            True if connection is valid, False if invalid
        """
        return connection.is_healthy()

    def _destroy_connection(self, connection: Neo4jConnection) -> None:
        """
        Destroy Connection

        Args:
            connection: Connection to be destroyed
        """
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Error destroying Neo4j connection: {e}")

    def health_check(self) -> Dict[str, Any]:
        """
        Perform Connection Pool Health Check

        Returns:
            Health check result dictionary
        """
        current_time = time.time()

        # Check if health check needs to be performed
        if current_time - self._last_health_check < self._health_check_interval:
            return self._last_health_status

        with self._lock:
            unhealthy_count = 0

            # Check active connections
            for conn_id, connection in list(self._active_connections.items()):
                if not connection.is_healthy():
                    unhealthy_count += 1

            # Check idle connections
            idle_connection = []

            while not self._idle_connections.empty():
                try:
                    connection = self._idle_connections.get_nowait()
                    if connection.is_healthy():
                        idle_connection.append(connection)
                    else:
                        unhealthy_count += 1
                        self._destroy_connection(connection)
                except Exception:
                    break

            # Put healthy connections back into the queue
            for connection in idle_connection:
                try:
                    self._idle_connections.put_nowait(connection)
                except Exception:
                    self._destroy_connection(connection)

            status = {
                "status": "unhealthy" if unhealthy_count > 0 else "healthy",
                "active_connections": len(self._active_connections),
                "idle_connections": self._idle_connections.qsize(),
                "total_connections": len(self._active_connections)
                + self._idle_connections.qsize(),
                "unhealthy_connections": unhealthy_count,
                "pool_name": self._name,
                "connection_type": "neo4j",
            }

            self._last_health_check = current_time
            self._last_health_status = status

            if unhealthy_count > 0:
                logger.warning(
                    f"Neo4j connection pool health check found {unhealthy_count} unhealthy connections"
                )

            return status

    def get_stats(self) -> Dict[str, Any]:
        """
        Get Connection Pool Statistics

        Returns:
            Statistics dictionary
        """
        with self._lock:
            return {
                "pool_name": self._name,
                "connection_type": "neo4j",
                "min_size": self._min_size,
                "max_size": self._max_size,
                "active_connections": len(self._active_connections),
                "idle_connections": self._idle_connections.qsize(),
                "total_connections": len(self._active_connections)
                + self._idle_connections.qsize(),
                "wait_timeout": self._wait_timeout,
                "max_idle_time": self._max_idle_time,
                "max_lifetime": self._max_lifetime,
                "health_check_interval": self._health_check_interval,
            }

    @contextmanager
    def session(self, database: Optional[str] = None, **kwargs) -> Iterator[Session]:
        """
        Session Context Manager

        Provides convenient session usage

        Example:
            with pool.session() as session:
                result = session.run("MATCH (n) RETURN n LIMIT 10")
        """
        connection = self.acquire()
        session = None

        try:
            db = database or self._default_database
            session = connection.session(database=db, **kwargs)
            yield session
        finally:
            if session:
                session.close()
            self.release(connection)

    def run_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        database: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Convenient Query Method

        Automatically acquire connection, execute query, and release connection

        Args:
            query: Cypher query statement
            parameters: Query parameters
            database: Target database

        Returns:
            Query result list
        """
        with self.session(database=database) as session:
            result = session.run(query, parameters or {})
            return [record.data() for record in result]

    def run_transaction(
        self, transaction_function, database: Optional[str] = None
    ) -> Any:
        """
        Convenient Transaction Method

        Automatically acquire connection, execute transaction, and release connection

        Args:
            transaction_function: Transaction function
            database: Target database

        Returns:
            Transaction execution result
        """
        connection = self.acquire()
        try:
            return connection.run_transaction(transaction_function, database=database)
        finally:
            self.release(connection)


# Register Neo4j Connection Type
from ..plugin_registry import PluginRegistry


def register_neo4j_plugin(registry: PluginRegistry) -> None:
    """
    Register Neo4j Plugin to Registry

    Args:
        registry: Plugin registry instance
    """
    registry.register(
        type_name='neo4j',
        pool_class=Neo4jConnectionPool,
        connection_class=Neo4jConnection
    )
    logger.info("Neo4j plugin registered")


# Alias for backward compatibility
Neo4jPool = Neo4jConnectionPool
