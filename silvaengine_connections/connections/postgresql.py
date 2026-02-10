"""
PostgreSQL Connection Pool Implementation

PostgreSQL connection pool implementation based on SQLAlchemy, supporting hot-plugging.
"""

import logging
import time
from typing import Any, Dict, Optional, TypeVar
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, OperationalError, TimeoutError as SATimeoutError
from sqlalchemy.pool import QueuePool, NullPool

from ..connection import BaseConnection
from ..connection_pool import BaseConnectionPool
from ..config import ConnectionConfig
from ..exceptions import (
    ConnectionError,
    ConnectionTimeoutError,
    PoolError,
    HealthCheckError,
    ConfigValidationError
)

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=Engine)


class PostgreSQLConnection(BaseConnection[Engine]):
    """
    PostgreSQL Connection Wrapper Class

    Uses SQLAlchemy Engine as the underlying connection object, providing connection management and health check functionality.

    Attributes:
        _engine: SQLAlchemy engine instance
        _connection_url: Database connection URL
        _pool_config: Connection pool configuration parameters
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize PostgreSQL Connection

        Args:
            config: Connection configuration dictionary, containing the following key fields:
                - host: Database host address
                - port: Database port (default: 5432)
                - database: Database name
                - username: Username
                - password: Password
                - ssl_mode: SSL mode (optional)
                - connect_args: Additional connection parameters (optional)
        """
        super().__init__(config)
        self._engine: Optional[Engine] = None
        self._connection_url = self._build_connection_url()
        self._pool_config = config.get('pool', {})

    def _build_connection_url(self) -> str:
        """
        Build SQLAlchemy Connection URL

        Returns:
            PostgreSQL connection URL string

        Raises:
            ConfigValidationError: When required configuration is missing
        """
        host = self._config.get('host')
        port = self._config.get('port', 5432)
        database = self._config.get('database')
        username = self._config.get('username')
        password = self._config.get('password')

        if not all([host, database, username, password]):
            raise ConfigValidationError(
                "PostgreSQL configuration missing required fields: host, database, username, password",
                config_key="postgresql.connection"
            )

        # Build connection URL
        url = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"

        # Add SSL parameters
        ssl_mode = self._config.get('ssl_mode')
        if ssl_mode:
            url += f"?sslmode={ssl_mode}"

        return url

    def connect(self) -> Engine:
        """
        Establish Database Connection

        Returns:
            SQLAlchemy Engine instance

        Raises:
            ConnectionError: When connection fails
        """
        try:
            connect_args = self._config.get('connect_args', {})

            # Configure connection pool parameters
            pool_class = NullPool if self._config.get('disable_pool') else QueuePool
            pool_kwargs = {
                'poolclass': pool_class,
                'pool_pre_ping': True,  # Ping check before connection
                'pool_recycle': self._pool_config.get('recycle', 3600),  # Connection recycle time
            }

            # Add pool size parameters only for non-NullPool
            if pool_class == QueuePool:
                pool_kwargs.update({
                    'pool_size': self._pool_config.get('size', 5),
                    'max_overflow': self._pool_config.get('max_overflow', 10),
                    'pool_timeout': self._pool_config.get('timeout', 30),
                })

            self._engine = create_engine(
                self._connection_url,
                connect_args=connect_args,
                **pool_kwargs
            )

            # Verify connection
            with self._engine.connect() as connection:
                connection.execute(text("SELECT 1"))

            self._is_closed = False
            logger.debug(f"PostgreSQL connection established [connection_id={self._connection_id}]")
            return self._engine

        except SQLAlchemyError as e:
            raise ConnectionError(
                f"PostgreSQL connection failed: {str(e)}",
                details={"connection_type": "postgresql", "original_error": str(e)}
            )

    def close(self) -> None:
        """
        Close Database Connection
        """
        if self._engine and not self._is_closed:
            try:
                self._engine.dispose()
                self._is_closed = True
                logger.debug(f"PostgreSQL connection closed [connection_id={self._connection_id}]")
            except SQLAlchemyError as e:
                logger.warning(f"Error closing PostgreSQL connection [connection_id={self._connection_id}]: {e}")

    def is_healthy(self) -> bool:
        """
        Check Connection Health Status

        Returns:
            True if connection is healthy, False if connection is abnormal
        """
        if not self._engine or self._is_closed:
            return False

        try:
            with self._engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                return result.scalar() == 1
        except SQLAlchemyError:
            return False

    def execute(self, query: str, settings: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute SQL Query

        Args:
            query: SQL query statement
            settings: Query parameters

        Returns:
            Query result

        Raises:
            ConnectionError: When execution fails
        """
        if not self._engine or self._is_closed:
            raise ConnectionError(
                "Connection not established or already closed",
                details={"connection_type": "postgresql"}
            )

        try:
            with self._engine.connect() as connection:
                result = connection.execute(text(query), settings or {})
                connection.commit()
                return result
        except SQLAlchemyError as e:
            raise ConnectionError(
                f"SQL execution failed: {str(e)}",
                details={"connection_type": "postgresql", "original_error": str(e)}
            )

    def begin_transaction(self) -> Any:
        """
        Begin Transaction

        Returns:
            Transaction context manager
        """
        if not self._engine:
            raise ConnectionError(
                "Connection not established",
                details={"connection_type": "postgresql"}
            )
        return self._engine.begin()

    @property
    def engine(self) -> Optional[Engine]:
        """Get SQLAlchemy engine instance"""
        return self._engine

    @property
    def raw_connection(self) -> Optional[Any]:
        """Get raw database connection"""
        if self._engine:
            return self._engine.raw_connection()
        return None


class PostgreSQLConnectionPool(BaseConnectionPool[PostgreSQLConnection]):
    """
    PostgreSQL Connection Pool Implementation

    PostgreSQL-specific connection pool implementation based on the base connection pool, providing connection management and monitoring functionality.

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
        wait_timeout: float = 10.0,
        health_check_interval: float = 60.0,
        enable_dynamic_resize: bool = True,
    ) -> None:
        """
        Initialize PostgreSQL Connection Pool

        Args:
            name: Connection pool unique identifier name
            config: Connection configuration object
            min_size: Minimum number of connections
            max_size: Maximum number of connections
            max_idle_time: Maximum idle time (seconds)
            max_lifetime: Maximum connection lifetime (seconds)
            wait_timeout: Connection acquisition timeout (seconds)
            health_check_interval: Health check interval (seconds)
            enable_dynamic_resize: Whether to enable dynamic pool resizing
        """
        # Build connection configuration dictionary
        conn_config = {
            'host': config.settings.get('host'),
            'port': config.settings.get('port', 5432),
            'database': config.settings.get('database'),
            'username': config.settings.get('username'),
            'password': config.settings.get('password'),
            'ssl_mode': config.settings.get('ssl_mode'),
            'connect_args': config.settings.get('connect_args', {}),
            'pool': config.pool_settings,
        }

        # Store config before calling parent init (parent may call _create_connection)
        self._config = conn_config
        self._health_check_interval = health_check_interval
        self._last_health_check = 0.0
        self._last_health_status: Dict[str, Any] = {}

        super().__init__(
            name=name,
            connection_class=PostgreSQLConnection,
            min_size=min_size,
            max_size=max_size,
            max_idle_time=max_idle_time,
            max_lifetime=max_lifetime,
            wait_timeout=wait_timeout,
            health_check_interval=health_check_interval,
            enable_dynamic_resize=enable_dynamic_resize,
        )

    def get_pool_type(self) -> str:
        """Get pool type identifier."""
        return "postgresql"

    def _create_connection(self) -> PostgreSQLConnection:
        """
        Create New PostgreSQL Connection

        Returns:
            Newly created PostgreSQL connection instance

        Raises:
            PoolError: When connection creation fails
        """
        try:
            connection = PostgreSQLConnection(self._config)
            connection.connect()
            return connection
        except ConnectionError:
            raise
        except Exception as e:
            raise PoolError(
                f"Failed to create PostgreSQL connection: {str(e)}",
                details={"pool_name": self._name, "original_error": str(e)}
            )

    def _validate_connection(self, connection: PostgreSQLConnection) -> bool:
        """
        Validate Connection Validity

        Args:
            connection: Connection to be validated

        Returns:
            True if connection is valid, False if invalid
        """
        return connection.is_healthy()

    def _destroy_connection(self, connection: PostgreSQLConnection) -> None:
        """
        Destroy Connection

        Args:
            connection: Connection to be destroyed
        """
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Error destroying PostgreSQL connection: {e}")

    def health_check(self) -> Dict[str, Any]:
        """
        Perform Connection Pool Health Check

        Returns:
            Health check result dictionary, containing:
                - status: Health status (healthy/unhealthy)
                - active_connections: Number of active connections
                - idle_connections: Number of idle connections
                - total_connections: Total number of connections
                - unhealthy_connections: Number of unhealthy connections
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
            idle_conns = []
            while not self._idle_connections.empty():
                try:
                    connection = self._idle_connections.get_nowait()
                    
                    if connection.is_healthy():
                        idle_conns.append(connection)
                    else:
                        unhealthy_count += 1
                        self._destroy_connection(connection)
                except Exception:
                    break

            # Put healthy connections back into the queue
            for connection in idle_conns:
                try:
                    self._idle_connections.put_nowait(connection)
                except Exception:
                    self._destroy_connection(connection)

            status = {
                'status': 'unhealthy' if unhealthy_count > 0 else 'healthy',
                'active_connections': len(self._active_connections),
                'idle_connections': self._idle_connections.qsize(),
                'total_connections': len(self._active_connections) + self._idle_connections.qsize(),
                'unhealthy_connections': unhealthy_count,
                'pool_name': self._name,
                'connection_type': 'postgresql'
            }

            self._last_health_check = current_time
            self._last_health_status = status

            if unhealthy_count > 0:
                logger.warning(
                    f"PostgreSQL connection pool health check found {unhealthy_count} unhealthy connections"
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
                'pool_name': self._name,
                'connection_type': 'postgresql',
                'min_size': self._min_size,
                'max_size': self._max_size,
                'active_connections': len(self._active_connections),
                'idle_connections': self._idle_connections.qsize(),
                'total_connections': len(self._active_connections) + self._idle_connections.qsize(),
                'wait_timeout': self._wait_timeout,
                'max_idle_time': self._max_idle_time,
                'max_lifetime': self._max_lifetime,
                'health_check_interval': self._health_check_interval,
            }

    @contextmanager
    def transaction(self):
        """
        Transaction Context Manager

        Provides convenient transaction execution

        Example:
            with pool.transaction() as connection:
                connection.execute("INSERT INTO users (name) VALUES ('test')")
        """
        connection = self.acquire()
        transaction = None
        
        try:
            transaction = connection.begin_transaction()
            yield connection
            
            if transaction:
                transaction.commit()
        except Exception:
            if transaction:
                transaction.rollback()
            raise
        finally:
            self.release(connection)


# Register PostgreSQL Connection Type
from ..plugin_registry import PluginRegistry

def register_postgresql_plugin(registry: PluginRegistry) -> None:
    """
    Register PostgreSQL Plugin to Registry

    Args:
        registry: Plugin registry instance
    """
    registry.register(
        type_name='postgresql',
        pool_class=PostgreSQLConnectionPool,
        connection_class=PostgreSQLConnection
    )
    logger.info("PostgreSQL plugin registered")


# Alias for backward compatibility
PostgreSQLPool = PostgreSQLConnectionPool
