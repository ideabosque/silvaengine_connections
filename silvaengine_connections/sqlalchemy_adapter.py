#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
SQLAlchemy Adapter for silvaengine_connections.

Provides SQLAlchemy-based connection pooling and ORM support,
integrating with the existing connection pool framework.
"""

import logging
import threading
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, List, Optional, Type

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import Session, sessionmaker, SessionFactory
from sqlalchemy.pool import QueuePool, NullPool, Pool

from silvaengine_utility import Invoker

from .connection import BaseConnection
from .connection_pool import BaseConnectionPool, PoolStatus
from .config import ConnectionConfig
from .exceptions import (
    ConnectionError,
    PoolError,
    ConfigurationError,
)


@dataclass
class SQLAlchemyEngineConfig:
    """
    SQLAlchemy Engine configuration.

    Attributes:
        url: Database URL
        pool_size: Number of connections in pool
        max_overflow: Max overflow connections
        pool_timeout: Pool acquisition timeout
        pool_recycle: Connection recycle time (seconds)
        pool_pre_ping: Enable connection health checks
        echo: Enable SQL echo
        connect_args: Additional connection arguments
    """

    url: str
    pool_size: int = 5
    max_overflow: int = 10
    pool_timeout: int = 30
    pool_recycle: int = 3600
    pool_pre_ping: bool = True
    echo: bool = False
    connect_args: Dict[str, Any] = field(default_factory=dict)


def create_engine_config(
    connection_config: ConnectionConfig,
) -> SQLAlchemyEngineConfig:
    """
    Create SQLAlchemy Engine config from ConnectionConfig.

    Args:
        connection_config: Connection configuration

    Returns:
        SQLAlchemyEngineConfig instance
    """
    settings = connection_config.settings

    url = settings.get("url")
    if not url:
        host = settings.get("host", "localhost")
        port = settings.get("port", 5432)
        database = settings.get("database", "postgres")
        username = settings.get("username", "postgres")
        password = settings.get("password", "")

        if password:
            url = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        else:
            url = f"postgresql://{username}@{host}:{port}/{database}"

    pool_settings = connection_config.pool_settings

    return SQLAlchemyEngineConfig(
        url=url,
        pool_size=pool_settings.get("pool_size", 5),
        max_overflow=pool_settings.get("max_overflow", 10),
        pool_timeout=pool_settings.get("pool_timeout", 30),
        pool_recycle=pool_settings.get("pool_recycle", 3600),
        pool_pre_ping=pool_settings.get("pool_pre_ping", True),
        echo=pool_settings.get("echo", False),
        connect_args=settings.get("connect_args", {}),
    )


class SQLAlchemyConnectionAdapter:
    """
    SQLAlchemy-based connection adapter.

    Provides a unified interface for SQLAlchemy Engine and Session management,
    with support for transaction management and context manager pattern.
    """

    def __init__(
        self,
        engine: Engine,
        session_factory: Optional[SessionFactory] = None,
    ) -> None:
        """
        Initialize the adapter.

        Args:
            engine: SQLAlchemy Engine
            session_factory: Optional session factory (created if not provided)
        """
        self._engine = engine
        self._session_factory = session_factory or sessionmaker(bind=engine)
        self._logger = logging.getLogger(f"{__name__}.SQLAlchemyConnectionAdapter")

    @property
    def engine(self) -> Engine:
        """Get the SQLAlchemy Engine."""
        return self._engine

    @property
    def url(self) -> str:
        """Get the database URL."""
        return str(self._engine.url)

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Get a database session with automatic cleanup.

        Example:
            with adapter.session() as session:
                result = session.execute(text("SELECT 1"))

        Yields:
            Session: Database session
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            self._logger.error(f"Session error, rolled back: {e}")
            raise
        finally:
            session.close()

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """
        Get a raw database connection.

        Example:
            with adapter.connection() as conn:
                result = conn.execute(text("SELECT 1"))

        Yields:
            Connection: Raw database connection
        """
        with self._engine.connect() as conn:
            yield conn

    def execute(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        commit: bool = True,
    ) -> Any:
        """
        Execute a raw SQL query.

        Args:
            query: SQL query string
            params: Query parameters
            commit: Whether to commit after execution

        Returns:
            Query result
        """
        with self.session() as session:
            result = session.execute(text(query), params or {})
            if commit:
                session.commit()
            return result

    def execute_many(
        self,
        query: str,
        params_list: List[Dict[str, Any]],
        commit: bool = True,
    ) -> None:
        """
        Execute a SQL query multiple times with different parameters.

        Args:
            query: SQL query string
            params_list: List of parameter dictionaries
            commit: Whether to commit after execution
        """
        with self.session() as session:
            for params in params_list:
                session.execute(text(query), params)
            if commit:
                session.commit()

    def begin(self) -> Any:
        """
        Begin a transaction.

        Returns:
            Transaction: SQLAlchemy transaction object
        """
        return self._engine.begin()

    def ping(self) -> bool:
        """
        Check if the connection is alive.

        Returns:
            bool: True if connection is alive
        """
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            self._logger.warning(f"Connection ping failed: {e}")
            return False

    def dispose(self) -> None:
        """
        Dispose the engine and close all connections.
        """
        self._engine.dispose()
        self._logger.info("Engine disposed")

    def __enter__(self) -> "SQLAlchemyConnectionAdapter":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.dispose()


class SQLAlchemyPoolWrapper(BaseConnectionPool):
    """
    SQLAlchemy-based connection pool wrapper.

    Wraps SQLAlchemy's connection pool to integrate with the
    existing BaseConnectionPool interface.
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
        health_check_interval: float = 30.0,
        enable_dynamic_resize: bool = True,
    ) -> None:
        """
        Initialize the SQLAlchemy pool wrapper.

        Args:
            name: Pool name
            config: Connection configuration
            min_size: Minimum pool size
            max_size: Maximum pool size
            max_idle_time: Max idle time (seconds)
            max_lifetime: Max connection lifetime (seconds)
            wait_timeout: Wait timeout (seconds)
            health_check_interval: Health check interval (seconds)
            enable_dynamic_resize: Enable dynamic resizing
        """
        self._sqlalchemy_config = create_engine_config(config)

        self._engine = self._create_engine()
        self._session_factory = sessionmaker(bind=self._engine)

        super().__init__(
            name=name,
            connection_class=SQLAlchemyConnectionAdapter,
            min_size=min_size,
            max_size=max_size,
            max_idle_time=max_idle_time,
            max_lifetime=max_lifetime,
            wait_timeout=wait_timeout,
            health_check_interval=health_check_interval,
            enable_dynamic_resize=enable_dynamic_resize,
        )

    def _create_engine(self) -> Engine:
        """
        Create SQLAlchemy Engine.

        Returns:
            Engine: SQLAlchemy Engine
        """
        config = self._sqlalchemy_config

        self._logger.info(
            f"Creating SQLAlchemy Engine: {config.url} "
            f"(pool_size={config.pool_size}, max_overflow={config.max_overflow})"
        )

        return create_engine(
            config.url,
            poolclass=QueuePool,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=config.pool_timeout,
            pool_recycle=config.pool_recycle,
            pool_pre_ping=config.pool_pre_ping,
            echo=config.echo,
            **config.connect_args,
        )

    def get_pool_type(self) -> str:
        """Get pool type identifier."""
        return "sqlalchemy"

    def _create_connection(self) -> SQLAlchemyConnectionAdapter:
        """Create a new connection."""
        return SQLAlchemyConnectionAdapter(
            engine=self._engine,
            session_factory=self._session_factory,
        )

    def get_engine(self) -> Engine:
        """
        Get the underlying SQLAlchemy Engine.

        Returns:
            Engine: SQLAlchemy Engine
        """
        return self._engine

    def get_session(self) -> Session:
        """
        Get a new database session.

        Returns:
            Session: Database session

        Example:
            session = pool.get_session()
            try:
                result = session.execute(text("SELECT 1"))
            finally:
                session.close()
        """
        return self._session_factory()

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Get a database session with automatic cleanup.

        Example:
            with pool.session() as session:
                result = session.execute(text("SELECT 1"))

        Yields:
            Session: Database session
        """
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def close(self) -> None:
        """Close the pool and dispose the engine."""
        super().close()
        self._engine.dispose()
        self._logger.info(f"SQLAlchemy pool closed: {self._name}")


class SQLAlchemyPoolManager:
    """
    Manager for SQLAlchemy-based connection pools.

    Provides a simplified interface for creating and managing
    SQLAlchemy connection pools.
    """

    _instance: Optional["SQLAlchemyPoolManager"] = None
    _lock = threading.Lock()

    def __new__(cls) -> "SQLAlchemyPoolManager":
        """Create or return singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self) -> None:
        """Initialize the manager."""
        if self._initialized:
            return

        self._pools: Dict[str, SQLAlchemyPoolWrapper] = {}
        self._logger = logging.getLogger(f"{__name__}.SQLAlchemyPoolManager")
        self._initialized = True

    def create_pool(
        self,
        name: str,
        config: ConnectionConfig,
        **pool_kwargs: Any,
    ) -> SQLAlchemyPoolWrapper:
        """
        Create a SQLAlchemy connection pool.

        Args:
            name: Pool name
            config: Connection configuration
            **pool_kwargs: Additional pool arguments

        Returns:
            SQLAlchemyPoolWrapper: Created pool
        """
        if name in self._pools:
            raise PoolError(f"Pool '{name}' already exists")

        pool = SQLAlchemyPoolWrapper(name=name, config=config, **pool_kwargs)
        self._pools[name] = pool

        self._logger.info(f"Created SQLAlchemy pool: {name}")
        return pool

    def get_pool(self, name: str) -> Optional[SQLAlchemyPoolWrapper]:
        """
        Get a pool by name.

        Args:
            name: Pool name

        Returns:
            Optional[SQLAlchemyPoolWrapper]: Pool or None
        """
        return self._pools.get(name)

    def remove_pool(self, name: str) -> bool:
        """
        Remove a pool.

        Args:
            name: Pool name

        Returns:
            bool: True if removed
        """
        pool = self._pools.get(name)
        if pool:
            pool.close()
            del self._pools[name]
            self._logger.info(f"Removed SQLAlchemy pool: {name}")
            return True
        return False

    def close_all(self) -> None:
        """Close all pools."""
        for name, pool in self._pools.items():
            pool.close()
        self._pools.clear()
        self._logger.info("All SQLAlchemy pools closed")


__all__ = [
    "SQLAlchemyEngineConfig",
    "SQLAlchemyConnectionAdapter",
    "SQLAlchemyPoolWrapper",
    "SQLAlchemyPoolManager",
    "create_engine_config",
]
