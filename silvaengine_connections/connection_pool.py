#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Base Connection Pool class for silvaengine_connections.

Provides a robust connection pool implementation with health checking,
dynamic resizing, and comprehensive metrics.
"""

import logging
import queue
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generic, Iterator, List, Optional, Type, TypeVar

from .connection import BaseConnection
from .exceptions import PoolError, PoolExhaustedError, PoolNotReadyError

C = TypeVar("C", bound=BaseConnection)


class PoolStatus(Enum):
    """Connection pool status enumeration."""

    INITIALIZING = "initializing"
    READY = "ready"
    PAUSED = "paused"
    SHUTDOWN = "shutdown"


@dataclass
class PoolMetrics:
    """Connection pool metrics data class."""

    total_created: int = 0
    total_destroyed: int = 0
    total_borrowed: int = 0
    total_returned: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    wait_time_avg: float = 0.0
    wait_time_max: float = 0.0
    health_check_failures: int = 0
    last_health_check: Optional[float] = None


class BaseConnectionPool(ABC, Generic[C]):
    """
    Abstract base class for connection pools.

    Provides a robust implementation with:
    - Connection lifecycle management
    - Health checking
    - Dynamic resizing
    - Comprehensive metrics
    - Thread-safe operations

    Type Parameters:
        C: Connection type, must be a subclass of BaseConnection

    Example:
        ```python
        class PostgreSQLPool(BaseConnectionPool[PostgreSQLConnection]):
            def get_pool_type(self) -> str:
                return "postgresql"

            def _create_connection(self) -> PostgreSQLConnection:
                return PostgreSQLConnection(self._config)

        # Usage
        pool = PostgreSQLPool("main", PostgreSQLConnection, min_size=2, max_size=10)
        with pool.connection() as connection:
            result = connection.execute("SELECT 1")
        ```
    """

    def __init__(
        self,
        name: str,
        connection_class: Type[C],
        min_size: int = 2,
        max_size: int = 10,
        max_idle_time: float = 300.0,
        max_lifetime: float = 3600.0,
        wait_timeout: float = 10.0,
        health_check_interval: float = 30.0,
        enable_dynamic_resize: bool = True,
    ) -> None:
        """
        Initialize the connection pool.

        Args:
            name: Pool name
            connection_class: Connection class type
            min_size: Minimum number of connections
            max_size: Maximum number of connections
            max_idle_time: Maximum idle time in seconds
            max_lifetime: Maximum connection lifetime in seconds
            wait_timeout: Connection acquisition timeout in seconds
            health_check_interval: Health check interval in seconds
            enable_dynamic_resize: Whether to enable dynamic resizing
        """
        self._name = name
        self._connection_class = connection_class
        self._min_size = min_size
        self._max_size = max_size
        self._max_idle_time = max_idle_time
        self._max_lifetime = max_lifetime
        self._wait_timeout = wait_timeout
        self._health_check_interval = health_check_interval
        self._enable_dynamic_resize = enable_dynamic_resize

        self._status = PoolStatus.INITIALIZING
        self._metrics = PoolMetrics()
        self._lock = threading.RLock()
        self._logger = logging.getLogger(f"{__name__}.{name}")

        # Connection storage
        self._idle_connections: queue.Queue[C] = queue.Queue(maxsize=max_size)
        self._active_connections: Dict[int, C] = {}

        self._initialize_pool()

    @property
    def name(self) -> str:
        """Get pool name."""
        return self._name

    @property
    def status(self) -> PoolStatus:
        """Get pool status."""
        return self._status

    @property
    def metrics(self) -> PoolMetrics:
        """Get pool metrics (returns a copy)."""
        with self._lock:
            return PoolMetrics(**self._metrics.__dict__)

    @abstractmethod
    def get_pool_type(self) -> str:
        """
        Get pool type identifier.

        Returns:
            str: Type identifier (e.g., 'postgresql', 'neo4j', 'httpx', 'boto3')
        """
        pass

    @abstractmethod
    def _create_connection(self) -> C:
        """
        Create a new connection instance.

        Returns:
            C: New connection object
        """
        pass

    def _initialize_pool(self) -> None:
        """Initialize pool with minimum connections."""
        with self._lock:
            for _ in range(self._min_size):
                try:
                    connection = self._create_connection()
                    self._idle_connections.put(connection)
                    self._metrics.total_created += 1
                except Exception as e:
                    self._logger.error(f"Failed to create initial connection: {e}")

            self._metrics.idle_connections = self._idle_connections.qsize()
            self._status = PoolStatus.READY
            self._logger.info(
                f"Pool initialized: {self._name} "
                f"(type={self.get_pool_type()}, "
                f"min={self._min_size}, max={self._max_size})"
            )

    def acquire(self) -> C:
        """
        Acquire a connection from the pool.

        Returns:
            C: Connection object

        Raises:
            PoolNotReadyError: If pool is not ready
            PoolExhaustedError: If pool is exhausted
        """
        if self._status != PoolStatus.READY:
            raise PoolNotReadyError(
                f"Pool {self._name} is not ready",
                pool_name=self._name,
                status=self._status.value,
            )

        start_time = time.time()

        with self._lock:
            # Try to get from idle queue
            try:
                connection = self._idle_connections.get(block=False)

                if self._is_connection_valid(connection):
                    connection.is_used = True
                    self._active_connections[connection.connection_id] = connection
                    self._metrics.total_borrowed += 1
                    self._metrics.active_connections += 1
                    self._metrics.idle_connections -= 1
                    return connection
                else:
                    # Connection invalid, destroy and try again
                    self._destroy_connection(connection)
            except queue.Empty:
                pass

            # Check if we can create new connection
            total_connections = (
                len(self._active_connections) + self._idle_connections.qsize()
            )

            if total_connections < self._max_size:
                try:
                    connection = self._create_connection()
                    connection.is_used = True
                    self._active_connections[connection.connection_id] = connection
                    self._metrics.total_created += 1
                    self._metrics.total_borrowed += 1
                    self._metrics.active_connections += 1

                    return connection
                except Exception as e:
                    self._logger.error(f"Failed to create new connection: {e}")

        # Wait for available connection
        try:
            connection = self._idle_connections.get(timeout=self._wait_timeout)
            wait_time = time.time() - start_time

            with self._lock:
                self._update_wait_time_metrics(wait_time)

                if self._is_connection_valid(connection):
                    connection.is_used = True
                    self._active_connections[connection.connection_id] = connection
                    self._metrics.total_borrowed += 1
                    self._metrics.active_connections += 1
                    self._metrics.idle_connections -= 1
                    return connection
                else:
                    # Connection invalid, destroy and retry
                    self._destroy_connection(connection)
                    # Retry within the same acquire call to avoid recursion
                    return self._acquire_with_retry()

        except queue.Empty:
            raise PoolExhaustedError(
                f"Pool {self._name} exhausted, "
                f"unable to acquire connection within {self._wait_timeout}s",
                pool_name=self._name,
            )

    def _acquire_with_retry(self) -> C:
        """
        Retry acquiring a connection without recursion.

        Returns:
            C: Connection object

        Raises:
            PoolExhaustedError: If pool is exhausted
        """
        # Try to get from idle queue without waiting
        try:
            connection = self._idle_connections.get(block=False)

            if self._is_connection_valid(connection):
                with self._lock:
                    connection.is_used = True
                    self._active_connections[connection.connection_id] = connection
                    self._metrics.total_borrowed += 1
                    self._metrics.active_connections += 1
                    self._metrics.idle_connections -= 1
                return connection
            else:
                self._destroy_connection(connection)
                # Try one more time
                return self._acquire_with_retry()
        except queue.Empty:
            # No idle connections available, try to create new one
            with self._lock:
                total_connections = (
                    len(self._active_connections) + self._idle_connections.qsize()
                )

                if total_connections < self._max_size:
                    try:
                        connection = self._create_connection()
                        connection.is_used = True
                        self._active_connections[connection.connection_id] = connection
                        self._metrics.total_created += 1
                        self._metrics.total_borrowed += 1
                        self._metrics.active_connections += 1
                        return connection
                    except Exception as e:
                        self._logger.error(f"Failed to create new connection: {e}")

            raise PoolExhaustedError(
                f"Pool {self._name} exhausted, no connections available",
                pool_name=self._name,
            )

    def release(self, connection: C) -> None:
        """
        Release a connection back to the pool.

        Args:
            connection: Connection to release
        """
        if connection is None:
            return

        with self._lock:
            if connection.connection_id in self._active_connections:
                del self._active_connections[connection.connection_id]
                self._metrics.active_connections -= 1

            if self._status == PoolStatus.SHUTDOWN:
                self._destroy_connection(connection)
                return

            if self._is_connection_valid(connection):
                connection.reset()
                try:
                    self._idle_connections.put(connection, block=False)
                    self._metrics.total_returned += 1
                    self._metrics.idle_connections += 1
                except queue.Full:
                    self._destroy_connection(connection)
            else:
                self._destroy_connection(connection)

    @contextmanager
    def connection(self) -> Iterator[C]:
        """
        Context manager for automatic connection management.

        Example:
            ```python
            with pool.connection() as connection:
                result = connection.execute("SELECT 1")
            ```

        Yields:
            C: Connection object
        """
        connection = None

        try:
            connection = self.acquire()
            yield connection
        finally:
            if connection:
                self.release(connection)

    def _is_connection_valid(self, connection: C) -> bool:
        """
        Check if connection is valid.

        Args:
            connection: Connection object

        Returns:
            bool: True if valid
        """
        # Check lifetime
        if connection.get_lifetime() > self._max_lifetime:
            return False

        # Check idle time
        if not connection.is_used and connection.get_idle_time() > self._max_idle_time:
            return False

        # Health check
        return connection.is_healthy()

    def _destroy_connection(self, connection: C) -> None:
        """Destroy a connection."""
        try:
            connection.close()
        except Exception as e:
            self._logger.debug(f"Error closing connection: {e}")
        finally:
            with self._lock:
                self._metrics.total_destroyed += 1

    def close(self) -> None:
        """Close the pool and release all resources."""
        with self._lock:
            self._status = PoolStatus.SHUTDOWN

            # Close active connections
            for connection in list(self._active_connections.values()):
                self._destroy_connection(connection)
            self._active_connections.clear()

            # Close idle connections
            while not self._idle_connections.empty():
                try:
                    connection = self._idle_connections.get(block=False)
                    self._destroy_connection(connection)
                except queue.Empty:
                    break

            self._metrics.active_connections = 0
            self._metrics.idle_connections = 0

            self._logger.info(f"Pool closed: {self._name}")

    def _update_wait_time_metrics(self, wait_time: float) -> None:
        """Update wait time metrics."""
        self._metrics.wait_time_max = max(self._metrics.wait_time_max, wait_time)
        # Exponential moving average
        alpha = 0.1
        self._metrics.wait_time_avg = (
            alpha * wait_time + (1 - alpha) * self._metrics.wait_time_avg
        )

    def resize(self, new_min_size: int, new_max_size: int) -> None:
        """
        Dynamically resize the pool.

        Args:
            new_min_size: New minimum size
            new_max_size: New maximum size
        """
        with self._lock:
            self._min_size = new_min_size
            self._max_size = new_max_size

            # Add connections if needed
            current_total = (
                len(self._active_connections) + self._idle_connections.qsize()
            )
            if current_total < self._min_size:
                for _ in range(self._min_size - current_total):
                    try:
                        connection = self._create_connection()
                        self._idle_connections.put(connection)
                        self._metrics.total_created += 1
                        self._metrics.idle_connections += 1
                    except Exception as e:
                        self._logger.error(
                            f"Failed to create connection during resize: {e}"
                        )

            self._logger.info(
                f"Pool resized: {self._name} (min={new_min_size}, max={new_max_size})"
            )

    def __enter__(self) -> "BaseConnectionPool[C]":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<{self.__class__.__name__}("
            f"name={self._name}, "
            f"type={self.get_pool_type()}, "
            f"status={self._status.value})>"
        )
