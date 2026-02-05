#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Base Connection class for silvaengine_connections.

Provides the foundation for all connection implementations with
context manager support and lifecycle management.
"""

import time
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar
from types import TracebackType

from .exceptions import ConnectionClosedError, ConnectionError

T = TypeVar('T')


class BaseConnection(ABC, Generic[T]):
    """
    Abstract base class for all connection types.
    
    All concrete connection implementations must inherit from this class
    and implement the required abstract methods.
    
    Type Parameters:
        T: The type of the underlying connection object (e.g., sqlalchemy.Engine, neo4j.Driver)
    
    Example:
        ```python
        class PostgreSQLConnection(BaseConnection[Engine]):
            def connect(self) -> Engine:
                return create_engine(self._config['url'])
        
        # Usage with context manager
        with PostgreSQLConnection(1, config) as conn:
            result = conn.raw_connection.execute("SELECT 1")
        ```
    """
    
    _counter = 0
    _counter_lock = threading.Lock()
    
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the connection.
        
        Args:
            config: Configuration dictionary containing connection parameters.
        """
        with self._counter_lock:
            BaseConnection._counter += 1
            self._connection_id = BaseConnection._counter
        
        self._config = config
        self._is_used = False
        self._is_closed = False
        self._created_at = time.time()
        self._last_used_at = time.time()
        self._raw_connection: Optional[T] = None
        self._lock = threading.RLock()
    
    @property
    def connection_id(self) -> int:
        """Get the unique connection identifier."""
        return self._connection_id
    
    @property
    def is_used(self) -> bool:
        """Check if the connection is currently in use."""
        return self._is_used
    
    @is_used.setter
    def is_used(self, value: bool) -> None:
        """
        Set the connection usage status.
        
        Args:
            value: True if connection is being used, False otherwise.
        """
        with self._lock:
            self._is_used = value
            if value:
                self._last_used_at = time.time()
    
    @property
    def is_closed(self) -> bool:
        """Check if the connection has been closed."""
        return self._is_closed
    
    @property
    def raw_connection(self) -> Optional[T]:
        """
        Get the underlying raw connection object.
        
        Returns:
            The underlying connection object or None if not connected.
        """
        return self._raw_connection
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get a copy of the connection configuration."""
        return self._config.copy()
    
    @abstractmethod
    def connect(self) -> T:
        """
        Establish the underlying connection.
        
        Returns:
            T: The established connection object.
            
        Raises:
            ConnectionError: If connection establishment fails.
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close the underlying connection and release resources.
        
        This method should be idempotent - calling it multiple times
        should not raise an error.
        """
        pass
    
    @abstractmethod
    def is_healthy(self) -> bool:
        """
        Check if the connection is healthy.
        
        Returns:
            bool: True if the connection is healthy and usable, False otherwise.
        """
        pass
    
    def reset(self) -> None:
        """
        Reset the connection state for return to the pool.
        
        Subclasses can override this method to perform custom reset logic
        (e.g., rollback transactions, clear session state).
        """
        with self._lock:
            self._is_used = False
    
    def get_lifetime(self) -> float:
        """
        Get the total lifetime of the connection.
        
        Returns:
            float: Time in seconds since connection creation.
        """
        return time.time() - self._created_at
    
    def get_idle_time(self) -> float:
        """
        Get the idle time of the connection.
        
        Returns:
            float: Time in seconds since last use.
        """
        return time.time() - self._last_used_at
    
    def _ensure_not_closed(self) -> None:
        """
        Ensure the connection is not closed before operations.
        
        Raises:
            ConnectionClosedError: If the connection is closed.
        """
        if self._is_closed:
            raise ConnectionClosedError(
                f"Connection {self._connection_id} is closed",
                connection_id=self._connection_id
            )
    
    def __enter__(self) -> 'BaseConnection[T]':
        """
        Context manager entry.
        
        Returns:
            BaseConnection: Self for use in context.
        """
        return self
    
    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        """
        Context manager exit - automatically closes the connection.
        
        Args:
            exc_type: Exception type if an exception occurred.
            exc_val: Exception value if an exception occurred.
            exc_tb: Exception traceback if an exception occurred.
        """
        self.close()
    
    def __repr__(self) -> str:
        """String representation of the connection."""
        return f"<{self.__class__.__name__}(id={self._connection_id}, used={self._is_used}, closed={self._is_closed})>"
