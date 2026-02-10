#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Connection Lifecycle Management for silvaengine_connections.

This module provides comprehensive connection lifecycle management including:
- Connection configuration validation
- Connection creation and initialization
- Connection registration and management
- Connection pool lifecycle management
- Error handling and recovery

All connection operations are encapsulated within this module to ensure
modularity, consistency, and maintainability.
"""

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from .config import ConnectionConfig, ConfigManager
from .connection import BaseConnection
from .connection_pool import BaseConnectionPool, PoolMetrics, PoolStatus
from .exceptions import (
    ConfigValidationError,
    ConnectionError,
    ConnectionFailedError,
    ConnectionNotFoundError,
    PoolAlreadyExistsError,
    PoolError,
    PoolNotFoundError,
    PluginNotFoundError,
)
from .plugin_registry import ConnectionPlugin, PluginRegistry


T = TypeVar("T", bound=BaseConnection)
P = TypeVar("P", bound=BaseConnectionPool)


class ConnectionState(Enum):
    """Connection lifecycle state enumeration."""

    PENDING = "pending"
    INITIALIZING = "initializing"
    READY = "ready"
    ACTIVE = "active"
    IDLE = "idle"
    CLOSING = "closing"
    CLOSED = "closed"
    ERROR = "error"


@dataclass
class ConnectionLifecycleEvent:
    """Connection lifecycle event data class."""

    event_type: str
    connection_name: str
    timestamp: float = field(default_factory=time.time)
    details: Dict[str, Any] = field(default_factory=dict)
    error: Optional[Exception] = None


@dataclass
class ConnectionLifecycleContext:
    """Connection lifecycle context data class."""

    connection_name: str
    connection_type: str
    config: ConnectionConfig
    state: ConnectionState = ConnectionState.PENDING
    created_at: float = field(default_factory=time.time)
    initialized_at: Optional[float] = None
    last_used_at: Optional[float] = None
    closed_at: Optional[float] = None
    error_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConnectionLifecycleManager:
    """
    Connection Lifecycle Manager.

    Manages the complete lifecycle of connections including:
    - Configuration validation
    - Connection creation and initialization
    - Connection registration
    - State tracking and transitions
    - Error handling and recovery
    - Resource cleanup

    This class provides a centralized management interface for all
    connection-related operations, ensuring consistency and reliability.

    Example:
        ```python
        # Create lifecycle manager
        lifecycle_manager = ConnectionLifecycleManager()

        # Validate configuration
        config = ConnectionConfig.from_dict({...})
        errors = lifecycle_manager.validate_config(config)

        # Create connection
        context = lifecycle_manager.create_connection("postgres_main", config)

        # Register connection
        lifecycle_manager.register_connection(context)

        # Use connection
        with lifecycle_manager.acquire_connection("postgres_main") as conn:
            result = conn.execute("SELECT 1")

        # Close connection
        lifecycle_manager.close_connection("postgres_main")
        ```
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the lifecycle manager.

        Args:
            logger: Optional logger instance.
        """
        self._logger = logger or logging.getLogger(__name__)
        self._contexts: Dict[str, ConnectionLifecycleContext] = {}
        self._connections: Dict[str, BaseConnection] = {}
        self._pools: Dict[str, BaseConnectionPool] = {}
        self._lock = threading.RLock()
        self._event_handlers: Dict[str, List[Callable]] = {
            "created": [],
            "initialized": [],
            "registered": [],
            "acquired": [],
            "released": [],
            "closed": [],
            "error": [],
        }
        self._config_manager = ConfigManager()
        self._plugin_registry = PluginRegistry()

    def validate_config(self, config: ConnectionConfig) -> List[str]:
        """
        Validate connection configuration.

        Performs comprehensive validation including:
        - Required field validation
        - Type validation
        - Range validation
        - Cross-field validation

        Args:
            config: Connection configuration to validate.

        Returns:
            List of validation error messages (empty if valid).
        """
        errors = []

        # Validate required fields
        if not config.type:
            errors.append("Connection type is required")

        if not isinstance(config.type, str):
            errors.append("Connection type must be a string")

        # Validate enabled field
        if not isinstance(config.enabled, bool):
            errors.append("Enabled must be a boolean")

        # Validate settings
        if not isinstance(config.settings, dict):
            errors.append("Settings must be a dictionary")

        # Validate pool settings
        errors.extend(self._validate_pool_settings(config))

        # Validate connection-specific settings
        errors.extend(self._validate_connection_settings(config))

        self._logger.debug(
            f"Configuration validation for type '{config.type}': "
            f"{len(errors)} errors found"
        )

        return errors

    def _validate_pool_settings(self, config: ConnectionConfig) -> List[str]:
        """Validate pool configuration settings."""
        errors = []
        pool_settings = config.pool_settings

        if not isinstance(pool_settings, dict):
            errors.append("Pool settings must be a dictionary")
            return errors

        # Validate min_size
        min_size = pool_settings.get("min_size")
        if min_size is not None:
            if not isinstance(min_size, int) or min_size < 0:
                errors.append("min_size must be a non-negative integer")

        # Validate max_size
        max_size = pool_settings.get("max_size")
        if max_size is not None:
            if not isinstance(max_size, int) or max_size < 1:
                errors.append("max_size must be a positive integer")

        # Validate min_size <= max_size
        if (
            min_size is not None
            and max_size is not None
            and min_size > max_size
        ):
            errors.append("min_size cannot be greater than max_size")

        # Validate timeout values
        for timeout_field in ["wait_timeout", "max_idle_time", "max_lifetime"]:
            value = pool_settings.get(timeout_field)
            if value is not None and (not isinstance(value, (int, float)) or value < 0):
                errors.append(f"{timeout_field} must be a non-negative number")

        # Validate health_check_interval
        health_interval = pool_settings.get("health_check_interval")
        if health_interval is not None and (
            not isinstance(health_interval, (int, float)) or health_interval < 0
        ):
            errors.append("health_check_interval must be a non-negative number")

        # Validate enable_dynamic_resize
        enable_resize = pool_settings.get("enable_dynamic_resize")
        if enable_resize is not None and not isinstance(enable_resize, bool):
            errors.append("enable_dynamic_resize must be a boolean")

        return errors

    def _validate_connection_settings(self, config: ConnectionConfig) -> List[str]:
        """Validate connection-specific settings based on type."""
        errors = []
        settings = config.settings
        connection_type = config.type

        if connection_type == "postgresql":
            errors.extend(self._validate_postgresql_settings(settings))
        elif connection_type == "neo4j":
            errors.extend(self._validate_neo4j_settings(settings))
        elif connection_type == "httpx":
            errors.extend(self._validate_httpx_settings(settings))
        elif connection_type == "boto3":
            errors.extend(self._validate_boto3_settings(settings))

        return errors

    def _validate_postgresql_settings(self, settings: Dict[str, Any]) -> List[str]:
        """Validate PostgreSQL-specific settings."""
        errors = []

        required_fields = ["host", "port", "database", "username", "password"]
        for field in required_fields:
            if field not in settings:
                errors.append(f"PostgreSQL settings missing required field: {field}")

        # Validate port
        port = settings.get("port")
        if port is not None and (not isinstance(port, int) or port < 1 or port > 65535):
            errors.append("PostgreSQL port must be an integer between 1 and 65535")

        return errors

    def _validate_neo4j_settings(self, settings: Dict[str, Any]) -> List[str]:
        """Validate Neo4j-specific settings."""
        errors = []

        required_fields = ["uri", "username", "password"]
        for field in required_fields:
            if field not in settings:
                errors.append(f"Neo4j settings missing required field: {field}")

        # Validate URI format
        uri = settings.get("uri", "")
        if uri and not uri.startswith(("bolt://", "neo4j://", "neo4j+s://")):
            errors.append("Neo4j URI must start with bolt://, neo4j://, or neo4j+s://")

        return errors

    def _validate_httpx_settings(self, settings: Dict[str, Any]) -> List[str]:
        """Validate HTTPX-specific settings."""
        errors = []

        # Base URL is required for HTTPX
        if "base_url" not in settings and "url" not in settings:
            errors.append("HTTPX settings must include base_url or url")

        return errors

    def _validate_boto3_settings(self, settings: Dict[str, Any]) -> List[str]:
        """Validate Boto3-specific settings."""
        errors = []

        # Service name is required for Boto3
        if "service_name" not in settings:
            errors.append("Boto3 settings must include service_name")

        return errors

    def create_connection_context(
        self, name: str, config: ConnectionConfig
    ) -> ConnectionLifecycleContext:
        """
        Create a connection lifecycle context.

        Args:
            name: Connection name.
            config: Connection configuration.

        Returns:
            ConnectionLifecycleContext instance.

        Raises:
            ConfigValidationError: If configuration is invalid.
        """
        # Validate configuration
        errors = self.validate_config(config)
        if errors:
            raise ConfigValidationError(
                f"Invalid configuration for '{name}': {'; '.join(errors)}"
            )

        # Create context
        context = ConnectionLifecycleContext(
            connection_name=name,
            connection_type=config.type,
            config=config,
            state=ConnectionState.PENDING,
        )

        with self._lock:
            self._contexts[name] = context

        self._logger.info(f"Created connection context: {name} (type: {config.type})")
        self._emit_event("created", name, {"config": config.to_dict()})

        return context

    def initialize_connection(
        self, name: str, config: Optional[ConnectionConfig] = None
    ) -> ConnectionLifecycleContext:
        """
        Initialize a connection.

        This method performs the complete initialization process:
        1. Get or create context
        2. Update state to INITIALIZING
        3. Create connection instance
        4. Update state to READY
        5. Emit initialization event

        Args:
            name: Connection name.
            config: Optional configuration (uses existing context if not provided).

        Returns:
            ConnectionLifecycleContext instance.

        Raises:
            ConnectionFailedError: If initialization fails.
        """
        with self._lock:
            # Get or create context
            if name in self._contexts:
                context = self._contexts[name]
            elif config:
                context = self.create_connection_context(name, config)
            else:
                raise ConnectionNotFoundError(f"Connection '{name}' not found")

            # Update state
            context.state = ConnectionState.INITIALIZING

            try:
                # Get plugin
                plugin = self._plugin_registry.get_safe(context.connection_type)

                # Create connection
                connection = plugin.connection_class(context.config.settings)

                # Store connection
                self._connections[name] = connection

                # Update context
                context.state = ConnectionState.READY
                context.initialized_at = time.time()

                self._logger.info(f"Initialized connection: {name}")
                self._emit_event("initialized", name)

                return context

            except Exception as e:
                context.state = ConnectionState.ERROR
                context.error_count += 1
                self._logger.error(f"Failed to initialize connection '{name}': {e}")
                self._emit_event("error", name, error=e)
                raise ConnectionFailedError(
                    f"Failed to initialize connection '{name}': {e}",
                    host=context.config.settings.get("host"),
                )

    def register_connection(
        self, name: str, connection: BaseConnection
    ) -> ConnectionLifecycleContext:
        """
        Register an existing connection.

        Args:
            name: Connection name.
            connection: Connection instance to register.

        Returns:
            ConnectionLifecycleContext instance.

        Raises:
            PoolAlreadyExistsError: If connection with same name already exists.
        """
        with self._lock:
            if name in self._connections:
                raise PoolAlreadyExistsError(
                    f"Connection '{name}' is already registered", pool_name=name
                )

            # Create context if not exists
            if name not in self._contexts:
                context = ConnectionLifecycleContext(
                    connection_name=name,
                    connection_type=connection.__class__.__name__,
                    config=ConnectionConfig(type=connection.__class__.__name__),
                    state=ConnectionState.READY,
                    initialized_at=time.time(),
                )
                self._contexts[name] = context
            else:
                context = self._contexts[name]
                context.state = ConnectionState.READY

            # Store connection
            self._connections[name] = connection

            self._logger.info(f"Registered connection: {name}")
            self._emit_event("registered", name)

            return context

    def acquire_connection(self, name: str) -> BaseConnection:
        """
        Acquire a connection for use.

        Args:
            name: Connection name.

        Returns:
            BaseConnection instance.

        Raises:
            ConnectionNotFoundError: If connection not found.
        """
        with self._lock:
            if name not in self._connections:
                raise ConnectionNotFoundError(f"Connection '{name}' not found")

            connection = self._connections[name]
            context = self._contexts[name]

            # Update context
            context.state = ConnectionState.ACTIVE
            context.last_used_at = time.time()

            # Mark connection as used
            connection.is_used = True

            self._logger.debug(f"Acquired connection: {name}")
            self._emit_event("acquired", name)

            return connection

    def release_connection(self, name: str) -> None:
        """
        Release a connection back to the pool.

        Args:
            name: Connection name.
        """
        with self._lock:
            if name not in self._connections:
                self._logger.warning(f"Cannot release unknown connection: {name}")
                return

            connection = self._connections[name]
            context = self._contexts[name]

            # Update connection state
            connection.reset()

            # Update context
            if context.state == ConnectionState.ACTIVE:
                context.state = ConnectionState.IDLE

            self._logger.debug(f"Released connection: {name}")
            self._emit_event("released", name)

    def close_connection(self, name: str) -> None:
        """
        Close a connection.

        Args:
            name: Connection name.
        """
        with self._lock:
            if name not in self._connections:
                self._logger.warning(f"Cannot close unknown connection: {name}")
                return

            context = self._contexts[name]
            context.state = ConnectionState.CLOSING

            try:
                connection = self._connections[name]
                connection.close()

                context.state = ConnectionState.CLOSED
                context.closed_at = time.time()

                del self._connections[name]

                self._logger.info(f"Closed connection: {name}")
                self._emit_event("closed", name)

            except Exception as e:
                context.state = ConnectionState.ERROR
                context.error_count += 1
                self._logger.error(f"Error closing connection '{name}': {e}")
                self._emit_event("error", name, error=e)

    def get_connection_context(self, name: str) -> Optional[ConnectionLifecycleContext]:
        """
        Get connection lifecycle context.

        Args:
            name: Connection name.

        Returns:
            ConnectionLifecycleContext or None if not found.
        """
        return self._contexts.get(name)

    def get_connection(self, name: str) -> Optional[BaseConnection]:
        """
        Get connection instance.

        Args:
            name: Connection name.

        Returns:
            BaseConnection or None if not found.
        """
        return self._connections.get(name)

    def get_all_contexts(self) -> Dict[str, ConnectionLifecycleContext]:
        """Get all connection contexts."""
        return self._contexts.copy()

    def get_all_connections(self) -> Dict[str, BaseConnection]:
        """Get all connections."""
        return self._connections.copy()

    def close_all_connections(self) -> None:
        """Close all connections."""
        with self._lock:
            for name in list(self._connections.keys()):
                try:
                    self.close_connection(name)
                except Exception as e:
                    self._logger.error(f"Error closing connection '{name}': {e}")

    def add_event_handler(self, event_type: str, handler: Callable) -> None:
        """
        Add an event handler.

        Args:
            event_type: Event type (created, initialized, registered, acquired, released, closed, error).
            handler: Handler function to call when event occurs.
        """
        if event_type in self._event_handlers:
            self._event_handlers[event_type].append(handler)

    def remove_event_handler(self, event_type: str, handler: Callable) -> bool:
        """
        Remove an event handler.

        Args:
            event_type: Event type.
            handler: Handler function to remove.

        Returns:
            True if handler was removed.
        """
        if event_type in self._event_handlers:
            try:
                self._event_handlers[event_type].remove(handler)
                return True
            except ValueError:
                pass
        return False

    def _emit_event(
        self,
        event_type: str,
        connection_name: str,
        details: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
    ) -> None:
        """Emit a lifecycle event."""
        event = ConnectionLifecycleEvent(
            event_type=event_type,
            connection_name=connection_name,
            details=details or {},
            error=error,
        )

        for handler in self._event_handlers.get(event_type, []):
            try:
                handler(event)
            except Exception as e:
                self._logger.error(f"Error in event handler: {e}")

    def register_plugin(
        self,
        type_name: str,
        pool_class: Type[BaseConnectionPool],
        connection_class: Type[BaseConnection],
    ) -> None:
        """
        Register a connection plugin.

        Args:
            type_name: Connection type name.
            pool_class: Pool class.
            connection_class: Connection class.
        """
        self._plugin_registry.register(type_name, pool_class, connection_class)
        self._logger.info(f"Registered connection plugin: {type_name}")

    def get_plugin(self, type_name: str) -> Optional[ConnectionPlugin]:
        """
        Get a registered plugin.

        Args:
            type_name: Connection type name.

        Returns:
            ConnectionPlugin or None if not found.
        """
        return self._plugin_registry.get(type_name)

    def get_registered_types(self) -> List[str]:
        """Get all registered connection types."""
        return self._plugin_registry.get_all_types()


class ConnectionPoolLifecycleManager(ConnectionLifecycleManager):
    """
    Connection Pool Lifecycle Manager.

    Extends ConnectionLifecycleManager to provide pool-specific lifecycle
    management including pool creation, resizing, and health monitoring.

    Example:
        ```python
        # Create pool lifecycle manager
        pool_manager = ConnectionPoolLifecycleManager()

        # Create pool
        pool = pool_manager.create_pool("postgres_main", config)

        # Use pool
        with pool_manager.acquire_pool_connection("postgres_main") as conn:
            result = conn.execute("SELECT 1")

        # Resize pool
        pool_manager.resize_pool("postgres_main", min_size=5, max_size=20)

        # Close pool
        pool_manager.close_pool("postgres_main")
        ```
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize the pool lifecycle manager."""
        super().__init__(logger)
        self._pool_contexts: Dict[str, ConnectionLifecycleContext] = {}

    def create_pool(
        self, name: str, config: ConnectionConfig
    ) -> BaseConnectionPool:
        """
        Create a connection pool.

        Args:
            name: Pool name.
            config: Connection configuration.

        Returns:
            BaseConnectionPool instance.

        Raises:
            ConfigValidationError: If configuration is invalid.
            PluginNotFoundError: If connection type is not registered.
            PoolAlreadyExistsError: If pool already exists.
        """
        with self._lock:
            if name in self._pools:
                raise PoolAlreadyExistsError(
                    f"Pool '{name}' already exists", pool_name=name
                )

            # Validate configuration
            errors = self.validate_config(config)
            if errors:
                raise ConfigValidationError(
                    f"Invalid configuration for pool '{name}': {'; '.join(errors)}"
                )

            # Get plugin
            plugin = self._plugin_registry.get_safe(config.type)

            # Apply defaults
            config = self._config_manager.apply_defaults(config)

            # Create pool - specific connection pool classes handle connection_class internally
            pool = plugin.pool_class(
                name=name,
                config=config,
                **config.pool_settings,
            )

            # Store pool
            self._pools[name] = pool

            # Create context
            context = ConnectionLifecycleContext(
                connection_name=name,
                connection_type=config.type,
                config=config,
                state=ConnectionState.READY,
                initialized_at=time.time(),
            )
            self._pool_contexts[name] = context

            self._logger.info(f"Created pool: {name} (type: {config.type})")
            self._emit_event("created", name, {"config": config.to_dict()})

            return pool

    def get_pool(self, name: str) -> Optional[BaseConnectionPool]:
        """
        Get a connection pool.

        Args:
            name: Pool name.

        Returns:
            BaseConnectionPool or None if not found.
        """
        return self._pools.get(name)

    def get_pool_safe(self, name: str) -> BaseConnectionPool:
        """
        Get a connection pool, raising exception if not found.

        Args:
            name: Pool name.

        Returns:
            BaseConnectionPool instance.

        Raises:
            PoolNotFoundError: If pool not found.
        """
        pool = self.get_pool(name)
        if pool is None:
            raise PoolNotFoundError(f"Pool '{name}' not found", pool_name=name)
        return pool

    def get_all_pools(self) -> Dict[str, BaseConnectionPool]:
        """Get all pools."""
        return self._pools.copy()

    def acquire_pool_connection(self, pool_name: str):
        """
        Acquire a connection from a pool.

        Args:
            pool_name: Pool name.

        Returns:
            Connection context manager.

        Raises:
            PoolNotFoundError: If pool not found.
        """
        pool = self.get_pool_safe(pool_name)
        context = self._pool_contexts.get(pool_name)

        if context:
            context.state = ConnectionState.ACTIVE
            context.last_used_at = time.time()

        self._emit_event("acquired", pool_name)
        return pool.connection()

    def release_pool_connection(self, pool_name: str) -> None:
        """
        Release a connection back to a pool.

        Note: This is handled automatically by the context manager.
        This method is provided for manual release if needed.

        Args:
            pool_name: Pool name.
        """
        context = self._pool_contexts.get(pool_name)
        if context and context.state == ConnectionState.ACTIVE:
            context.state = ConnectionState.IDLE
            self._emit_event("released", pool_name)

    def resize_pool(
        self, name: str, min_size: int, max_size: int
    ) -> None:
        """
        Resize a connection pool.

        Args:
            name: Pool name.
            min_size: New minimum size.
            max_size: New maximum size.

        Raises:
            PoolNotFoundError: If pool not found.
        """
        pool = self.get_pool_safe(name)
        pool.resize(min_size, max_size)
        self._logger.info(f"Resized pool: {name} (min={min_size}, max={max_size})")

    def health_check_pool(self, name: str) -> Dict[str, Any]:
        """
        Perform health check on a pool.

        Args:
            name: Pool name.

        Returns:
            Health check result dictionary.

        Raises:
            PoolNotFoundError: If pool not found.
        """
        pool = self.get_pool_safe(name)
        context = self._pool_contexts.get(name)

        result = {
            "pool_name": name,
            "status": "unknown",
            "metrics": {},
            "timestamp": time.time(),
        }

        try:
            # Get pool metrics
            metrics = pool.metrics
            result["metrics"] = {
                "total_created": metrics.total_created,
                "total_destroyed": metrics.total_destroyed,
                "active_connections": metrics.active_connections,
                "idle_connections": metrics.idle_connections,
                "health_check_failures": metrics.health_check_failures,
            }

            # Check pool status
            if pool.status == PoolStatus.READY:
                result["status"] = "healthy"
            elif pool.status == PoolStatus.SHUTDOWN:
                result["status"] = "shutdown"
            else:
                result["status"] = "degraded"

            # Update context
            if context:
                context.metadata["last_health_check"] = time.time()

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            if context:
                context.error_count += 1

        return result

    def close_pool(self, name: str) -> None:
        """
        Close a connection pool.

        Args:
            name: Pool name.
        """
        with self._lock:
            if name not in self._pools:
                self._logger.warning(f"Cannot close unknown pool: {name}")
                return

            context = self._pool_contexts.get(name)
            if context:
                context.state = ConnectionState.CLOSING

            try:
                pool = self._pools[name]
                pool.close()

                if context:
                    context.state = ConnectionState.CLOSED
                    context.closed_at = time.time()

                del self._pools[name]
                del self._pool_contexts[name]

                self._logger.info(f"Closed pool: {name}")
                self._emit_event("closed", name)

            except Exception as e:
                if context:
                    context.state = ConnectionState.ERROR
                    context.error_count += 1
                self._logger.error(f"Error closing pool '{name}': {e}")
                self._emit_event("error", name, error=e)

    def close_all_pools(self) -> None:
        """Close all pools."""
        with self._lock:
            for name in list(self._pools.keys()):
                try:
                    self.close_pool(name)
                except Exception as e:
                    self._logger.error(f"Error closing pool '{name}': {e}")

    def get_pool_metrics(self, name: str) -> Optional[PoolMetrics]:
        """
        Get pool metrics.

        Args:
            name: Pool name.

        Returns:
            PoolMetrics or None if pool not found.
        """
        pool = self.get_pool(name)
        if pool:
            return pool.metrics
        return None

    def get_all_pool_metrics(self) -> Dict[str, PoolMetrics]:
        """Get metrics for all pools."""
        return {name: pool.metrics for name, pool in self._pools.items()}
