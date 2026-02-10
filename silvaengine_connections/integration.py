#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Plugin Integration Module for silvaengine_connections.

This module provides standardized integration between silvaengine_base's
PluginManager and silvaengine_connections, enabling:
- Automatic plugin registration and initialization
- Standardized configuration loading and validation
- Complete connection lifecycle management
- Error handling and recovery
- Metrics collection and monitoring

The module ensures seamless integration with the PluginManager while
maintaining all connection management functionality within the
silvaengine_connections module.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Type

from .config import ConnectionConfig, ConfigManager
from .connection import BaseConnection
from .connection_pool import BaseConnectionPool
from .exceptions import (
    ConfigValidationError,
    ConnectionFailedError,
    PluginNotFoundError,
    PoolAlreadyExistsError,
)
from .lifecycle import ConnectionPoolLifecycleManager, ConnectionState
from .plugin_registry import ConnectionPlugin, PluginRegistry
from .pool_manager import ConnectionPoolManager


class ConnectionPluginIntegration:
    """
    Connection Plugin Integration for PluginManager.

    This class provides the standardized interface between PluginManager
    and silvaengine_connections, handling:
    - Plugin initialization from configuration
    - Connection type registration
    - Pool creation and management
    - Configuration validation
    - Error handling

    Example:
        ```python
        # In PluginManager initialization
        from silvaengine_connections import ConnectionPluginIntegration

        integration = ConnectionPluginIntegration()
        manager = integration.initialize_from_config(config)

        # Use the connection pool manager
        pool = manager.get_pool("postgres_main")
        with pool.connection() as conn:
            result = conn.execute("SELECT 1")
        ```
    """

    # Default connection type mappings
    DEFAULT_CONNECTION_TYPES = {
        "postgresql": (
            "silvaengine_connections.connections.postgresql.PostgreSQLPool",
            "silvaengine_connections.connections.postgresql.PostgreSQLConnection",
        ),
        "neo4j": (
            "silvaengine_connections.connections.neo4j.Neo4jPool",
            "silvaengine_connections.connections.neo4j.Neo4jConnection",
        ),
        "httpx": (
            "silvaengine_connections.connections.httpx.HTTPXPool",
            "silvaengine_connections.connections.httpx.HTTPXConnection",
        ),
        "boto3": (
            "silvaengine_connections.connections.boto3.Boto3Pool",
            "silvaengine_connections.connections.boto3.Boto3Connection",
        ),
    }

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the integration.

        Args:
            logger: Optional logger instance.
        """
        self._logger = logger or logging.getLogger(__name__)
        self._lifecycle_manager = ConnectionPoolLifecycleManager(logger)
        self._config_manager = ConfigManager()
        self._initialized = False
        self._pool_manager: Optional[ConnectionPoolManager] = None

    def initialize_from_config(
        self, config: Dict[str, Any]
    ) -> ConnectionPoolManager:
        """
        Initialize connection pools from PluginManager configuration.

        This is the main entry point for PluginManager integration.
        It handles the complete initialization process:
        1. Register default connection types
        2. Validate configuration
        3. Create connection pools
        4. Return the pool manager

        Args:
            config: Configuration dictionary from PluginManager.
                Expected format:
                {
                    "postgres_main": {
                        "type": "postgresql",
                        "enabled": True,
                        "settings": {...},
                        "pool": {...}
                    },
                    "neo4j_main": {...}
                }

        Returns:
            ConnectionPoolManager: Initialized pool manager.

        Raises:
            ConfigValidationError: If configuration is invalid.
            ConnectionFailedError: If initialization fails.
        """
        self._logger.info("Initializing connection plugin from configuration")

        try:
            # Register default connection types
            self._register_default_connection_types()

            # Get or create pool manager
            self._pool_manager = ConnectionPoolManager(self._logger)

            # Validate and process configuration
            if not isinstance(config, dict):
                raise ConfigValidationError("Configuration must be a dictionary")

            # Create pools from configuration
            created_pools = self._create_pools_from_config(config)

            self._initialized = True
            self._logger.info(
                f"Connection plugin initialized successfully "
                f"with {len(created_pools)} pools: {created_pools}"
            )

            return self._pool_manager

        except Exception as e:
            self._logger.error(f"Failed to initialize connection plugin: {e}")
            raise ConnectionFailedError(
                f"Connection plugin initialization failed: {e}"
            )

    def _register_default_connection_types(self) -> None:
        """Register default connection types."""
        for type_name, (pool_path, conn_path) in self.DEFAULT_CONNECTION_TYPES.items():
            try:
                pool_class = self._import_class(pool_path)
                conn_class = self._import_class(conn_path)

                if pool_class and conn_class:
                    self._lifecycle_manager.register_plugin(
                        type_name, pool_class, conn_class
                    )
                    self._pool_manager.register_connection_type(
                        type_name, pool_class, conn_class
                    )
                    self._logger.debug(f"Registered connection type: {type_name}")
            except Exception as e:
                self._logger.debug(
                    f"Could not register connection type {type_name}: {e}"
                )

    def _import_class(self, class_path: str) -> Optional[Type]:
        """
        Import a class from its full path.

        Args:
            class_path: Full class path (e.g., 'module.submodule.ClassName').

        Returns:
            Imported class or None if import fails.
        """
        try:
            module_path, class_name = class_path.rsplit(".", 1)
            module = __import__(module_path, fromlist=[class_name])
            return getattr(module, class_name)
        except (ImportError, AttributeError) as e:
            self._logger.debug(f"Could not import {class_path}: {e}")
            return None

    def _create_pools_from_config(
        self, config: Dict[str, Any]
    ) -> List[str]:
        """
        Create connection pools from configuration.

        Args:
            config: Configuration dictionary.

        Returns:
            List of created pool names.
        """
        created = []

        for pool_name, pool_config in config.items():
            try:
                # Skip non-dictionary configurations
                if not isinstance(pool_config, dict):
                    self._logger.warning(
                        f"Skipping invalid config for '{pool_name}': not a dictionary"
                    )
                    continue

                # Check if enabled
                if not pool_config.get("enabled", True):
                    self._logger.debug(f"Pool '{pool_name}' is disabled, skipping")
                    continue

                # Validate configuration
                connection_config = ConnectionConfig.from_dict(pool_config)
                errors = self._lifecycle_manager.validate_config(connection_config)
                if errors:
                    self._logger.error(
                        f"Invalid configuration for pool '{pool_name}': {errors}"
                    )
                    continue

                # Create pool using lifecycle manager
                pool = self._lifecycle_manager.create_pool(pool_name, connection_config)
                
                # Also register in pool manager for external access
                self._pool_manager._pools[pool_name] = pool
                
                created.append(pool_name)
                self._logger.info(f"Created pool: {pool_name}")

            except Exception as e:
                self._logger.error(f"Failed to create pool '{pool_name}': {e}")
                # Continue with other pools even if one fails
                continue

        return created

    def get_pool_manager(self) -> Optional[ConnectionPoolManager]:
        """
        Get the connection pool manager.

        Returns:
            ConnectionPoolManager or None if not initialized.
        """
        return self._pool_manager

    def get_lifecycle_manager(self) -> ConnectionPoolLifecycleManager:
        """
        Get the lifecycle manager.

        Returns:
            ConnectionPoolLifecycleManager instance.
        """
        return self._lifecycle_manager

    def is_initialized(self) -> bool:
        """Check if the integration is initialized."""
        return self._initialized

    def shutdown(self) -> None:
        """Shutdown all connections and pools."""
        if self._lifecycle_manager:
            self._lifecycle_manager.close_all_pools()
        self._initialized = False
        self._logger.info("Connection plugin shutdown complete")


class ConnectionPluginInitializer:
    """
    Standard initializer for PluginManager integration.

    This class provides a standardized initialization interface that
    PluginManager can call to initialize the connection plugin.

    Example:
        ```python
        # In PluginManager
        from silvaengine_connections import ConnectionPluginInitializer

        initializer = ConnectionPluginInitializer()
        manager = initializer.init(config)
        ```
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        """
        Initialize the initializer.

        Args:
            logger: Optional logger instance.
        """
        self._logger = logger or logging.getLogger(__name__)
        self._integration = ConnectionPluginIntegration(logger)

    def init(self, config: Dict[str, Any]) -> ConnectionPoolManager:
        """
        Initialize connection pools (standard PluginManager interface).

        Args:
            config: Configuration dictionary from PluginManager.

        Returns:
            ConnectionPoolManager: Initialized pool manager.
        """
        return self._integration.initialize_from_config(config)

    def get_integration(self) -> ConnectionPluginIntegration:
        """
        Get the integration instance.

        Returns:
            ConnectionPluginIntegration instance.
        """
        return self._integration


def create_connection_plugin_initializer(
    logger: Optional[logging.Logger] = None,
) -> ConnectionPluginInitializer:
    """
    Factory function to create a connection plugin initializer.

    Args:
        logger: Optional logger instance.

    Returns:
        ConnectionPluginInitializer instance.
    """
    return ConnectionPluginInitializer(logger)


# Standard entry point for PluginManager
def init(config: Dict[str, Any]) -> ConnectionPoolManager:
    """
    Standard initialization function for PluginManager.

    This function serves as the unified entry point for PluginManager
    to initialize the connection plugin.

    Args:
        config: Configuration dictionary from PluginManager.

    Returns:
        ConnectionPoolManager: Initialized pool manager.

    Example:
        ```python
        # In PluginManager configuration
        {
            "type": "connection_pool",
            "config": {
                "postgres_main": {
                    "type": "postgresql",
                    "enabled": True,
                    "settings": {...},
                    "pool": {...}
                }
            },
            "enabled": True,
            "module_name": "silvaengine_connections",
            "function_name": "init"
        }
        ```
    """
    initializer = ConnectionPluginInitializer()
    return initializer.init(config)
