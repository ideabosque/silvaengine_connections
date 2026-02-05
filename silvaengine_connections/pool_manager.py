#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Connection Pool Manager for silvaengine_connections.

Implements hot-pluggable connection pool management with support for
dynamic registration, configuration updates, and lifecycle management.
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Type

from .config import ConfigManager, ConnectionConfig
from .connection import BaseConnection
from .connection_pool import BaseConnectionPool
from .exceptions import (
    PluginNotFoundError,
    PoolAlreadyExistsError,
    PoolManagerError,
    PoolNotFoundError,
)
from .plugin_registry import PluginRegistry


class ConnectionPoolManager:
    """
    Singleton manager for connection pools with hot-pluggable configuration.

    This class manages multiple connection pool instances, providing:
    - Dynamic pool registration and unregistration
    - Hot configuration reloading
    - Health monitoring
    - Statistics aggregation
    - Lifecycle management

    The manager is implemented as a singleton to ensure consistent
    pool management across the application.

    Example:
        ```python
        # Get manager instance
        manager = ConnectionPoolManager()

        # Register a pool
        manager.register_pool('postgres_main', PostgreSQLPool, config)

        # Get pool instance
        pool = manager.get_pool('postgres_main')

        # Use pool
        with pool.connection() as conn:
            pass

        # Hot reload configuration
        manager.reload_pool('postgres_main', new_config)

        # Shutdown all pools
        manager.shutdown_all()
        ```
    """

    _instance: Optional["ConnectionPoolManager"] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(
        cls, logger: Optional[logging.Logger] = None
    ) -> "ConnectionPoolManager":
        """
        Create or return the singleton instance.

        Args:
            logger: Optional logger instance.

        Returns:
            ConnectionPoolManager: The singleton instance.
        """
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize(logger)
        return cls._instance

    def _initialize(self, logger: Optional[logging.Logger] = None) -> None:
        """Initialize the manager instance."""
        self._logger = logger or logging.getLogger(__name__)
        self._pools: Dict[str, BaseConnectionPool] = {}
        self._pool_locks: Dict[str, threading.RLock] = {}
        self._manager_lock = threading.RLock()
        self._plugin_registry = PluginRegistry()
        self._config_manager = ConfigManager()
        self._initialized = True
        self._logger.info("ConnectionPoolManager initialized")

    @classmethod
    def get_instance(cls) -> "ConnectionPoolManager":
        """Get manager instance."""
        return cls()

    def register_connection_type(
        self,
        type_name: str,
        pool_class: Type[BaseConnectionPool],
        connection_class: Type[BaseConnection],
    ) -> None:
        """
        Register a new connection type (hot-pluggable).

        Args:
            type_name: Connection type name (e.g., 'postgresql', 'neo4j')
            pool_class: Connection pool class
            connection_class: Connection class
        """
        self._plugin_registry.register(type_name, pool_class, connection_class)
        self._logger.info(f"Registered connection type: {type_name}")

    def unregister_connection_type(self, type_name: str) -> bool:
        """
        Unregister a connection type.

        Args:
            type_name: Connection type name

        Returns:
            bool: True if unregistered
        """
        # Close all pools of this type first
        pools_to_remove = [
            name
            for name, pool in self._pools.items()
            if pool.get_pool_type() == type_name
        ]

        for name in pools_to_remove:
            self.remove_pool(name)

        result = self._plugin_registry.unregister(type_name)

        if result:
            self._logger.info(f"Unregistered connection type: {type_name}")
        return result

    def create_pool(
        self,
        name: str,
        config: ConnectionConfig,
    ) -> BaseConnectionPool:
        """
        Create a connection pool from configuration.

        Args:
            name: Pool name
            config: Connection configuration

        Returns:
            BaseConnectionPool: Created pool
        """
        with self._manager_lock:
            if name in self._pools:
                raise PoolAlreadyExistsError(
                    f"Pool '{name}' already exists", pool_name=name
                )

            # Get plugin
            plugin = self._plugin_registry.get_safe(config.type)

            # Apply defaults
            config = self._config_manager.apply_defaults(config)

            # Create pool - pass config object and pool settings
            pool = plugin.pool_class(
                name=name,
                config=config,
                **config.pool_settings,
            )

            self._pools[name] = pool
            self._pool_locks[name] = threading.RLock()

            self._logger.info(f"Created pool: {name} (type: {config.type})")
            return pool

    def remove_pool(self, name: str) -> bool:
        """
        Remove a connection pool.

        Args:
            name: Pool name

        Returns:
            bool: True if removed
        """
        with self._manager_lock:
            if name not in self._pools:
                return False

            pool = self._pools[name]
            pool.close()

            del self._pools[name]
            del self._pool_locks[name]

            self._logger.info(f"Removed pool: {name}")
            return True

    def get_pool(self, name: str) -> Optional[BaseConnectionPool]:
        """
        Get a connection pool by name.

        Args:
            name: Pool name

        Returns:
            Optional[BaseConnectionPool]: Pool or None
        """
        return self._pools.get(name)

    def get_pool_safe(self, name: str) -> BaseConnectionPool:
        """
        Get a connection pool, raising exception if not found.

        Args:
            name: Pool name

        Returns:
            BaseConnectionPool: Pool

        Raises:
            PoolNotFoundError: If pool not found
        """
        pool = self.get_pool(name)
        if pool is None:
            raise PoolNotFoundError(f"Pool '{name}' not found", pool_name=name)
        return pool

    def initialize_from_config(self, config_dict: Dict[str, Any]) -> List[str]:
        """
        Initialize pools from configuration dictionary.

        Args:
            config_dict: Configuration dictionary

        Returns:
            List[str]: List of created pool names
        """
        created = []

        # Load configurations
        config_names = self._config_manager.load_from_dict(config_dict)

        for name in config_names:
            config = self._config_manager.get_config(name)
            if not config.enabled:
                continue

            try:
                pool = self.create_pool(name, config)
                created.append(name)
            except Exception as e:
                self._logger.error(f"Failed to create pool '{name}': {e}")

        return created

    def get_all_pools(self) -> Dict[str, BaseConnectionPool]:
        """Get all pools."""
        return self._pools.copy()

    def get_pool_names(self) -> List[str]:
        """Get all pool names."""
        return list(self._pools.keys())

    def get_connection_types(self) -> List[str]:
        """Get all registered connection types."""
        return self._plugin_registry.get_all_types()

    def shutdown_all(self) -> None:
        """Shutdown all pools."""
        with self._manager_lock:
            for name, pool in list(self._pools.items()):
                try:
                    pool.close()
                    self._logger.info(f"Shutdown pool: {name}")
                except Exception as e:
                    self._logger.error(f"Error shutting down pool '{name}': {e}")

            self._pools.clear()
            self._pool_locks.clear()

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get metrics for all pools."""
        return {name: pool.metrics for name, pool in self._pools.items()}

    def reset(self) -> None:
        """Reset the manager (for testing)."""
        with self._lock:
            with self._manager_lock:
                self.shutdown_all()
                self._plugin_registry.clear()
                self._config_manager.clear()
                ConnectionPoolManager._instance = None

    def create_pools_from_config(self, pools_config: Dict[str, Any]) -> List[str]:
        """
        Create multiple connection pools from configuration.

        This method batch creates connection pools based on the provided
        configuration dictionary. It handles individual pool creation
        failures gracefully, continuing with remaining pools.

        Args:
            pools_config: Dictionary mapping pool names to their configurations.
                Example:
                {
                    "postgres_main": {
                        "type": "postgresql",
                        "enabled": True,
                        "settings": {...},
                        "pool": {...}
                    }
                }

        Returns:
            List[str]: List of successfully created pool names.
        """
        created = []

        for pool_name, pool_config in pools_config.items():
            try:
                if not pool_config.get("enabled", True):
                    self._logger.debug(f"Pool {pool_name} is disabled, skipping")
                    continue

                # Convert to ConnectionConfig
                config = ConnectionConfig.from_dict(pool_config)

                # Create connection pool
                self.create_pool(pool_name, config)
                created.append(pool_name)
                self._logger.info(f"Created connection pool: {pool_name} (type: {config.type})")

            except Exception as e:
                self._logger.error(f"Failed to create pool {pool_name}: {e}")

        return created

    def reload_configuration(
        self,
        config: Dict[str, Any],
        plugin_registry: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Hot reload configuration.

        This method shuts down all existing pools and reinitializes
        with the new configuration. It can optionally register new
        plugins before creating pools.

        Args:
            config: Full configuration dictionary including pools and plugins.
            plugin_registry: Optional dictionary of plugin_name -> register_function
                to register before creating pools.

        Returns:
            bool: True if reload succeeded, False otherwise.
        """
        self._logger.info("Reloading resource pool configuration")

        try:
            # Shutdown all existing pools
            self.shutdown_all()

            # Register plugins if provided
            if plugin_registry:
                for plugin_name, register_func in plugin_registry.items():
                    try:
                        register_func(self._plugin_registry)
                        self._logger.debug(f"Registered plugin: {plugin_name}")
                    except Exception as e:
                        self._logger.warning(f"Failed to register plugin {plugin_name}: {e}")

            # Create pools from new configuration
            if config and config.get("enabled", True):
                pools_config = config.get("pools", {})
                self.create_pools_from_config(pools_config)

            self._logger.info("Configuration reload completed")
            return True

        except Exception as e:
            self._logger.error(f"Failed to reload configuration: {e}")
            return False

    def register_plugins(self, plugins: Dict[str, Any]) -> None:
        """
        Register multiple plugins at once.

        Args:
            plugins: Dictionary mapping plugin names to their registration functions.
                Each function should accept a PluginRegistry instance as argument.
        """
        for plugin_name, register_func in plugins.items():
            try:
                register_func(self._plugin_registry)
                self._logger.debug(f"Registered plugin: {plugin_name}")
            except Exception as e:
                self._logger.warning(f"Failed to register plugin {plugin_name}: {e}")
