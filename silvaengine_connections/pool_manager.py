#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Connection Pool Manager for silvaengine_connections.

Implements hot-pluggable connection pool management with support for
dynamic registration, configuration updates, and lifecycle management.
"""

import inspect
import logging
import threading
from typing import Any, Dict, List, Optional, Type

from .config import ConfigManager, ConnectionConfig
from .connection import BaseConnection
from .connection_pool import BaseConnectionPool
from .exceptions import (
    PoolAlreadyExistsError,
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
                    instance = super().__new__(cls)
                    instance._initialize(logger)
                    cls._instance = instance
        return cls._instance

    def _initialize(self, logger: Optional[logging.Logger] = None) -> None:
        """
        Initialize the manager instance.

        This method is called once during singleton creation and sets up
        all internal data structures and dependencies.

        Args:
            logger: Optional logger instance. If not provided, a default
                logger for this module will be used.
        """
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
        """
        Get the singleton manager instance.

        This is an alias for the constructor, provided for explicit
        singleton access semantics.

        Returns:
            ConnectionPoolManager: The singleton manager instance.

        Example:
            >>> manager = ConnectionPoolManager.get_instance()
            >>> pool = manager.get_pool("postgres_main")
        """
        return cls()

    def register_connection_type(
        self,
        type_name: str,
        pool_class: Type[BaseConnectionPool],
        connection_class: Type[BaseConnection],
    ) -> None:
        """
        Register a new connection type (hot-pluggable).

        This method allows dynamic registration of new connection types
        at runtime without modifying the core codebase.

        Args:
            type_name: Connection type name (e.g., 'postgresql', 'neo4j').
                Must be unique among registered types.
            pool_class: Connection pool class that inherits from BaseConnectionPool.
            connection_class: Connection class that inherits from BaseConnection.

        Raises:
            PluginAlreadyExistsError: If a connection type with the same
                name is already registered.

        Example:
            >>> manager.register_connection_type(
            ...     "postgresql",
            ...     PostgreSQLPool,
            ...     PostgreSQLConnection
            ... )
        """
        self._plugin_registry.register(type_name, pool_class, connection_class)
        self._logger.info(f"Registered connection type: {type_name}")

    def unregister_connection_type(self, type_name: str) -> bool:
        """
        Unregister a connection type.

        This method removes a connection type registration and closes all
        pools of that type. This is useful for cleanup or when replacing
        a connection type implementation.

        Args:
            type_name: Connection type name to unregister.

        Returns:
            bool: True if the type was successfully unregistered,
                False if the type was not found.

        Example:
            >>> success = manager.unregister_connection_type("postgresql")
            >>> print(success)  # True or False
        """
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

        This method creates a new connection pool with the specified name
        and configuration. The pool type is determined by the config.type field.

        Args:
            name: Unique pool name for identification.
            config: Connection configuration object containing type, settings,
                and pool parameters.

        Returns:
            BaseConnectionPool: The created pool instance.

        Raises:
            PoolAlreadyExistsError: If a pool with the same name already exists.
            PluginNotFoundError: If the connection type is not registered.

        Example:
            >>> config = ConnectionConfig(
            ...     type="postgresql",
            ...     settings={"host": "localhost", "database": "mydb"},
            ...     pool_settings={"min_size": 2, "max_size": 10}
            ... )
            >>> pool = manager.create_pool("postgres_main", config)
        """
        with self._manager_lock:
            if name in self._pools:
                raise PoolAlreadyExistsError(
                    f"Pool '{name}' already exists", pool_name=name
                )

            plugin = self._plugin_registry.get_safe(config.type)
            config = self._config_manager.apply_defaults(config)

            # Only pass pool_settings keys that the pool class's __init__
            # actually accepts.  Keys like ``recycle`` belong to the inner
            # connection (e.g. SQLAlchemy pool_recycle) and are carried via
            # ``config.pool_settings`` → ``PostgreSQLConnection._pool_config``;
            # they must NOT be unpacked into the pool wrapper constructor.
            init_sig = inspect.signature(plugin.pool_class.__init__)
            accepted_params = set(init_sig.parameters.keys())
            pool_kwargs = {
                k: v for k, v in config.pool_settings.items() if k in accepted_params
            }

            pool = plugin.pool_class(
                name=name,
                config=config,
                **pool_kwargs,
            )

            self._pools[name] = pool
            self._pool_locks[name] = threading.RLock()

            self._logger.info(f"Created pool: {name} (type: {config.type})")
            return pool

    def remove_pool(self, name: str) -> bool:
        """
        Remove a connection pool.

        This method closes the pool and removes it from the manager.
        All connections in the pool will be properly closed.

        Args:
            name: Pool name to remove.

        Returns:
            bool: True if the pool was successfully removed,
                False if the pool was not found.

        Example:
            >>> success = manager.remove_pool("postgres_main")
            >>> print(success)  # True or False
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
            name: Pool name to retrieve.

        Returns:
            Optional[BaseConnectionPool]: The pool instance if found,
                None otherwise.

        Example:
            >>> pool = manager.get_pool("postgres_main")
            >>> if pool:
            ...     with pool.connection() as conn:
            ...         result = conn.execute("SELECT 1")
        """
        return self._pools.get(name)

    def get_pool_safe(self, name: str) -> BaseConnectionPool:
        """
        Get a connection pool, raising exception if not found.

        This method is useful when the pool must exist and its absence
        indicates a configuration or programming error.

        Args:
            name: Pool name to retrieve.

        Returns:
            BaseConnectionPool: The pool instance.

        Raises:
            PoolNotFoundError: If no pool with the given name exists.

        Example:
            >>> try:
            ...     pool = manager.get_pool_safe("postgres_main")
            ...     with pool.connection() as conn:
            ...         result = conn.execute("SELECT 1")
            ... except PoolNotFoundError:
            ...     print("Pool not configured")
        """
        pool = self.get_pool(name)

        if pool is None:
            raise PoolNotFoundError(f"Pool '{name}' not found", pool_name=name)
        return pool

    def connection(self, pool_name: Optional[str] = None):
        """
        Get a connection context manager from the specified pool.

        This method provides a convenient way to get a connection directly
        from the manager without explicitly calling get_pool() first.
        If pool_name is not specified, uses the first available pool.

        Args:
            pool_name: Name of the pool to get connection from.
                      If None, uses the first available pool.

        Returns:
            Context manager for connection acquisition.

        Raises:
            PoolNotFoundError: If no pool is found.
            PoolNotReadyError: If pool is not ready.

        Example:
            >>> with manager.connection() as conn:
            ...     result = conn.execute("SELECT 1")
            >>>
            >>> # Or with specific pool name
            >>> with manager.connection("postgres_main") as conn:
            ...     result = conn.execute("SELECT 1")
        """
        if pool_name is None:
            # Try to get the first available pool
            if not self._pools:
                raise PoolNotFoundError("No pools available", pool_name="default")
            pool = next(iter(self._pools.values()))
        else:
            pool = self.get_pool(pool_name)

            if pool is None:
                raise PoolNotFoundError(
                    f"Pool '{pool_name}' not found", pool_name=pool_name
                )

        return pool.connection()

    def initialize_from_config(self, config_dict: Dict[str, Any]) -> List[str]:
        """
        Initialize pools from configuration dictionary.

        This method loads pool configurations from a dictionary and creates
        all enabled pools. Individual pool creation failures are logged but
        do not prevent other pools from being created.

        Args:
            config_dict: Configuration dictionary with the following format:
                {
                    "pools": {
                        "pool_name": {
                            "type": "postgresql",
                            "enabled": True,
                            "settings": {...},
                            "pool": {...}
                        }
                    }
                }

        Returns:
            List[str]: List of successfully created pool names.

        Example:
            >>> config = {
            ...     "pools": {
            ...         "postgres_main": {"type": "postgresql", ...},
            ...         "neo4j_main": {"type": "neo4j", ...}
            ...     }
            ... }
            >>> created = manager.initialize_from_config(config)
            >>> print(created)  # ["postgres_main", "neo4j_main"]
        """
        created = []

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
        """
        Get all registered pools.

        Returns:
            Dict[str, BaseConnectionPool]: A copy of the pools dictionary,
                mapping pool names to pool instances.

        Example:
            >>> pools = manager.get_all_pools()
            >>> for name, pool in pools.items():
            ...     print(f"{name}: {pool.status}")
        """
        return self._pools.copy()

    def get_pool_names(self) -> List[str]:
        """
        Get all pool names.

        Returns:
            List[str]: List of all registered pool names.

        Example:
            >>> names = manager.get_pool_names()
            >>> print(names)  # ["postgres_main", "neo4j_main"]
        """
        return list(self._pools.keys())

    def get_connection_types(self) -> List[str]:
        """
        Get all registered connection types.

        Returns:
            List[str]: List of all registered connection type names
                (e.g., ["postgresql", "neo4j", "httpx", "boto3"]).

        Example:
            >>> types = manager.get_connection_types()
            >>> print(types)  # ["postgresql", "neo4j"]
        """
        return self._plugin_registry.get_all_types()

    def shutdown_all(self) -> None:
        """
        Shutdown all pools.

        This method closes all registered pools and releases their resources.
        It should be called during application shutdown to ensure clean
        connection closure.

        Example:
            >>> manager.shutdown_all()
        """
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
        """
        Get metrics for all pools.

        This method collects metrics from all registered pools, useful
        for monitoring and debugging connection pool health.

        Returns:
            Dict[str, Any]: Dictionary mapping pool names to their metrics.
                Each metrics object contains fields like total_created,
                total_borrowed, active_connections, etc.

        Example:
            >>> metrics = manager.get_all_metrics()
            >>> for name, pool_metrics in metrics.items():
            ...     print(f"{name}: {pool_metrics.active_connections} active")
        """
        return {name: pool.metrics for name, pool in self._pools.items()}

    def reset(self) -> None:
        """
        Reset the manager (for testing).

        This method completely resets the manager state by shutting down
        all pools, clearing registries, and resetting the singleton instance.
        It is primarily intended for use in test environments.

        Warning:
            This method should not be called in production environments
            as it will disrupt all active connections.

        Example:
            >>> manager.reset()  # Clear all state for testing
        """
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
            :return: List of successfully created pool names.
        """
        created = []
        last_error: Optional[Exception] = None

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

                self._logger.info(
                    f"Created connection pool: {pool_name} (type: {config.type})"
                )

            except Exception as e:
                last_error = e
                self._logger.error(f"Failed to create pool {pool_name}: {e}")

        # If no pools were created despite non-empty config, surface the
        # error instead of silently returning an empty list.  This covers
        # both creation failures (last_error set) and all-disabled configs
        # (last_error is None but no pools were created).
        if not created and pools_config:
            if last_error is not None:
                raise last_error
            raise RuntimeError(
                f"No pools were created from {len(pools_config)} config(s) — all disabled or invalid"
            )

        return created

    def reload_configuration(
        self, config: Dict[str, Any], plugin_registry: Optional[Dict[str, Any]] = None
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
                        self._logger.warning(
                            f"Failed to register plugin {plugin_name}: {e}"
                        )

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

        This method provides a convenient way to register multiple connection
        type plugins in a single call. Each plugin is registered independently,
        and failures are logged but do not prevent other plugins from being
        registered.

        Args:
            plugins: Dictionary mapping plugin names to their registration functions.
                Each function should accept a PluginRegistry instance as argument.
                Example:
                {
                    "postgresql": register_postgresql,
                    "neo4j": register_neo4j
                }

        Example:
            >>> def register_postgresql(registry):
            ...     registry.register("postgresql", PostgreSQLPool, PostgreSQLConnection)
            >>> manager.register_plugins({"postgresql": register_postgresql})
        """
        for plugin_name, register_func in plugins.items():
            try:
                register_func(self._plugin_registry)
                self._logger.debug(f"Registered plugin: {plugin_name}")
            except Exception as e:
                self._logger.warning(f"Failed to register plugin {plugin_name}: {e}")
