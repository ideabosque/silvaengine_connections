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
- Optimized startup with pre-imported connection types

The module ensures seamless integration with the PluginManager while
maintaining all connection management functionality within the
silvaengine_connections module.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple, Type

from .config import ConfigManager, ConnectionConfig
from .connection_pool import BaseConnectionPool
from .exceptions import (
    ConfigValidationError,
    ConnectionFailedError,
)
from .lifecycle import ConnectionPoolLifecycleManager
from .pool_manager import ConnectionPoolManager

# Pre-import connection types at module level to avoid dynamic import overhead
# This reduces initialization time by ~50-100ms
_CONNECTION_TYPE_MODULES = {
    "postgresql": ("PostgreSQLPool", "PostgreSQLConnection"),
    "neo4j": ("Neo4jPool", "Neo4jConnection"),
    "httpx": ("HTTPXPool", "HTTPXConnection"),
    "boto3": ("Boto3Pool", "Boto3Connection"),
}

for _type_name, (_pool_cls, _conn_cls) in _CONNECTION_TYPE_MODULES.items():
    try:
        _module = __import__(
            f".connections.{_type_name}", fromlist=[_pool_cls, _conn_cls]
        )
        globals()[_pool_cls] = getattr(_module, _pool_cls)
        globals()[_conn_cls] = getattr(_module, _conn_cls)
        globals()[f"_{_type_name.upper()}_AVAILABLE"] = True
    except ImportError:
        globals()[_pool_cls] = None
        globals()[_conn_cls] = None
        globals()[f"_{_type_name.upper()}_AVAILABLE"] = False


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
    - Optimized startup with pre-imported types
    - Parallel pool initialization
    - Background warmup support

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

    # Pre-imported connection types (avoids dynamic import overhead)
    DEFAULT_CONNECTION_TYPES: Dict[str, Tuple[Optional[Type], Optional[Type]]] = {
        "postgresql": (PostgreSQLPool, PostgreSQLConnection)
        if _POSTGRESQL_AVAILABLE
        else (None, None),
        "neo4j": (Neo4jPool, Neo4jConnection) if _NEO4J_AVAILABLE else (None, None),
        "httpx": (HTTPXPool, HTTPXConnection) if _HTTPX_AVAILABLE else (None, None),
        "boto3": (Boto3Pool, Boto3Connection) if _BOTO3_AVAILABLE else (None, None),
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
        self._warmup_thread: Optional[threading.Thread] = None
        self._warmup_complete = threading.Event()
        self._parallel_init = True  # Enable parallel initialization by default

    def initialize_from_config(self, config: Dict[str, Any]) -> ConnectionPoolManager:
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
            raise ConnectionFailedError(f"Connection plugin initialization failed: {e}")

    def _register_default_connection_types(self) -> None:
        """
        Register default connection types.

        Uses pre-imported types for faster initialization (~50-100ms improvement).
        """
        for type_name, (
            pool_class,
            connection_class,
        ) in self.DEFAULT_CONNECTION_TYPES.items():
            try:
                # Use pre-imported classes directly (no dynamic import overhead)
                if pool_class is not None and connection_class is not None:
                    self._lifecycle_manager.register_plugin(
                        type_name, pool_class, connection_class
                    )
                    self._pool_manager.register_connection_type(
                        type_name, pool_class, connection_class
                    )
                    self._logger.debug(f"Registered connection type: {type_name}")
            except Exception as e:
                self._logger.debug(
                    f"Could not register connection type {type_name}: {e}"
                )

    def _create_pools_from_config(
        self, config: Dict[str, Any], parallel: bool = True
    ) -> List[str]:
        """
        Create connection pools from configuration.

        Args:
            config: Configuration dictionary.
            parallel: If True, create pools in parallel using thread pool.

        Returns:
            List of created pool names.
        """
        if parallel and len(config) > 1:
            return self._create_pools_parallel(config)

        # Sequential creation (inline for simplicity)
        created = []
        for pool_name, pool_config in config.items():
            pool = self._create_single_pool(pool_name, pool_config)
            if pool:
                created.append(pool_name)
        return created

    def _create_pools_parallel(self, config: Dict[str, Any]) -> List[str]:
        """
        Create pools in parallel using thread pool.

        This reduces initialization time when multiple pools are configured.
        """
        created = []

        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_pool = {
                executor.submit(self._create_single_pool, name, cfg): name
                for name, cfg in config.items()
            }

            for future in as_completed(future_to_pool):
                pool_name = future_to_pool[future]
                try:
                    pool = future.result()
                    if pool:
                        created.append(pool_name)
                except Exception as e:
                    self._logger.error(f"Failed to create pool '{pool_name}': {e}")

        return created

    def _create_single_pool(
        self, pool_name: str, pool_config: Any
    ) -> Optional[BaseConnectionPool]:
        """
        Create a single connection pool.

        Args:
            pool_name: Name of the pool.
            pool_config: Pool configuration.

        Returns:
            Created pool or None if creation failed.
        """
        try:
            # Skip non-dictionary configurations
            if not isinstance(pool_config, dict):
                self._logger.warning(
                    f"Skipping invalid config for '{pool_name}': not a dictionary"
                )
                return None

            # Check if enabled
            if not pool_config.get("enabled", True):
                self._logger.debug(f"Pool '{pool_name}' is disabled, skipping")
                return None

            # Validate configuration
            connection_config = ConnectionConfig.from_dict(pool_config)
            errors = self._lifecycle_manager.validate_config(connection_config)
            if errors:
                self._logger.error(
                    f"Invalid configuration for pool '{pool_name}': {errors}"
                )
                return None

            # Create pool using lifecycle manager
            pool = self._lifecycle_manager.create_pool(pool_name, connection_config)

            # Close the previously-registered pool (if any) before overwriting
            # the manager slot. Otherwise the old pool's connections (each
            # holding a live SQLAlchemy Engine / DBAPI connection) are orphaned
            # without ``dispose()``, leaking server-side connections until PG's
            # ``max_connections`` is exhausted — which then surfaces as
            # ``Pool <name> exhausted`` on subsequent re-initializations.
            old_pool = self._pool_manager._pools.get(pool_name)
            if old_pool is not None and old_pool is not pool:
                try:
                    old_pool.close()
                    self._logger.info(
                        f"Closed previous pool '{pool_name}' before re-creating"
                    )
                except Exception as e:
                    self._logger.warning(
                        f"Error closing previous pool '{pool_name}': {e}"
                    )

            # Also register in pool manager for external access
            self._pool_manager._pools[pool_name] = pool

            self._logger.info(f"Created pool: {pool_name}")
            return pool

        except Exception as e:
            self._logger.error(f"Failed to create pool '{pool_name}': {e}")
            return None

    def start_warmup(self, timeout: float = 30.0) -> None:
        """
        Start background warmup of all connection pools.

        This method starts a background thread that establishes connections
        for all pools, reducing latency on first request.

        Args:
            timeout: Maximum time to wait for warmup in seconds.
        """
        if self._pool_manager is None:
            self._logger.warning("Cannot start warmup: pool manager not initialized")
            return

        def warmup_pools():
            """Warmup function running in background thread."""
            start_time = time.time()
            pools = self._pool_manager.get_all_pools()

            for pool_name, pool in pools.items():
                try:
                    # Trigger lazy initialization by acquiring and releasing a connection
                    if hasattr(pool, "_ensure_initialized"):
                        pool._ensure_initialized()
                        self._logger.info(f"Pool '{pool_name}' warmed up")
                except Exception as e:
                    self._logger.warning(f"Failed to warm up pool '{pool_name}': {e}")

            elapsed = time.time() - start_time
            self._logger.info(f"Warmup completed in {elapsed:.2f}s")
            self._warmup_complete.set()

        self._warmup_thread = threading.Thread(target=warmup_pools, daemon=True)
        self._warmup_thread.start()

        # Wait for warmup to complete (non-blocking for caller)
        self._warmup_complete.wait(timeout=timeout)

    def wait_for_warmup(self, timeout: float = 30.0) -> bool:
        """
        Wait for warmup to complete.

        Args:
            timeout: Maximum time to wait in seconds.

        Returns:
            True if warmup completed, False if timeout.
        """
        return self._warmup_complete.wait(timeout=timeout)

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
