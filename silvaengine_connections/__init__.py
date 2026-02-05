#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
silvaengine_connections - Hot-pluggable connection pool management for AWS Lambda.

This package provides a comprehensive connection pool management system with
support for PostgreSQL, Neo4j, HTTPX, and Boto3 connections.

Features:
- Hot-pluggable connection type registration
- Context manager support for safe connection handling
- Dynamic pool resizing and health monitoring
- Comprehensive metrics and statistics
- Thread-safe operations

Example:
    ```python
    from silvaengine_connections import ConnectionPoolManager

    # Get the singleton manager instance
    manager = ConnectionPoolManager()

    # Initialize from configuration
    config = {
        "pools": {
            "postgres_main": {
                "type": "postgresql",
                "enabled": True,
                "params": {"url": "postgresql://user:pass@host/db"},
                "pool": {"min_size": 2, "max_size": 10}
            }
        }
    }
    manager.initialize_from_config(config)

    # Use connection with context manager
    with manager.get_pool("postgres_main").connection() as conn:
        result = conn.execute("SELECT 1")
    ```
"""

from typing import Any, Dict

# Core components
from .connection import BaseConnection
from .connection_pool import BaseConnectionPool, PoolMetrics, PoolStatus
from .pool_manager import ConnectionPoolManager
from .plugin_registry import PluginRegistry, ConnectionPlugin
from .config import ConnectionConfig, ConfigManager

# Exceptions
from .exceptions import (
    ConnectionError,
    ConnectionTimeoutError,
    ConnectionFailedError,
    AuthenticationError,
    ConnectionNotFoundError,
    ConnectionClosedError,
    PoolError,
    PoolExhaustedError,
    PoolNotReadyError,
    PoolNotFoundError,
    PoolAlreadyExistsError,
    PoolManagerError,
    PluginNotFoundError,
    PluginAlreadyExistsError,
    ConfigurationError,
    ConfigValidationError,
    ConfigNotFoundError,
    HealthCheckError,
)

# Try to import optional connection implementations
try:
    from .connections.postgresql import PostgreSQLConnection, PostgreSQLPool
except ImportError:
    PostgreSQLConnection = None
    PostgreSQLPool = None

try:
    from .connections.neo4j import Neo4jConnection, Neo4jPool
except ImportError:
    Neo4jConnection = None
    Neo4jPool = None

try:
    from .connections.httpx import HTTPXConnection, HTTPXPool
except ImportError:
    HTTPXConnection = None
    HTTPXPool = None

try:
    from .connections.boto3 import Boto3Connection, Boto3Pool
except ImportError:
    Boto3Connection = None
    Boto3Pool = None


# Alias for backward compatibility with standard configuration format
PoolManager = ConnectionPoolManager


def init(config: Dict[str, Any]) -> ConnectionPoolManager:
    """
    Initialize connection pools from standard configuration format.

    This function serves as the unified entry point for initializing
    connection pools according to the standard configuration format.

    Supports two configuration formats:

    1. Standard format (from PluginManager):
        Direct connection pool configuration without wrapper keys.
        {
            "postgresql": {
                "type": "postgresql",
                "enabled": True,
                "settings": {...},
                "pool": {...}
            },
            "neo4j": {
                "type": "neo4j",
                "enabled": True,
                "settings": {...},
                "pool": {...}
            }
        }

    2. Legacy format (with resources key):
        {
            "resources": {
                "neo4j": {...},
                "postgresql": {...}
            },
            "enabled": True
        }

    Args:
        config: Configuration dictionary containing connection pool settings.
                When called from PluginManager, this is the content of the
                'config' field in plugin configuration.

    Returns:
        ConnectionPoolManager: Initialized manager instance with configured pools.

    Example:
        ```python
        from silvaengine_connections import init

        # Standard format (recommended)
        config = {
            "postgresql": {
                "type": "postgresql",
                "enabled": True,
                "settings": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "mydb",
                    "username": "user",
                    "password": "pass"
                },
                "pool": {
                    "min_size": 2,
                    "max_size": 10
                }
            }
        }

        manager = init(config)
        pool = manager.get_pool("postgresql")
        ```
    """
    manager = ConnectionPoolManager()

    if not isinstance(config, dict):
        return manager

    # Check for legacy format with top-level 'enabled' field
    if config.get("enabled") is False:
        return manager

    # Determine configuration format
    if "resources" in config:
        # Legacy format with resources key
        pools_config = config.get("resources", {})
    else:
        # Standard format: config is directly the pools configuration
        # Filter out reserved keys that are not pool configurations
        reserved_keys = {
            "enabled", "type", "module_name",
            "class_name", "function_name"
        }
        pools_config = {
            name: pool_config
            for name, pool_config in config.items()
            if isinstance(pool_config, dict) and name not in reserved_keys
        }

    # Process pool configurations
    processed_config = {}
    for name, pool_config in pools_config.items():
        if isinstance(pool_config, dict):
            # Ensure type is set if not present
            if "type" not in pool_config:
                pool_config = {**pool_config, "type": name}
            processed_config[name] = pool_config

    if processed_config:
        manager.create_pools_from_config(processed_config)

    return manager


__all__ = [
    # Core classes
    "BaseConnection",
    "BaseConnectionPool",
    "ConnectionPoolManager",
    "PoolManager",  # Alias for backward compatibility
    "PluginRegistry",
    "ConnectionPlugin",
    "ConnectionConfig",
    "ConfigManager",
    "PoolMetrics",
    "PoolStatus",

    # Functions
    "init",

    # Exceptions
    "ConnectionError",
    "ConnectionTimeoutError",
    "ConnectionFailedError",
    "AuthenticationError",
    "ConnectionNotFoundError",
    "ConnectionClosedError",
    "PoolError",
    "PoolExhaustedError",
    "PoolNotReadyError",
    "PoolNotFoundError",
    "PoolAlreadyExistsError",
    "PoolManagerError",
    "PluginNotFoundError",
    "PluginAlreadyExistsError",
    "ConfigurationError",
    "ConfigValidationError",
    "ConfigNotFoundError",
    "HealthCheckError",

    # Connection implementations (may be None if dependencies not installed)
    "PostgreSQLConnection",
    "PostgreSQLPool",
    "Neo4jConnection",
    "Neo4jPool",
    "HTTPXConnection",
    "HTTPXPool",
    "Boto3Connection",
    "Boto3Pool",
]
