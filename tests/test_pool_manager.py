#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for ConnectionPoolManager.

This module provides comprehensive tests for the ConnectionPoolManager class,
including singleton pattern, pool lifecycle, plugin registration, and
configuration management.
"""

import logging
import threading
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, Mock, patch

from silvaengine_connections.connection import BaseConnection
from silvaengine_connections.connection_pool import BaseConnectionPool, PoolMetrics, PoolStatus
from silvaengine_connections.config import ConnectionConfig
from silvaengine_connections.exceptions import (
    PoolAlreadyExistsError,
    PoolNotFoundError,
    PluginAlreadyExistsError,
    PluginNotFoundError,
)
from silvaengine_connections.pool_manager import ConnectionPoolManager


class MockConnection(BaseConnection):
    """Mock connection for testing."""

    _id_counter = 0

    def __init__(self):
        MockConnection._id_counter += 1
        self.connection_id = MockConnection._id_counter
        self.is_used = False
        self._closed = False

    def is_healthy(self) -> bool:
        return not self._closed

    def get_lifetime(self) -> float:
        return 0.0

    def get_idle_time(self) -> float:
        return 0.0

    def reset(self) -> None:
        self.is_used = False

    def close(self) -> None:
        self._closed = True


class MockPool(BaseConnectionPool[MockConnection]):
    """Mock pool implementation for testing."""

    def __init__(self, name: str = "test_pool", config=None, **kwargs):
        self._test_config = config
        super().__init__(
            name=name,
            connection_class=MockConnection,
            min_size=kwargs.get("min_size", 2),
            max_size=kwargs.get("max_size", 10),
            max_idle_time=kwargs.get("max_idle_time", 300.0),
            max_lifetime=kwargs.get("max_lifetime", 3600.0),
            wait_timeout=kwargs.get("wait_timeout", 10.0),
            health_check_interval=kwargs.get("health_check_interval", 30.0),
            enable_dynamic_resize=kwargs.get("enable_dynamic_resize", True),
        )

    def get_pool_type(self) -> str:
        return "mock"

    def _create_connection(self) -> MockConnection:
        return MockConnection()


class TestConnectionPoolManagerSingleton(unittest.TestCase):
    """Tests for ConnectionPoolManager singleton pattern."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None

    def tearDown(self):
        """Clean up after tests."""
        if ConnectionPoolManager._instance is not None:
            ConnectionPoolManager._instance.reset()
            ConnectionPoolManager._instance = None

    def test_singleton_returns_same_instance(self):
        """Test that constructor returns the same instance."""
        manager1 = ConnectionPoolManager()
        manager2 = ConnectionPoolManager()

        self.assertIs(manager1, manager2)

    def test_get_instance_returns_singleton(self):
        """Test that get_instance returns the singleton."""
        manager1 = ConnectionPoolManager()
        manager2 = ConnectionPoolManager.get_instance()

        self.assertIs(manager1, manager2)

    def test_singleton_with_logger(self):
        """Test singleton creation with custom logger."""
        logger = logging.getLogger("test_logger")

        manager = ConnectionPoolManager(logger=logger)

        self.assertEqual(manager._logger.name, "test_logger")


class TestConnectionPoolManagerPluginRegistration(unittest.TestCase):
    """Tests for plugin registration methods."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_register_connection_type(self):
        """Test registering a new connection type."""
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

        types = self.manager.get_connection_types()

        self.assertIn("mock", types)

    def test_register_duplicate_connection_type_raises_error(self):
        """Test that registering duplicate type raises error."""
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

        with self.assertRaises(PluginAlreadyExistsError):
            self.manager.register_connection_type(
                "mock",
                MockPool,
                MockConnection,
            )

    def test_unregister_connection_type(self):
        """Test unregistering a connection type."""
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

        result = self.manager.unregister_connection_type("mock")

        self.assertTrue(result)
        self.assertNotIn("mock", self.manager.get_connection_types())

    def test_unregister_nonexistent_type_returns_false(self):
        """Test that unregistering nonexistent type returns False."""
        result = self.manager.unregister_connection_type("nonexistent")

        self.assertFalse(result)

    def test_unregister_type_closes_pools(self):
        """Test that unregistering type closes all pools of that type."""
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

        config = ConnectionConfig(type="mock")
        self.manager.create_pool("test_pool", config)

        self.manager.unregister_connection_type("mock")

        self.assertIsNone(self.manager.get_pool("test_pool"))


class TestConnectionPoolManagerPoolLifecycle(unittest.TestCase):
    """Tests for pool lifecycle methods."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_create_pool(self):
        """Test creating a connection pool."""
        config = ConnectionConfig(
            type="mock",
            pool_settings={"min_size": 2, "max_size": 5},
        )

        pool = self.manager.create_pool("test_pool", config)

        self.assertIsNotNone(pool)
        self.assertEqual(pool.name, "test_pool")
        self.assertIn("test_pool", self.manager.get_pool_names())

    def test_create_duplicate_pool_raises_error(self):
        """Test that creating duplicate pool raises error."""
        config = ConnectionConfig(type="mock")

        self.manager.create_pool("test_pool", config)

        with self.assertRaises(PoolAlreadyExistsError):
            self.manager.create_pool("test_pool", config)

    def test_remove_pool(self):
        """Test removing a connection pool."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("test_pool", config)

        result = self.manager.remove_pool("test_pool")

        self.assertTrue(result)
        self.assertNotIn("test_pool", self.manager.get_pool_names())

    def test_remove_nonexistent_pool_returns_false(self):
        """Test that removing nonexistent pool returns False."""
        result = self.manager.remove_pool("nonexistent")

        self.assertFalse(result)

    def test_get_pool(self):
        """Test getting a pool by name."""
        config = ConnectionConfig(type="mock")
        created_pool = self.manager.create_pool("test_pool", config)

        retrieved_pool = self.manager.get_pool("test_pool")

        self.assertIs(created_pool, retrieved_pool)

    def test_get_pool_nonexistent_returns_none(self):
        """Test that getting nonexistent pool returns None."""
        pool = self.manager.get_pool("nonexistent")

        self.assertIsNone(pool)

    def test_get_pool_safe(self):
        """Test getting a pool with exception on not found."""
        config = ConnectionConfig(type="mock")
        created_pool = self.manager.create_pool("test_pool", config)

        retrieved_pool = self.manager.get_pool_safe("test_pool")

        self.assertIs(created_pool, retrieved_pool)

    def test_get_pool_safe_raises_error_on_nonexistent(self):
        """Test that get_pool_safe raises error on nonexistent pool."""
        with self.assertRaises(PoolNotFoundError):
            self.manager.get_pool_safe("nonexistent")


class TestConnectionPoolManagerConfiguration(unittest.TestCase):
    """Tests for configuration methods."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_initialize_from_config(self):
        """Test initializing pools from configuration dictionary."""
        config_dict = {
            "pools": {
                "pool1": {
                    "type": "mock",
                    "enabled": True,
                    "pool": {"min_size": 2, "max_size": 5},
                },
                "pool2": {
                    "type": "mock",
                    "enabled": True,
                    "pool": {"min_size": 1, "max_size": 3},
                },
            }
        }

        created = self.manager.initialize_from_config(config_dict)

        self.assertEqual(len(created), 2)
        self.assertIn("pool1", created)
        self.assertIn("pool2", created)

    def test_initialize_from_config_skips_disabled(self):
        """Test that disabled pools are skipped."""
        config_dict = {
            "pools": {
                "enabled_pool": {
                    "type": "mock",
                    "enabled": True,
                },
                "disabled_pool": {
                    "type": "mock",
                    "enabled": False,
                },
            }
        }

        created = self.manager.initialize_from_config(config_dict)

        self.assertEqual(len(created), 1)
        self.assertIn("enabled_pool", created)
        self.assertNotIn("disabled_pool", created)

    def test_create_pools_from_config(self):
        """Test creating pools from configuration."""
        pools_config = {
            "pool1": {
                "type": "mock",
                "enabled": True,
                "pool": {"min_size": 2},
            },
        }

        created = self.manager.create_pools_from_config(pools_config)

        self.assertEqual(len(created), 1)
        self.assertIn("pool1", created)

    def test_reload_configuration(self):
        """Test hot reloading configuration."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("old_pool", config)

        new_config = {
            "enabled": True,
            "pools": {
                "new_pool": {
                    "type": "mock",
                    "enabled": True,
                },
            },
        }

        result = self.manager.reload_configuration(new_config)

        self.assertTrue(result)
        self.assertIsNone(self.manager.get_pool("old_pool"))
        self.assertIsNotNone(self.manager.get_pool("new_pool"))


class TestConnectionPoolManagerMetrics(unittest.TestCase):
    """Tests for metrics methods."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_get_all_metrics(self):
        """Test getting metrics for all pools."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("pool1", config)
        self.manager.create_pool("pool2", config)

        metrics = self.manager.get_all_metrics()

        self.assertIn("pool1", metrics)
        self.assertIn("pool2", metrics)
        self.assertIsInstance(metrics["pool1"], PoolMetrics)

    def test_get_all_pools(self):
        """Test getting all pools."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("pool1", config)

        pools = self.manager.get_all_pools()

        self.assertIn("pool1", pools)
        self.assertIsInstance(pools["pool1"], BaseConnectionPool)

    def test_get_pool_names(self):
        """Test getting all pool names."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("pool1", config)
        self.manager.create_pool("pool2", config)

        names = self.manager.get_pool_names()

        self.assertEqual(len(names), 2)
        self.assertIn("pool1", names)
        self.assertIn("pool2", names)


class TestConnectionPoolManagerShutdown(unittest.TestCase):
    """Tests for shutdown methods."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_shutdown_all(self):
        """Test shutting down all pools."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("pool1", config)
        self.manager.create_pool("pool2", config)

        self.manager.shutdown_all()

        self.assertEqual(len(self.manager.get_pool_names()), 0)

    def test_reset(self):
        """Test resetting the manager."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("pool1", config)

        self.manager.reset()

        self.assertIsNone(ConnectionPoolManager._instance)


class TestConnectionPoolManagerPlugins(unittest.TestCase):
    """Tests for plugin registration methods."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_register_plugins(self):
        """Test registering multiple plugins at once."""

        def register_mock(registry):
            registry.register("mock", MockPool, MockConnection)

        self.manager.register_plugins({"mock": register_mock})

        self.assertIn("mock", self.manager.get_connection_types())

    def test_register_plugins_handles_failures(self):
        """Test that plugin registration failures are handled."""

        def register_good(registry):
            registry.register("mock", MockPool, MockConnection)

        def register_bad(registry):
            raise ValueError("Intentional failure")

        self.manager.register_plugins({
            "mock": register_good,
            "bad": register_bad,
        })

        self.assertIn("mock", self.manager.get_connection_types())


class TestConnectionPoolManagerThreadSafety(unittest.TestCase):
    """Tests for thread safety."""

    def setUp(self):
        """Set up test fixtures."""
        ConnectionPoolManager._instance = None
        self.manager = ConnectionPoolManager()
        self.manager.register_connection_type(
            "mock",
            MockPool,
            MockConnection,
        )

    def tearDown(self):
        """Clean up after tests."""
        self.manager.reset()
        ConnectionPoolManager._instance = None

    def test_concurrent_pool_creation(self):
        """Test concurrent pool creation."""
        errors = []
        created_pools = []

        def create_pool(index):
            try:
                config = ConnectionConfig(type="mock")
                pool_name = f"pool_{index}"
                self.manager.create_pool(pool_name, config)
                created_pools.append(pool_name)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=create_pool, args=(i,)) for i in range(5)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.assertEqual(len(errors), 0)
        self.assertEqual(len(created_pools), 5)

    def test_concurrent_pool_access(self):
        """Test concurrent pool access."""
        config = ConnectionConfig(type="mock")
        self.manager.create_pool("test_pool", config)

        errors = []

        def access_pool():
            try:
                pool = self.manager.get_pool("test_pool")
                self.assertIsNotNone(pool)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=access_pool) for _ in range(10)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        self.assertEqual(len(errors), 0)


if __name__ == "__main__":
    unittest.main()
