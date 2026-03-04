#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for Connection Pool.
"""

import pytest
import threading
import time
from typing import Any, Dict
from unittest.mock import Mock, patch, MagicMock

from silvaengine_connections import (
    BaseConnectionPool,
    PoolMetrics,
    PoolStatus,
    PoolError,
    PoolExhaustedError,
    PoolNotReadyError,
)


class MockConnection:
    """Mock connection for testing."""

    _id_counter = 0

    def __init__(self):
        MockConnection._id_counter += 1
        self.connection_id = MockConnection._id_counter
        self.is_used = False
        self.created_at = time.time()
        self._closed = False

    def is_healthy(self) -> bool:
        return not self._closed

    def get_lifetime(self) -> float:
        return time.time() - self.created_at

    def get_idle_time(self) -> float:
        return 0.0 if self.is_used else time.time() - self.created_at

    def reset(self) -> None:
        self.is_used = False

    def close(self) -> None:
        self._closed = True

    def execute(self, *args, **kwargs):
        return f"executed: {args}"


class MockPool(BaseConnectionPool[MockConnection]):
    """Mock pool implementation for testing BaseConnectionPool."""

    def __init__(self, name: str = "test_pool", min_size: int = 2, max_size: int = 10):
        self._test_config = {"name": name}
        super().__init__(
            name=name,
            connection_class=MockConnection,
            min_size=min_size,
            max_size=max_size,
            max_idle_time=300.0,
            max_lifetime=3600.0,
            wait_timeout=10.0,
            health_check_interval=30.0,
            enable_dynamic_resize=True,
        )

    def get_pool_type(self) -> str:
        return "mock"

    def _create_connection(self) -> MockConnection:
        return MockConnection()


class TestBaseConnectionPool:
    """Tests for BaseConnectionPool."""

    def test_pool_creation(self):
        """Test pool creation."""
        pool = MockPool("test_pool")

        assert pool.name == "test_pool"
        assert pool.status == PoolStatus.READY

    @staticmethod
    def _create_test_pool(
        min_size: int = 2,
        max_size: int = 10,
    ) -> MockPool:
        """Create a test pool."""
        return MockPool("test_pool", min_size=min_size, max_size=max_size)

    def test_pool_metrics(self):
        """Test pool metrics."""
        pool = self._create_test_pool()
        metrics = pool.metrics

        assert isinstance(metrics, PoolMetrics)
        assert hasattr(metrics, "total_created")
        assert hasattr(metrics, "total_borrowed")
        assert hasattr(metrics, "active_connections")

    def test_pool_status(self):
        """Test pool status."""
        pool = self._create_test_pool()

        assert pool.status == PoolStatus.READY

    def test_pool_resize(self):
        """Test pool resize."""
        pool = self._create_test_pool(min_size=2, max_size=10)

        with patch.object(pool, "_create_connection") as mock_create:
            mock_create.return_value = MockConnection()
            pool.resize(5, 20)

        assert pool._min_size == 5
        assert pool._max_size == 20


class TestPoolMetrics:
    """Tests for PoolMetrics."""

    def test_metrics_creation(self):
        """Test metrics creation."""
        metrics = PoolMetrics()

        assert metrics.total_created == 0
        assert metrics.total_destroyed == 0
        assert metrics.total_borrowed == 0
        assert metrics.total_returned == 0
        assert metrics.active_connections == 0
        assert metrics.idle_connections == 0
        assert metrics.wait_time_avg == 0.0
        assert metrics.wait_time_max == 0.0

    def test_metrics_update(self):
        """Test metrics update."""
        metrics = PoolMetrics()

        metrics.total_created = 5
        metrics.total_borrowed = 3
        metrics.active_connections = 2
        metrics.idle_connections = 3

        assert metrics.total_created == 5
        assert metrics.total_borrowed == 3
        assert metrics.active_connections == 2


class TestPoolStatus:
    """Tests for PoolStatus enum."""

    def test_status_values(self):
        """Test pool status values."""
        assert PoolStatus.INITIALIZING.value == "initializing"
        assert PoolStatus.READY.value == "ready"
        assert PoolStatus.PAUSED.value == "paused"
        assert PoolStatus.SHUTDOWN.value == "shutdown"

    def test_all_statuses(self):
        """Test all status values."""
        statuses = list(PoolStatus)
        assert len(statuses) == 4
        assert PoolStatus.INITIALIZING in statuses
        assert PoolStatus.READY in statuses
        assert PoolStatus.PAUSED in statuses
        assert PoolStatus.SHUTDOWN in statuses


class TestPoolExceptions:
    """Tests for pool exceptions."""

    def test_pool_not_ready_error(self):
        """Test PoolNotReadyError."""
        error = PoolNotReadyError(
            "Pool not ready",
            pool_name="test_pool",
            status="initializing",
        )

        assert error.details.get("pool_name") == "test_pool"
        assert error.details.get("status") == "initializing"

    def test_pool_exhausted_error(self):
        """Test PoolExhaustedError."""
        error = PoolExhaustedError(
            "Pool exhausted",
            pool_name="test_pool",
        )

        assert error.details.get("pool_name") == "test_pool"

    def test_pool_error(self):
        """Test PoolError."""
        error = PoolError("Pool error", details={"pool_name": "test_pool"})

        assert error.details.get("pool_name") == "test_pool"


class TestConnectionPoolContextManager:
    """Tests for connection pool context manager."""

    def test_context_manager(self):
        """Test context manager."""
        pool = TestBaseConnectionPool._create_test_pool()

        acquired_connections = []

        with patch.object(pool, "acquire") as mock_acquire:
            with patch.object(pool, "release") as mock_release:
                mock_acquire.return_value = MockConnection()

                with pool.connection() as conn:
                    acquired_connections.append(conn)

                mock_acquire.assert_called_once()
                mock_release.assert_called_once()

        assert len(acquired_connections) == 1


class TestPoolThreadSafety:
    """Tests for pool thread safety."""

    def test_concurrent_acquire_release(self):
        """Test concurrent acquire and release."""
        pool = TestBaseConnectionPool._create_test_pool(min_size=5, max_size=10)

        results = []
        errors = []

        def worker():
            try:
                with pool.connection() as conn:
                    time.sleep(0.01)
                    results.append(conn.connection_id)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        assert len(errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
