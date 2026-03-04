#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Unit tests for Circuit Breaker.
"""

import pytest
import threading
import time
from typing import Any, Callable
from unittest.mock import Mock, patch

from silvaengine_connections import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
)


class TestCircuitBreakerConfig:
    """Tests for CircuitBreakerConfig."""

    def test_default_config(self):
        """Test default configuration."""
        config = CircuitBreakerConfig()

        assert config.failure_threshold == 5
        assert config.success_threshold == 3
        assert config.timeout == 60.0
        assert config.half_open_max_calls == 3

    def test_custom_config(self):
        """Test custom configuration."""
        config = CircuitBreakerConfig(
            failure_threshold=10,
            success_threshold=3,
            timeout=120.0,
            half_open_max_calls=5,
        )

        assert config.failure_threshold == 10
        assert config.success_threshold == 3
        assert config.timeout == 120.0
        assert config.half_open_max_calls == 5


class TestCircuitState:
    """Tests for CircuitState enum."""

    def test_state_values(self):
        """Test circuit state values."""
        assert CircuitState.CLOSED.value == "closed"
        assert CircuitState.OPEN.value == "open"
        assert CircuitState.HALF_OPEN.value == "half_open"

    def test_all_states(self):
        """Test all state values."""
        states = list(CircuitState)
        assert len(states) == 3
        assert CircuitState.CLOSED in states
        assert CircuitState.OPEN in states
        assert CircuitState.HALF_OPEN in states


class TestCircuitBreaker:
    """Tests for CircuitBreaker."""

    def setup_method(self):
        """Setup before each test."""
        self.config = CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout=1.0,
            half_open_max_calls=2,
        )

    def test_initial_state(self):
        """Test initial circuit breaker state."""
        cb = CircuitBreaker("test", self.config)

        assert cb.get_state() == CircuitState.CLOSED

    def test_successful_call(self):
        """Test successful call."""
        cb = CircuitBreaker("test", self.config)

        def success_func():
            return "success"

        result = cb.call(success_func)

        assert result == "success"
        assert cb.get_state() == CircuitState.CLOSED
        stats = cb.get_stats()
        assert stats["total_calls"] == 1
        assert stats["successful_calls"] == 1
        assert stats["failed_calls"] == 0

    def test_failed_call(self):
        """Test failed call."""
        cb = CircuitBreaker("test", self.config)

        def fail_func():
            raise Exception("failure")

        for _ in range(2):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        assert cb.get_state() == CircuitState.CLOSED

        try:
            cb.call(fail_func)
        except Exception:
            pass

        assert cb.get_state() == CircuitState.OPEN

    def test_circuit_opens_after_threshold(self):
        """Test circuit opens after failure threshold."""
        cb = CircuitBreaker("test", self.config)

        def fail_func():
            raise Exception("failure")

        for _ in range(self.config.failure_threshold):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        assert cb.get_state() == CircuitState.OPEN

    def test_call_when_open(self):
        """Test calling when circuit is open."""
        cb = CircuitBreaker("test", self.config)

        def fail_func():
            raise Exception("failure")

        for _ in range(self.config.failure_threshold + 1):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        assert cb.get_state() == CircuitState.OPEN

        with pytest.raises(Exception) as exc_info:
            cb.call(fail_func)

        assert "is OPEN" in str(exc_info.value)

    def test_transition_to_half_open(self):
        """Test transition to half-open state."""
        cb = CircuitBreaker("test", self.config)

        def fail_func():
            raise Exception("failure")

        for _ in range(self.config.failure_threshold):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        assert cb.get_state() == CircuitState.OPEN

        time.sleep(self.config.timeout + 0.5)

        def success_func():
            return "success"

        try:
            cb.call(success_func)
        except Exception:
            pass

        assert cb.get_state() == CircuitState.HALF_OPEN

    def test_transition_to_closed_from_half_open(self):
        """Test transition from half-open to closed."""
        cb = CircuitBreaker("test", self.config)

        def fail_func():
            raise Exception("failure")

        for _ in range(self.config.failure_threshold):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        time.sleep(self.config.timeout + 0.5)

        def success_func():
            return "success"

        for _ in range(self.config.success_threshold):
            cb.call(success_func)

        assert cb.get_state() == CircuitState.CLOSED

    def test_reset(self):
        """Test circuit breaker reset."""
        cb = CircuitBreaker("test", self.config)

        def fail_func():
            raise Exception("failure")

        for _ in range(self.config.failure_threshold):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        assert cb.get_state() == CircuitState.OPEN

        cb.reset()

        assert cb.get_state() == CircuitState.CLOSED
        stats = cb.get_stats()
        assert stats["total_calls"] == 0

    def test_get_stats(self):
        """Test getting stats."""
        cb = CircuitBreaker("test", self.config)

        def success_func():
            return "success"

        def fail_func():
            raise Exception("failure")

        cb.call(success_func)

        try:
            cb.call(fail_func)
        except Exception:
            pass

        stats = cb.get_stats()

        assert "total_calls" in stats
        assert "successful_calls" in stats
        assert "failed_calls" in stats
        assert "state" in stats

    def test_concurrent_calls(self):
        """Test concurrent calls to circuit breaker."""
        cb = CircuitBreaker("test", self.config)

        def work_func():
            time.sleep(0.01)
            return "done"

        threads = [threading.Thread(target=lambda: cb.call(work_func)) for _ in range(10)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        stats = cb.get_stats()
        assert stats["total_calls"] == 10


class TestCircuitBreakerEdgeCases:
    """Tests for circuit breaker edge cases."""

    def test_zero_timeout(self):
        """Test circuit breaker with zero timeout transitions to half-open quickly."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            timeout=0.0,
        )
        cb = CircuitBreaker("test", config)

        def fail_func():
            raise Exception("failure")

        for _ in range(2):
            try:
                cb.call(fail_func)
            except Exception:
                pass

        # With zero timeout, circuit may be OPEN or HALF_OPEN depending on timing
        assert cb.get_state() in (CircuitState.OPEN, CircuitState.HALF_OPEN)

    def test_single_failure_threshold(self):
        """Test circuit breaker with single failure threshold."""
        config = CircuitBreakerConfig(
            failure_threshold=1,
            timeout=1.0,
        )
        cb = CircuitBreaker("test", config)

        def fail_func():
            raise Exception("failure")

        try:
            cb.call(fail_func)
        except Exception:
            pass

        assert cb.get_state() == CircuitState.OPEN

    def test_call_with_args(self):
        """Test calling function with arguments."""
        config = CircuitBreakerConfig()
        cb = CircuitBreaker("test", config)

        def func_with_args(a, b, kwarg=None):
            return f"{a}-{b}-{kwarg}"

        result = cb.call(func_with_args, "x", "y", kwarg="z")

        assert result == "x-y-z"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
