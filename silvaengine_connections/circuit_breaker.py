#!/usr/bin python
# -*- coding: utf-8 -*-
"""
Circuit Breaker pattern implementation for connection pool.

Provides fault tolerance and resilience by wrapping operations with
circuit breaker logic to prevent cascading failures.
"""

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, Optional, TypeVar

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker state enumeration."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""

    failure_threshold: int = 5
    success_threshold: int = 3
    timeout: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreaker:
    """
    Circuit breaker implementation for fault tolerance.

    States:
        - CLOSED: Normal operation, requests pass through
        - OPEN: Circuit is open, requests are rejected
        - HALF_OPEN: Testing if service recovered

    Example:
        ```python
        config = CircuitBreakerConfig(
            failure_threshold=5,
            success_threshold=3,
            timeout=60.0
        )
        breaker = CircuitBreaker("pool_breaker", config)

        result = breaker.call(some_function, arg1, arg2)
        ```
    """

    def __init__(self, name: str, config: CircuitBreakerConfig) -> None:
        """
        Initialize circuit breaker.

        Args:
            name: Circuit breaker name
            config: Circuit breaker configuration
        """
        self._name = name
        self._config = config
        self._state = CircuitState.CLOSED
        self._stats = CircuitBreakerStats()
        self._lock = threading.RLock()
        self._last_state_change_time: float = time.time()
        self._half_open_calls: int = 0
        self._logger = logging.getLogger(f"{__name__}.{name}")

    @property
    def name(self) -> str:
        """Get circuit breaker name."""
        return self._name

    def call(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            *args: Positional arguments for function
            **kwargs: Keyword arguments for function

        Returns:
            T: Function result

        Raises:
            Exception: Re-raises exception if function fails
            RuntimeError: If circuit is open and rejecting calls
        """
        if not self._should_attempt():
            self._stats.rejected_calls += 1
            error_msg = (
                f"Circuit breaker '{self._name}' is OPEN, "
                f"call rejected. State: {self._state.value}"
            )
            self._logger.warning(error_msg)
            raise RuntimeError(error_msg)

        with self._lock:
            self._stats.total_calls += 1

            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1
                if self._half_open_calls > self._config.half_open_max_calls:
                    self._stats.rejected_calls += 1
                    error_msg = (
                        f"Circuit breaker '{self._name}' exceeded "
                        f"half-open max calls ({self._config.half_open_max_calls})"
                    )
                    self._logger.warning(error_msg)
                    raise RuntimeError(error_msg)

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _should_attempt(self) -> bool:
        """
        Check if attempt should be made to call the protected function.

        Returns:
            bool: True if call should be attempted
        """
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                elapsed = time.time() - self._last_state_change_time
                if elapsed >= self._config.timeout:
                    self._transition_to_half_open()
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                return True

            return False

    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        if self._state != CircuitState.OPEN:
            self._logger.warning(
                f"Circuit breaker '{self._name}' transitioning "
                f"from {self._state.value} to OPEN"
            )
            self._state = CircuitState.OPEN
            self._last_state_change_time = time.time()
            self._half_open_calls = 0

    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        if self._state != CircuitState.HALF_OPEN:
            self._logger.info(
                f"Circuit breaker '{self._name}' transitioning "
                f"from {self._state.value} to HALF_OPEN"
            )
            self._state = CircuitState.HALF_OPEN
            self._last_state_change_time = time.time()
            self._half_open_calls = 0
            self._stats.consecutive_failures = 0

    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state."""
        if self._state != CircuitState.CLOSED:
            self._logger.info(
                f"Circuit breaker '{self._name}' transitioning "
                f"from {self._state.value} to CLOSED"
            )
            self._state = CircuitState.CLOSED
            self._last_state_change_time = time.time()
            self._stats.consecutive_failures = 0

    def _on_success(self) -> None:
        """Handle successful call."""
        with self._lock:
            self._stats.successful_calls += 1
            self._stats.consecutive_successes += 1
            self._stats.consecutive_failures = 0
            self._stats.last_success_time = time.time()

            if self._state == CircuitState.HALF_OPEN:
                if self._stats.consecutive_successes >= self._config.success_threshold:
                    self._transition_to_closed()

    def _on_failure(self) -> None:
        """Handle failed call."""
        with self._lock:
            self._stats.failed_calls += 1
            self._stats.consecutive_failures += 1
            self._stats.consecutive_successes = 0
            self._stats.last_failure_time = time.time()

            if self._state == CircuitState.CLOSED:
                if self._stats.consecutive_failures >= self._config.failure_threshold:
                    self._transition_to_open()

            elif self._state == CircuitState.HALF_OPEN:
                self._transition_to_open()

    def reset(self) -> None:
        """
        Reset circuit breaker to closed state.

        Clears all statistics and resets the circuit to CLOSED state.
        """
        with self._lock:
            self._logger.info(f"Circuit breaker '{self._name}' reset")
            self._state = CircuitState.CLOSED
            self._stats = CircuitBreakerStats()
            self._last_state_change_time = time.time()
            self._half_open_calls = 0

    def get_state(self) -> CircuitState:
        """
        Get current circuit state.

        Returns:
            CircuitState: Current state
        """
        with self._lock:
            if self._state == CircuitState.OPEN:
                elapsed = time.time() - self._last_state_change_time
                if elapsed >= self._config.timeout:
                    return CircuitState.HALF_OPEN
            return self._state

    def get_stats(self) -> Dict[str, Any]:
        """
        Get circuit breaker statistics.

        Returns:
            Dict containing statistics
        """
        with self._lock:
            return {
                "name": self._name,
                "state": self.get_state().value,
                "total_calls": self._stats.total_calls,
                "successful_calls": self._stats.successful_calls,
                "failed_calls": self._stats.failed_calls,
                "rejected_calls": self._stats.rejected_calls,
                "consecutive_failures": self._stats.consecutive_failures,
                "consecutive_successes": self._stats.consecutive_successes,
                "last_failure_time": self._stats.last_failure_time,
                "last_success_time": self._stats.last_success_time,
                "time_in_current_state": time.time() - self._last_state_change_time,
            }

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<CircuitBreaker("
            f"name={self._name}, "
            f"state={self.get_state().value}, "
            f"total_calls={self._stats.total_calls})>"
        )
