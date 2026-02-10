"""
HTTPX Connection Pool Implementation

Asynchronous/synchronous HTTP client connection pool implementation based on HTTPX, supporting hot-plugging.
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Coroutine, Dict, Iterator, Optional, TypeVar, Union

import httpx
from httpx import AsyncClient, Client, Response, Timeout

from ..config import ConnectionConfig
from ..connection import BaseConnection
from ..connection_pool import BaseConnectionPool
from ..exceptions import (
    ConfigValidationError,
    ConnectionError,
    ConnectionTimeoutError,
    HealthCheckError,
    PoolError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=Union[Client, AsyncClient])


class HTTPXConnection(BaseConnection[Union[Client, AsyncClient]]):
    """
    HTTPX Connection Wrapper Class

    Uses HTTPX Client as the underlying connection object, providing HTTP request management and health check functionality.
    Supports both synchronous and asynchronous modes.

    Attributes:
        _client: HTTPX client instance
        _base_url: Base URL
        _async_mode: Whether async mode
        _client_config: Client configuration parameters
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize HTTPX Connection

        Args:
            config: Connection configuration dictionary, containing the following key fields:
                - base_url: Base URL (optional)
                - async_mode: Whether async mode (default: False)
                - timeout: Request timeout in seconds (default: 30)
                - connect_timeout: Connection timeout in seconds (default: 5)
                - read_timeout: Read timeout in seconds (default: 30)
                - pool_limits: Connection pool limit configuration
                    - max_connections: Maximum number of connections
                    - max_keepalive_connections: Maximum number of keepalive connections
                - headers: Default request headers
                - cookies: Default cookies
                - verify: Whether to verify SSL certificate (default: True)
                - cert: Client certificate path
                - http2: Whether to enable HTTP/2 (default: False)
                - proxy: Proxy configuration
                - follow_redirects: Whether to follow redirects (default: True)
        """
        super().__init__(config)
        self._client: Optional[Union[Client, AsyncClient]] = None
        self._base_url = config.get("base_url", "")
        self._async_mode = config.get("async_mode", False)
        self._client_config = self._build_client_config()

    def _build_client_config(self) -> Dict[str, Any]:
        """
        Build HTTPX Client Configuration

        Returns:
            HTTPX client configuration dictionary
        """
        config = {}

        # Base URL
        if self._base_url:
            config["base_url"] = self._base_url

        # Timeout configuration
        timeout_config = self._config.get("timeout", 30.0)

        if isinstance(timeout_config, dict):
            config["timeout"] = Timeout(
                connect=timeout_config.get("connect", 5.0),
                read=timeout_config.get("read", 30.0),
                write=timeout_config.get("write", 30.0),
                pool=timeout_config.get("pool", 5.0),
            )
        else:
            config["timeout"] = Timeout(timeout_config)

        # Connection pool limits
        pool_limits = self._config.get("pool_limits", {})

        if pool_limits:
            limits = httpx.Limits(
                max_connections=pool_limits.get("max_connections", 100),
                max_keepalive_connections=pool_limits.get(
                    "max_keepalive_connections", 20
                ),
            )
            config["limits"] = limits

        # Default request headers
        if "headers" in self._config:
            config["headers"] = self._config["headers"]

        # Cookies
        if "cookies" in self._config:
            config["cookies"] = self._config["cookies"]

        # SSL verification
        if "verify" in self._config:
            config["verify"] = self._config["verify"]

        # Client certificate
        if "cert" in self._config:
            config["cert"] = self._config["cert"]

        # HTTP/2 support
        if "http2" in self._config:
            config["http2"] = self._config["http2"]

        # Proxy configuration
        if "proxy" in self._config:
            proxy_config = self._config["proxy"]

            if isinstance(proxy_config, str):
                config["proxy"] = proxy_config
            elif isinstance(proxy_config, dict):
                config["proxies"] = proxy_config

        # Follow redirects
        if "follow_redirects" in self._config:
            config["follow_redirects"] = self._config["follow_redirects"]

        return config

    def connect(self) -> Union[Client, AsyncClient]:
        """
        Establish HTTP Client Connection

        Returns:
            HTTPX Client or AsyncClient instance

        Raises:
            ConnectionError: When connection fails
        """
        try:
            if self._async_mode:
                self._client = AsyncClient(**self._client_config)
            else:
                self._client = Client(**self._client_config)

            self._is_closed = False
            mode_str = "async" if self._async_mode else "sync"
            logger.debug(
                f"HTTPX {mode_str} client created [connection_id={self._connection_id}]"
            )
            return self._client

        except Exception as e:
            raise ConnectionError(
                f"HTTPX client creation failed: {str(e)}",
                details={"connection_type": "httpx", "original_error": str(e)},
            )

    def close(self) -> None:
        """
        Close HTTP Client Connection
        """
        if self._client and not self._is_closed:
            try:
                self._client.close()
                self._is_closed = True
                logger.debug(f"HTTPX client closed [connection_id={self._connection_id}]")
            except Exception as e:
                logger.warning(
                    f"Error closing HTTPX client [connection_id={self._connection_id}]: {e}"
                )

    def is_healthy(self) -> bool:
        """
        Check Connection Health Status

        Check connection status by sending HEAD request to base URL

        Returns:
            True if connection is healthy, False if connection is abnormal
        """
        if not self._client or self._is_closed:
            return False

        # If no base URL, assume connection is healthy
        if not self._base_url:
            return True

        try:
            if self._async_mode:
                # Cannot directly check in async mode, assume healthy
                return True
            else:
                response = self._client.head(self._base_url, timeout=5.0)
                return response.status_code < 500
        except Exception:
            return False

    def get(self, url: str, **kwargs) -> Response:
        """
        Send GET Request

        Args:
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object

        Raises:
            ConnectionError: When request fails
        """
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> Response:
        """
        Send POST Request

        Args:
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> Response:
        """
        Send PUT Request

        Args:
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> Response:
        """
        Send DELETE Request

        Args:
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        return self.request("DELETE", url, **kwargs)

    def patch(self, url: str, **kwargs) -> Response:
        """
        Send PATCH Request

        Args:
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        return self.request("PATCH", url, **kwargs)

    def request(
        self, method: str, url: str, **kwargs
    ) -> Coroutine[Any, Any, Response]:
        """
        Send HTTP Request

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object

        Raises:
            ConnectionError: When request fails
        """
        if not self._client or self._is_closed:
            raise ConnectionError(
                "HTTPX client not created or already closed", details={"connection_type": "httpx"}
            )

        if self._async_mode:
            raise ConnectionError(
                "Please use async request methods in async mode",
                details={"connection_type": "httpx"},
            )

        try:
            return self._client.request(method, url, **kwargs)
        except httpx.TimeoutException as e:
            raise ConnectionTimeoutError(
                f"HTTP request timeout: {str(e)}",
                connection_type="httpx",
                timeout=kwargs.get("timeout"),
                original_error=e,
            )
        except Exception as e:
            raise ConnectionError(
                f"HTTP request failed: {str(e)}",
                details={"connection_type": "httpx", "original_error": str(e)},
            )

    async def arequest(self, method: str, url: str, **kwargs) -> Response:
        """
        Send Asynchronous HTTP Request

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        if not self._client or self._is_closed:
            raise ConnectionError(
                "HTTPX client not created or already closed", details={"connection_type": "httpx"}
            )

        if not self._async_mode:
            raise ConnectionError(
                "Please use sync request methods in sync mode", details={"connection_type": "httpx"}
            )

        try:
            return await self._client.request(method, url, **kwargs)
        except httpx.TimeoutException as e:
            raise ConnectionTimeoutError(
                f"HTTP request timeout: {str(e)}",
                connection_type="httpx",
                timeout=kwargs.get("timeout"),
                original_error=e,
            )
        except Exception as e:
            raise ConnectionError(
                f"HTTP request failed: {str(e)}",
                details={"connection_type": "httpx", "original_error": str(e)},
            )

    async def aget(self, url: str, **kwargs) -> Response:
        """Send async GET request"""
        return await self.arequest("GET", url, **kwargs)

    async def apost(self, url: str, **kwargs) -> Response:
        """Send async POST request"""
        return await self.arequest("POST", url, **kwargs)

    async def aput(self, url: str, **kwargs) -> Response:
        """Send async PUT request"""
        return await self.arequest("PUT", url, **kwargs)

    async def adelete(self, url: str, **kwargs) -> Response:
        """Send async DELETE request"""
        return await self.arequest("DELETE", url, **kwargs)

    async def apatch(self, url: str, **kwargs) -> Response:
        """Send async PATCH request"""
        return await self.arequest("PATCH", url, **kwargs)

    @property
    def client(self) -> Optional[Union[Client, AsyncClient]]:
        """Get HTTPX client instance"""
        return self._client

    @property
    def is_async(self) -> bool:
        """Whether is async mode"""
        return self._async_mode


class HTTPXConnectionPool(BaseConnectionPool[HTTPXConnection]):
    """
    HTTPX Connection Pool Implementation

    HTTPX-specific connection pool implementation based on the base connection pool, providing HTTP client management and monitoring functionality.

    Attributes:
        _name: Connection pool name
        _config: Connection configuration
        _health_check_interval: Health check interval (seconds)
        _last_health_check: Last health check time
        _health_check_url: Health check URL
    """

    def __init__(
        self,
        name: str,
        config: ConnectionConfig,
        min_size: int = 2,
        max_size: int = 10,
        max_idle_time: float = 300.0,
        max_lifetime: float = 3600.0,
        health_check_interval: float = 60.0,
        health_check_url: Optional[str] = None,
    ) -> None:
        """
        Initialize HTTPX Connection Pool

        Args:
            name: Connection pool unique identifier name
            config: Connection configuration object
            min_size: Minimum number of connections
            max_size: Maximum number of connections
            max_idle_time: Maximum idle time (seconds)
            max_lifetime: Maximum connection lifetime (seconds)
            health_check_interval: Health check interval (seconds)
            health_check_url: Health check URL
        """
        # Build connection configuration dictionary
        conn_config = {
            "base_url": config.settings.get("base_url", ""),
            "async_mode": config.settings.get("async_mode", False),
            "timeout": config.settings.get("timeout", 30.0),
            "connect_timeout": config.settings.get("connect_timeout", 5.0),
            "read_timeout": config.settings.get("read_timeout", 30.0),
            "pool_limits": config.settings.get("pool_limits", {}),
            "headers": config.settings.get("headers", {}),
            "cookies": config.settings.get("cookies"),
            "verify": config.settings.get("verify", True),
            "cert": config.settings.get("cert"),
            "http2": config.settings.get("http2", False),
            "proxy": config.settings.get("proxy"),
            "follow_redirects": config.settings.get("follow_redirects", True),
        }

        super().__init__(
            name=name,
            connection_class=HTTPXConnection,
            min_size=min_size,
            max_size=max_size,
            max_idle_time=max_idle_time,
            max_lifetime=max_lifetime,
        )

        self._config = conn_config
        self._health_check_interval = health_check_interval
        self._last_health_check = 0.0
        self._last_health_status: Dict[str, Any] = {}
        self._health_check_url = health_check_url or config.settings.get(
            "health_check_url"
        )
        self._async_mode = config.settings.get("async_mode", False)

    def get_pool_type(self) -> str:
        """Get pool type identifier."""
        return "httpx"

    def _create_connection(self) -> HTTPXConnection:
        """
        Create New HTTPX Connection

        Returns:
            Newly created HTTPX connection instance

        Raises:
            PoolError: When connection creation fails
        """
        try:
            connection = HTTPXConnection(self._config)
            connection.connect()
            return connection
        except ConnectionError:
            raise
        except Exception as e:
            raise PoolError(
                f"Failed to create HTTPX connection: {str(e)}",
                details={"pool_name": self._name, "original_error": str(e)}
            )

    def _validate_connection(self, connection: HTTPXConnection) -> bool:
        """
        Validate Connection Validity

        Args:
            connection: Connection to be validated

        Returns:
            True if connection is valid, False if invalid
        """
        return connection.is_healthy()

    def _destroy_connection(self, connection: HTTPXConnection) -> None:
        """
        Destroy Connection

        Args:
            connection: Connection to be destroyed
        """
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Error destroying HTTPX connection: {e}")

    def health_check(self) -> Dict[str, Any]:
        """
        Perform Connection Pool Health Check

        Returns:
            Health check result dictionary
        """
        current_time = time.time()

        # Check if health check needs to be performed
        if current_time - self._last_health_check < self._health_check_interval:
            return self._last_health_status

        with self._lock:
            unhealthy_count = 0

            # Check active connections
            for connection_id, connection in list(self._active_connections.items()):
                if not connection.is_healthy():
                    unhealthy_count += 1

            # Check idle connections
            idle_conns = []

            while not self._idle_connections.empty():
                try:
                    connection = self._idle_connections.get_nowait()
                    
                    if connection.is_healthy():
                        idle_conns.append(connection)
                    else:
                        unhealthy_count += 1
                        self._destroy_connection(connection)
                except Exception:
                    break

            # Put healthy connections back into the queue
            for connection in idle_conns:
                try:
                    self._idle_connections.put_nowait(connection)
                except Exception:
                    self._destroy_connection(connection)

            status = {
                "status": "unhealthy" if unhealthy_count > 0 else "healthy",
                "active_connections": len(self._active_connections),
                "idle_connections": self._idle_connections.qsize(),
                "total_connections": len(self._active_connections)
                + self._idle_connections.qsize(),
                "unhealthy_connections": unhealthy_count,
                "pool_name": self._name,
                "connection_type": "httpx",
                "async_mode": self._async_mode,
            }

            self._last_health_check = current_time
            self._last_health_status = status

            if unhealthy_count > 0:
                logger.warning(
                    f"HTTPX connection pool health check found {unhealthy_count} unhealthy connections"
                )

            return status

    def get_stats(self) -> Dict[str, Any]:
        """
        Get Connection Pool Statistics

        Returns:
            Statistics dictionary
        """
        with self._lock:
            return {
                "pool_name": self._name,
                "connection_type": "httpx",
                "async_mode": self._async_mode,
                "min_size": self._min_size,
                "max_size": self._max_size,
                "active_connections": len(self._active_connections),
                "idle_connections": self._idle_connections.qsize(),
                "total_connections": len(self._active_connections)
                + self._idle_connections.qsize(),
                "wait_timeout": self._wait_timeout,
                "max_idle_time": self._max_idle_time,
                "max_lifetime": self._max_lifetime,
                "health_check_interval": self._health_check_interval,
            }

    def request(self, method: str, url: str, **kwargs) -> Response:
        """
        Convenient Request Method

        Automatically acquire connection, execute request, and release connection

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        with self.connection() as connection:
            return connection.request(method, url, **kwargs)

    def get(self, url: str, **kwargs) -> Response:
        """Send GET request"""
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs) -> Response:
        """Send POST request"""
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs) -> Response:
        """Send PUT request"""
        return self.request("PUT", url, **kwargs)

    def delete(self, url: str, **kwargs) -> Response:
        """Send DELETE request"""
        return self.request("DELETE", url, **kwargs)

    def patch(self, url: str, **kwargs) -> Response:
        """Send PATCH request"""
        return self.request("PATCH", url, **kwargs)

    async def arequest(self, method: str, url: str, **kwargs) -> Response:
        """
        Convenient Async Request Method

        Automatically acquire connection, execute async request, and release connection

        Args:
            method: HTTP method
            url: Request URL
            **kwargs: Request parameters

        Returns:
            HTTP response object
        """
        connection = self.acquire()
        
        try:
            return await connection.arequest(method, url, **kwargs)
        finally:
            self.release(connection)

    async def aget(self, url: str, **kwargs) -> Response:
        """Send async GET request"""
        return await self.arequest("GET", url, **kwargs)

    async def apost(self, url: str, **kwargs) -> Response:
        """Send async POST request"""
        return await self.arequest("POST", url, **kwargs)

    async def aput(self, url: str, **kwargs) -> Response:
        """Send async PUT request"""
        return await self.arequest("PUT", url, **kwargs)

    async def adelete(self, url: str, **kwargs) -> Response:
        """Send async DELETE request"""
        return await self.arequest("DELETE", url, **kwargs)

    async def apatch(self, url: str, **kwargs) -> Response:
        """Send async PATCH request"""
        return await self.arequest("PATCH", url, **kwargs)


# Register HTTPX Connection Type
from ..plugin_registry import PluginRegistry


def register_httpx_plugin(registry: PluginRegistry) -> None:
    """
    Register HTTPX Plugin to Registry

    Args:
        registry: Plugin registry instance
    """
    registry.register(
        type_name='httpx',
        pool_class=HTTPXConnectionPool,
        connection_class=HTTPXConnection
    )
    logger.info("HTTPX plugin registered")


# Alias for backward compatibility
HTTPXPool = HTTPXConnectionPool
