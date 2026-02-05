#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Exception definitions for silvaengine_connections.

Provides a comprehensive exception hierarchy for connection and pool management.
"""

from typing import Optional


class ConnectionError(Exception):
    """Base exception for connection-related errors."""
    
    ERROR_CODE = "CONN_ERROR"
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[dict] = None):
        """
        Initialize connection error.
        
        Args:
            message: Error message
            error_code: Error code for programmatic handling
            details: Additional error details
        """
        super().__init__(message)
        self.error_code = error_code or self.ERROR_CODE
        self.details = details or {}


class ConnectionTimeoutError(ConnectionError):
    """Raised when connection times out."""
    
    ERROR_CODE = "CONN_TIMEOUT"
    
    def __init__(self, message: str = "Connection timeout", timeout: Optional[float] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"timeout": timeout, **kwargs})


class ConnectionFailedError(ConnectionError):
    """Raised when connection attempt fails."""
    
    ERROR_CODE = "CONN_FAILED"
    
    def __init__(self, message: str = "Connection failed", host: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"host": host, **kwargs})


class AuthenticationError(ConnectionError):
    """Raised when authentication fails."""
    
    ERROR_CODE = "AUTH_FAILED"
    
    def __init__(self, message: str = "Authentication failed", **kwargs):
        super().__init__(message, self.ERROR_CODE, kwargs)


class ConnectionNotFoundError(ConnectionError):
    """Raised when connection is not found."""
    
    ERROR_CODE = "CONN_NOT_FOUND"
    
    def __init__(self, message: str = "Connection not found", conn_id: Optional[int] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"conn_id": conn_id, **kwargs})


class ConnectionClosedError(ConnectionError):
    """Raised when attempting to use a closed connection."""
    
    ERROR_CODE = "CONN_CLOSED"
    
    def __init__(self, message: str = "Connection is closed", conn_id: Optional[int] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"conn_id": conn_id, **kwargs})


class PoolError(Exception):
    """Base exception for pool-related errors."""
    
    ERROR_CODE = "POOL_ERROR"
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[dict] = None):
        super().__init__(message)
        self.error_code = error_code or self.ERROR_CODE
        self.details = details or {}


class PoolExhaustedError(PoolError):
    """Raised when connection pool is exhausted."""
    
    ERROR_CODE = "POOL_EXHAUSTED"
    
    def __init__(self, message: str = "Connection pool exhausted", pool_name: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"pool_name": pool_name, **kwargs})


class PoolNotReadyError(PoolError):
    """Raised when pool is not ready for operations."""
    
    ERROR_CODE = "POOL_NOT_READY"
    
    def __init__(self, message: str = "Pool is not ready", pool_name: Optional[str] = None, status: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"pool_name": pool_name, "status": status, **kwargs})


class PoolNotFoundError(PoolError):
    """Raised when pool is not found."""
    
    ERROR_CODE = "POOL_NOT_FOUND"
    
    def __init__(self, message: str = "Pool not found", pool_name: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"pool_name": pool_name, **kwargs})


class PoolAlreadyExistsError(PoolError):
    """Raised when attempting to create a pool that already exists."""
    
    ERROR_CODE = "POOL_EXISTS"
    
    def __init__(self, message: str = "Pool already exists", pool_name: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"pool_name": pool_name, **kwargs})


class PoolManagerError(Exception):
    """Base exception for pool manager errors."""
    
    ERROR_CODE = "MGR_ERROR"
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[dict] = None):
        super().__init__(message)
        self.error_code = error_code or self.ERROR_CODE
        self.details = details or {}


class PluginNotFoundError(PoolManagerError):
    """Raised when plugin type is not found."""
    
    ERROR_CODE = "PLUGIN_NOT_FOUND"
    
    def __init__(self, message: str = "Plugin not found", type_name: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"type_name": type_name, **kwargs})


class PluginAlreadyExistsError(PoolManagerError):
    """Raised when plugin type already exists."""
    
    ERROR_CODE = "PLUGIN_EXISTS"
    
    def __init__(self, message: str = "Plugin already exists", type_name: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"type_name": type_name, **kwargs})


class ConfigurationError(Exception):
    """Base exception for configuration errors."""
    
    ERROR_CODE = "CONFIG_ERROR"
    
    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[dict] = None):
        super().__init__(message)
        self.error_code = error_code or self.ERROR_CODE
        self.details = details or {}


class ConfigValidationError(ConfigurationError):
    """Raised when configuration validation fails."""
    
    ERROR_CODE = "CONFIG_INVALID"
    
    def __init__(self, message: str = "Configuration validation failed", field: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"field": field, **kwargs})


class ConfigNotFoundError(ConfigurationError):
    """Raised when configuration is not found."""
    
    ERROR_CODE = "CONFIG_NOT_FOUND"
    
    def __init__(self, message: str = "Configuration not found", config_name: Optional[str] = None, **kwargs):
        super().__init__(message, self.ERROR_CODE, {"config_name": config_name, **kwargs})


class HealthCheckError(Exception):
    """Raised when health check fails."""
    
    ERROR_CODE = "HEALTH_ERROR"
    
    def __init__(self, message: str = "Health check failed", pool_name: Optional[str] = None, **kwargs):
        super().__init__(message)
        self.error_code = self.ERROR_CODE
        self.details = {"pool_name": pool_name, **kwargs}
