#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Connection validator for silvaengine_connections.

Provides comprehensive connection validation including:
- Pre-connection parameter validation
- Connection health checks
- Database type-specific validation
- Connection pool parameter validation
"""

import re
import socket
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from .config import ConnectionConfig


class ValidationLevel(Enum):
    """Validation severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ValidationMessage:
    """Validation message data class."""

    code: str
    message: str
    level: ValidationLevel
    field: Optional[str] = None


@dataclass
class ConnectionValidationResult:
    """Connection validation result."""

    is_valid: bool = True
    can_connect: bool = True
    messages: List[ValidationMessage] = field(default_factory=list)

    def add_message(
        self, code: str, message: str, level: ValidationLevel, field: Optional[str] = None
    ) -> None:
        """Add a validation message."""
        self.messages.append(ValidationMessage(code, message, level, field))

        if level in (ValidationLevel.ERROR, ValidationLevel.CRITICAL):
            self.is_valid = False
        if level == ValidationLevel.CRITICAL:
            self.can_connect = False

    def get_errors(self) -> List[ValidationMessage]:
        """Get all error messages."""
        return [m for m in self.messages if m.level in (ValidationLevel.ERROR, ValidationLevel.CRITICAL)]

    def get_warnings(self) -> List[ValidationMessage]:
        """Get all warning messages."""
        return [m for m in self.messages if m.level == ValidationLevel.WARNING]


class BaseConnectionValidator(ABC):
    """
    Abstract base class for connection validators.

    Provides the foundation for all connection type validators with
    common validation logic and hooks for type-specific validation.
    """

    # Common port ranges
    WELL_KNOWN_PORTS: Set[int] = set(range(1, 1024))
    REGISTERED_PORTS: Set[int] = set(range(1024, 49152))

    def __init__(self, config: ConnectionConfig):
        """
        Initialize the validator.

        Args:
            config: Connection configuration to validate
        """
        self.config = config
        self.result = ConnectionValidationResult()

    def validate(self) -> ConnectionValidationResult:
        """
        Perform full validation.

        Returns:
            ConnectionValidationResult with validation status and messages
        """
        # Reset result
        self.result = ConnectionValidationResult()

        # Run common validations
        self._validate_common()

        # Run type-specific validations
        self._validate_type_specific()

        return self.result

    def _validate_common(self) -> None:
        """Run common validations applicable to all connection types."""
        # Validate pool settings
        self._validate_pool_settings()

        # Validate timeout settings
        self._validate_timeout_settings()

    def _validate_pool_settings(self) -> None:
        """Validate connection pool settings."""
        pool_settings = self.config.pool_settings

        min_size = pool_settings.get("min_size")
        max_size = pool_settings.get("max_size")

        if min_size is not None and max_size is not None:
            # Check ratio
            if max_size > 0:
                ratio = min_size / max_size
                if ratio < 0.1:
                    self.result.add_message(
                        "POOL_RATIO_LOW",
                        f"min_size ({min_size}) is less than 10% of max_size ({max_size}). "
                        f"This may cause connection pool inefficiency.",
                        ValidationLevel.WARNING,
                        "pool.min_size",
                    )
                elif ratio > 0.8:
                    self.result.add_message(
                        "POOL_RATIO_HIGH",
                        f"min_size ({min_size}) is more than 80% of max_size ({max_size}). "
                        f"This may limit pool elasticity.",
                        ValidationLevel.WARNING,
                        "pool.min_size",
                    )

        # Validate max_lifetime
        max_lifetime = pool_settings.get("max_lifetime", 3600)
        if max_lifetime < 60:
            self.result.add_message(
                "POOL_LIFETIME_SHORT",
                f"max_lifetime ({max_lifetime}s) is very short. "
                f"Frequent connection recycling may impact performance.",
                ValidationLevel.WARNING,
                "pool.max_lifetime",
            )
        elif max_lifetime > 86400:
            self.result.add_message(
                "POOL_LIFETIME_LONG",
                f"max_lifetime ({max_lifetime}s) is very long. "
                f"Long-lived connections may become stale.",
                ValidationLevel.WARNING,
                "pool.max_lifetime",
            )

    def _validate_timeout_settings(self) -> None:
        """Validate timeout settings."""
        pool_settings = self.config.pool_settings

        wait_timeout = pool_settings.get("wait_timeout", 10.0)
        if wait_timeout < 1.0:
            self.result.add_message(
                "TIMEOUT_TOO_SHORT",
                f"wait_timeout ({wait_timeout}s) is very short. "
                f"Connections may timeout frequently under load.",
                ValidationLevel.WARNING,
                "pool.wait_timeout",
            )
        elif wait_timeout > 60.0:
            self.result.add_message(
                "TIMEOUT_TOO_LONG",
                f"wait_timeout ({wait_timeout}s) is very long. "
                f"This may cause requests to hang for extended periods.",
                ValidationLevel.WARNING,
                "pool.wait_timeout",
            )

    @abstractmethod
    def _validate_type_specific(self) -> None:
        """Run type-specific validations. Must be implemented by subclasses."""
        pass

    def _is_valid_hostname(self, hostname: str) -> bool:
        """Check if string is a valid hostname."""
        if not hostname or len(hostname) > 253:
            return False

        # Allow localhost
        if hostname in ("localhost", "127.0.0.1", "::1"):
            return True

        # Check hostname format
        if hostname[-1] == ".":
            hostname = hostname[:-1]

        allowed = re.compile(r"^(?!-)[A-Z0-9-]{1,63}(?<!-)$", re.IGNORECASE)
        return all(allowed.match(x) for x in hostname.split("."))

    def _is_valid_ip_address(self, ip: str) -> bool:
        """Check if string is a valid IP address."""
        try:
            socket.inet_aton(ip)
            return True
        except socket.error:
            pass

        try:
            socket.inet_pton(socket.AF_INET6, ip)
            return True
        except socket.error:
            pass

        return False

    def _is_reachable_port(self, host: str, port: int, timeout: float = 2.0) -> bool:
        """
        Check if a port is reachable.

        Note: This performs an actual network check and should be used sparingly.
        """
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except (socket.timeout, socket.error, OSError):
            return False


class PostgreSQLConnectionValidator(BaseConnectionValidator):
    """PostgreSQL connection validator."""

    # Default PostgreSQL port
    DEFAULT_PORT = 5432

    # Valid SSL modes
    SSL_MODES = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}

    def _validate_type_specific(self) -> None:
        """Validate PostgreSQL-specific settings."""
        settings = self.config.settings

        # Validate host
        host = settings.get("host")
        if host:
            self._validate_host(host)

        # Validate port
        port = settings.get("port", self.DEFAULT_PORT)
        self._validate_port(port)

        # Validate database name
        database = settings.get("database")
        if database:
            self._validate_database_name(database)

        # Validate credentials
        self._validate_credentials()

        # Validate SSL mode
        ssl_mode = settings.get("ssl_mode")
        if ssl_mode:
            self._validate_ssl_mode(ssl_mode)

        # Validate connection parameters
        self._validate_connection_params()

    def _validate_host(self, host: str) -> None:
        """Validate PostgreSQL host."""
        if not self._is_valid_hostname(host) and not self._is_valid_ip_address(host):
            self.result.add_message(
                "PG_INVALID_HOST",
                f"Host '{host}' is not a valid hostname or IP address",
                ValidationLevel.ERROR,
                "settings.host",
            )
        elif host == "localhost":
            self.result.add_message(
                "PG_LOCALHOST",
                "Using 'localhost' may cause issues in containerized environments. "
                "Consider using explicit IP or hostname.",
                ValidationLevel.INFO,
                "settings.host",
            )

    def _validate_port(self, port: Any) -> None:
        """Validate PostgreSQL port."""
        if not isinstance(port, int):
            self.result.add_message(
                "PG_INVALID_PORT_TYPE",
                f"Port must be an integer, got {type(port).__name__}",
                ValidationLevel.ERROR,
                "settings.port",
            )
            return

        if port < 1 or port > 65535:
            self.result.add_message(
                "PG_INVALID_PORT_RANGE",
                f"Port {port} is out of valid range (1-65535)",
                ValidationLevel.ERROR,
                "settings.port",
            )
        elif port != self.DEFAULT_PORT:
            self.result.add_message(
                "PG_NON_DEFAULT_PORT",
                f"Using non-default port {port}. Ensure this is intentional.",
                ValidationLevel.INFO,
                "settings.port",
            )

        # Check if port is well-known (may require privileges)
        if port in self.WELL_KNOWN_PORTS and port != self.DEFAULT_PORT:
            self.result.add_message(
                "PG_WELL_KNOWN_PORT",
                f"Port {port} is a well-known port and may require elevated privileges",
                ValidationLevel.WARNING,
                "settings.port",
            )

    def _validate_database_name(self, database: str) -> None:
        """Validate PostgreSQL database name."""
        if len(database) > 63:
            self.result.add_message(
                "PG_DATABASE_TOO_LONG",
                f"Database name '{database}' exceeds 63 characters",
                ValidationLevel.ERROR,
                "settings.database",
            )

        # Check for valid PostgreSQL identifier
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database):
            self.result.add_message(
                "PG_INVALID_DATABASE_NAME",
                f"Database name '{database}' contains invalid characters",
                ValidationLevel.ERROR,
                "settings.database",
            )

        # Check for reserved names
        reserved = {"postgres", "template0", "template1"}
        if database.lower() in reserved:
            self.result.add_message(
                "PG_RESERVED_DATABASE",
                f"Database name '{database}' is a reserved system database",
                ValidationLevel.WARNING,
                "settings.database",
            )

    def _validate_credentials(self) -> None:
        """Validate PostgreSQL credentials."""
        username = self.config.settings.get("username")
        password = self.config.settings.get("password")

        if not username:
            self.result.add_message(
                "PG_MISSING_USERNAME",
                "Username is required for PostgreSQL connection",
                ValidationLevel.ERROR,
                "settings.username",
            )

        if not password:
            self.result.add_message(
                "PG_MISSING_PASSWORD",
                "Password is required for PostgreSQL connection",
                ValidationLevel.WARNING,
                "settings.password",
            )
        elif isinstance(password, str) and len(password) < 8:
            self.result.add_message(
                "PG_WEAK_PASSWORD",
                "Password is very short (less than 8 characters)",
                ValidationLevel.WARNING,
                "settings.password",
            )

    def _validate_ssl_mode(self, ssl_mode: str) -> None:
        """Validate PostgreSQL SSL mode."""
        if ssl_mode.lower() not in self.SSL_MODES:
            self.result.add_message(
                "PG_INVALID_SSL_MODE",
                f"SSL mode '{ssl_mode}' is not valid. Valid modes: {', '.join(self.SSL_MODES)}",
                ValidationLevel.ERROR,
                "settings.ssl_mode",
            )
        elif ssl_mode.lower() == "disable":
            self.result.add_message(
                "PG_SSL_DISABLED",
                "SSL is disabled. This is insecure for production environments.",
                ValidationLevel.WARNING,
                "settings.ssl_mode",
            )

    def _validate_connection_params(self) -> None:
        """Validate additional connection parameters."""
        connect_args = self.config.settings.get("connect_args", {})

        if not isinstance(connect_args, dict):
            self.result.add_message(
                "PG_INVALID_CONNECT_ARGS",
                "connect_args must be a dictionary",
                ValidationLevel.ERROR,
                "settings.connect_args",
            )


class Neo4jConnectionValidator(BaseConnectionValidator):
    """Neo4j connection validator."""

    # Default Neo4j ports
    DEFAULT_BOLT_PORT = 7687
    DEFAULT_HTTP_PORT = 7474
    DEFAULT_HTTPS_PORT = 7473

    # Valid URI schemes
    VALID_SCHEMES = {"bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"}

    def _validate_type_specific(self) -> None:
        """Validate Neo4j-specific settings."""
        settings = self.config.settings

        # Validate URI
        uri = settings.get("uri")
        if uri:
            self._validate_uri(uri)

        # Validate credentials
        self._validate_credentials()

        # Validate connection timeout
        timeout = settings.get("connection_timeout")
        if timeout is not None:
            self._validate_timeout(timeout)

    def _validate_uri(self, uri: str) -> None:
        """Validate Neo4j URI."""
        # Parse URI
        match = re.match(r'^([a-z+]+)://([^:/]+)(?::(\d+))?(/.*)?$', uri)

        if not match:
            self.result.add_message(
                "NEO4J_INVALID_URI",
                f"URI '{uri}' has invalid format",
                ValidationLevel.ERROR,
                "settings.uri",
            )
            return

        scheme, host, port, path = match.groups()

        # Validate scheme
        if scheme not in self.VALID_SCHEMES:
            self.result.add_message(
                "NEO4J_INVALID_SCHEME",
                f"URI scheme '{scheme}' is not valid. Valid schemes: {', '.join(self.VALID_SCHEMES)}",
                ValidationLevel.ERROR,
                "settings.uri",
            )

        # Validate host
        if not self._is_valid_hostname(host) and not self._is_valid_ip_address(host):
            self.result.add_message(
                "NEO4J_INVALID_HOST",
                f"Host '{host}' is not a valid hostname or IP address",
                ValidationLevel.ERROR,
                "settings.uri",
            )

        # Validate port if specified
        if port:
            port_num = int(port)
            if port_num < 1 or port_num > 65535:
                self.result.add_message(
                    "NEO4J_INVALID_PORT",
                    f"Port {port_num} is out of valid range",
                    ValidationLevel.ERROR,
                    "settings.uri",
                )

        # Check for encryption
        if scheme in ("bolt", "neo4j"):
            self.result.add_message(
                "NEO4J_NO_ENCRYPTION",
                "Connection is not encrypted. Consider using bolt+s or neo4j+s scheme.",
                ValidationLevel.WARNING,
                "settings.uri",
            )

    def _validate_credentials(self) -> None:
        """Validate Neo4j credentials."""
        username = self.config.settings.get("username")
        password = self.config.settings.get("password")

        if not username:
            self.result.add_message(
                "NEO4J_MISSING_USERNAME",
                "Username is recommended for Neo4j connection",
                ValidationLevel.WARNING,
                "settings.username",
            )

        if not password:
            self.result.add_message(
                "NEO4J_MISSING_PASSWORD",
                "Password is recommended for Neo4j connection",
                ValidationLevel.WARNING,
                "settings.password",
            )

    def _validate_timeout(self, timeout: Any) -> None:
        """Validate connection timeout."""
        if not isinstance(timeout, (int, float)):
            self.result.add_message(
                "NEO4J_INVALID_TIMEOUT_TYPE",
                f"Timeout must be a number, got {type(timeout).__name__}",
                ValidationLevel.ERROR,
                "settings.connection_timeout",
            )
        elif timeout < 1.0:
            self.result.add_message(
                "NEO4J_TIMEOUT_TOO_SHORT",
                f"Timeout ({timeout}s) is very short",
                ValidationLevel.WARNING,
                "settings.connection_timeout",
            )
        elif timeout > 300.0:
            self.result.add_message(
                "NEO4J_TIMEOUT_TOO_LONG",
                f"Timeout ({timeout}s) is very long",
                ValidationLevel.WARNING,
                "settings.connection_timeout",
            )


class HTTPXConnectionValidator(BaseConnectionValidator):
    """HTTPX connection validator."""

    def _validate_type_specific(self) -> None:
        """Validate HTTPX-specific settings."""
        settings = self.config.settings

        # Validate base URL
        base_url = settings.get("base_url") or settings.get("url")
        if base_url:
            self._validate_base_url(base_url)

        # Validate timeout
        timeout = settings.get("timeout")
        if timeout is not None:
            self._validate_timeout(timeout)

        # Validate pool limits
        pool_limits = settings.get("pool_limits")
        if pool_limits:
            self._validate_pool_limits(pool_limits)

        # Validate headers
        headers = settings.get("headers")
        if headers:
            self._validate_headers(headers)

    def _validate_base_url(self, base_url: str) -> None:
        """Validate HTTPX base URL."""
        if not re.match(r'^https?://', base_url):
            self.result.add_message(
                "HTTPX_INVALID_URL_SCHEME",
                "base_url must start with http:// or https://",
                ValidationLevel.ERROR,
                "settings.base_url",
            )
            return

        # Parse URL
        match = re.match(r'^(https?)://([^:/]+)(?::(\d+))?(/.*)?$', base_url)

        if not match:
            self.result.add_message(
                "HTTPX_INVALID_URL",
                f"base_url '{base_url}' has invalid format",
                ValidationLevel.ERROR,
                "settings.base_url",
            )
            return

        scheme, host, port, path = match.groups()

        # Check for HTTPS
        if scheme == "http":
            self.result.add_message(
                "HTTPX_HTTP_NOT_SECURE",
                "Using HTTP instead of HTTPS. This is insecure for production.",
                ValidationLevel.WARNING,
                "settings.base_url",
            )

        # Validate host
        if not self._is_valid_hostname(host) and not self._is_valid_ip_address(host):
            self.result.add_message(
                "HTTPX_INVALID_HOST",
                f"Host '{host}' is not a valid hostname or IP address",
                ValidationLevel.ERROR,
                "settings.base_url",
            )

    def _validate_timeout(self, timeout: Any) -> None:
        """Validate HTTPX timeout."""
        if not isinstance(timeout, (int, float)):
            self.result.add_message(
                "HTTPX_INVALID_TIMEOUT_TYPE",
                f"Timeout must be a number, got {type(timeout).__name__}",
                ValidationLevel.ERROR,
                "settings.timeout",
            )
        elif timeout < 0.1:
            self.result.add_message(
                "HTTPX_TIMEOUT_TOO_SHORT",
                f"Timeout ({timeout}s) is extremely short",
                ValidationLevel.WARNING,
                "settings.timeout",
            )
        elif timeout > 300:
            self.result.add_message(
                "HTTPX_TIMEOUT_TOO_LONG",
                f"Timeout ({timeout}s) is very long",
                ValidationLevel.WARNING,
                "settings.timeout",
            )

    def _validate_pool_limits(self, pool_limits: Dict[str, Any]) -> None:
        """Validate HTTPX pool limits."""
        if not isinstance(pool_limits, dict):
            self.result.add_message(
                "HTTPX_INVALID_POOL_LIMITS_TYPE",
                "pool_limits must be a dictionary",
                ValidationLevel.ERROR,
                "settings.pool_limits",
            )
            return

        max_connections = pool_limits.get("max_connections")
        if max_connections is not None:
            if not isinstance(max_connections, int) or max_connections < 1:
                self.result.add_message(
                    "HTTPX_INVALID_MAX_CONNECTIONS",
                    "max_connections must be a positive integer",
                    ValidationLevel.ERROR,
                    "settings.pool_limits.max_connections",
                )
            elif max_connections > 1000:
                self.result.add_message(
                    "HTTPX_HIGH_MAX_CONNECTIONS",
                    f"max_connections ({max_connections}) is very high",
                    ValidationLevel.WARNING,
                    "settings.pool_limits.max_connections",
                )

    def _validate_headers(self, headers: Dict[str, str]) -> None:
        """Validate HTTPX headers."""
        if not isinstance(headers, dict):
            self.result.add_message(
                "HTTPX_INVALID_HEADERS_TYPE",
                "headers must be a dictionary",
                ValidationLevel.ERROR,
                "settings.headers",
            )
            return

        # Check for sensitive headers
        sensitive_headers = {"authorization", "x-api-key", "api-key", "cookie"}
        for header in headers.keys():
            if header.lower() in sensitive_headers:
                self.result.add_message(
                    "HTTPX_SENSITIVE_HEADER",
                    f"Header '{header}' may contain sensitive information. "
                    f"Ensure it is properly secured.",
                    ValidationLevel.INFO,
                    "settings.headers",
                )


class Boto3ConnectionValidator(BaseConnectionValidator):
    """Boto3 connection validator."""

    # Valid AWS regions pattern
    AWS_REGION_PATTERN = re.compile(r'^[a-z]{2}-[a-z]+-\d$')

    # Common AWS services
    COMMON_SERVICES = {
        "s3", "dynamodb", "sqs", "sns", "lambda", "ec2", "rds",
        "cloudwatch", "logs", "events", "secretsmanager", "ssm",
        "kms", "iam", "sts", "ecr", "ecs", "eks", "elb", "route53",
    }

    def _validate_type_specific(self) -> None:
        """Validate Boto3-specific settings."""
        settings = self.config.settings

        # Validate service name
        service_name = settings.get("service_name")
        if service_name:
            self._validate_service_name(service_name)

        # Validate region
        region = settings.get("region_name") or settings.get("region")
        if region:
            self._validate_region(region)

        # Validate endpoint URL
        endpoint_url = settings.get("endpoint_url")
        if endpoint_url:
            self._validate_endpoint_url(endpoint_url)

        # Validate credentials
        self._validate_credentials()

    def _validate_service_name(self, service_name: str) -> None:
        """Validate AWS service name."""
        if not service_name:
            self.result.add_message(
                "BOTO3_MISSING_SERVICE",
                "Service name is required for Boto3 connection",
                ValidationLevel.ERROR,
                "settings.service_name",
            )
            return

        if service_name.lower() not in self.COMMON_SERVICES:
            self.result.add_message(
                "BOTO3_UNCOMMON_SERVICE",
                f"Service '{service_name}' is not in common services list. "
                f"Ensure the service name is correct.",
                ValidationLevel.INFO,
                "settings.service_name",
            )

    def _validate_region(self, region: str) -> None:
        """Validate AWS region."""
        if not self.AWS_REGION_PATTERN.match(region):
            self.result.add_message(
                "BOTO3_INVALID_REGION",
                f"Region '{region}' does not match expected format (e.g., us-east-1)",
                ValidationLevel.WARNING,
                "settings.region_name",
            )

    def _validate_endpoint_url(self, endpoint_url: str) -> None:
        """Validate custom endpoint URL."""
        if not re.match(r'^https?://', endpoint_url):
            self.result.add_message(
                "BOTO3_INVALID_ENDPOINT",
                "endpoint_url must start with http:// or https://",
                ValidationLevel.ERROR,
                "settings.endpoint_url",
            )

        # Check for localhost endpoints (localstack, etc.)
        if "localhost" in endpoint_url or "127.0.0.1" in endpoint_url:
            self.result.add_message(
                "BOTO3_LOCAL_ENDPOINT",
                "Using localhost endpoint. This is for development/testing only.",
                ValidationLevel.INFO,
                "settings.endpoint_url",
            )

    def _validate_credentials(self) -> None:
        """Validate AWS credentials configuration."""
        # Check for explicit credentials (not recommended)
        aws_access_key_id = self.config.settings.get("aws_access_key_id")
        aws_secret_access_key = self.config.settings.get("aws_secret_access_key")

        if aws_access_key_id or aws_secret_access_key:
            self.result.add_message(
                "BOTO3_HARDCODED_CREDENTIALS",
                "Hardcoded AWS credentials detected. "
                "Consider using IAM roles or environment variables.",
                ValidationLevel.WARNING,
                "settings",
            )


def get_validator_for_config(
    config: ConnectionConfig,
) -> Optional[BaseConnectionValidator]:
    """
    Get the appropriate validator for a connection configuration.

    Args:
        config: Connection configuration

    Returns:
        Validator instance or None if type is not supported
    """
    validators = {
        "postgresql": PostgreSQLConnectionValidator,
        "neo4j": Neo4jConnectionValidator,
        "httpx": HTTPXConnectionValidator,
        "boto3": Boto3ConnectionValidator,
    }

    validator_class = validators.get(config.type.lower())
    if validator_class:
        return validator_class(config)

    return None


def validate_connection_config(
    config: ConnectionConfig,
) -> ConnectionValidationResult:
    """
    Validate a connection configuration using the appropriate validator.

    Args:
        config: Connection configuration to validate

    Returns:
        ConnectionValidationResult with validation status and messages
    """
    validator = get_validator_for_config(config)

    if validator is None:
        result = ConnectionValidationResult()
        result.add_message(
            "UNKNOWN_CONNECTION_TYPE",
            f"No validator available for connection type: {config.type}",
            ValidationLevel.ERROR,
        )
        return result

    return validator.validate()
