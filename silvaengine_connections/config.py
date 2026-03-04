#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Configuration management for silvaengine_connections.

Provides configuration parsing, validation, and management for connection pools.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

from .exceptions import ConfigNotFoundError, ConfigValidationError


# Valid connection types
VALID_CONNECTION_TYPES: Set[str] = {
    "postgresql",
    "neo4j",
    "httpx",
    "boto3",
}

# Reserved pool names that cannot be used
RESERVED_POOL_NAMES: Set[str] = {
    "default",
    "global",
    "system",
    "internal",
}


@dataclass
class ValidationRule:
    """Validation rule for configuration fields."""

    field: str
    required: bool = False
    field_type: type = str
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    pattern: Optional[str] = None
    allowed_values: Optional[Set[str]] = None
    custom_validator: Optional[callable] = None


@dataclass
class ConnectionConfig:
    """
    Connection configuration data class.

    Attributes:
        type: Connection type (postgresql, neo4j, httpx, boto3)
        enabled: Whether the connection is enabled
        settings: Connection-specific parameters
        pool_settings: Pool configuration parameters
    """

    type: str
    enabled: bool = True
    settings: Dict[str, Any] = field(default_factory=dict)
    pool_settings: Dict[str, Any] = field(default_factory=dict)

    # Connection type specific validation rules
    _VALIDATION_RULES: Dict[str, List[ValidationRule]] = field(default_factory=dict, repr=False)

    def __post_init__(self):
        """Initialize validation rules after creation."""
        self._VALIDATION_RULES = {
            "postgresql": self._get_postgresql_rules(),
            "neo4j": self._get_neo4j_rules(),
            "httpx": self._get_httpx_rules(),
            "boto3": self._get_boto3_rules(),
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ConnectionConfig":
        """
        Create ConnectionConfig from dictionary.

        Args:
            config_dict: Configuration dictionary

        Returns:
            ConnectionConfig: Configuration object

        Raises:
            ConfigValidationError: If configuration is invalid
        """
        if not isinstance(config_dict, dict):
            raise ConfigValidationError("Configuration must be a dictionary")

        type_name = config_dict.get("type")

        if not type_name:
            raise ConfigValidationError("Connection type is required", field="type")

        if not isinstance(type_name, str):
            raise ConfigValidationError(
                "Connection type must be a string", field="type"
            )

        # Normalize type name
        type_name = type_name.strip().lower()

        return cls(
            type=type_name,
            enabled=config_dict.get("enabled", True),
            settings=config_dict.get("settings", {}),
            pool_settings=config_dict.get("pool", {}),
        )

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary.

        Returns:
            Dict[str, Any]: Configuration dictionary
        """
        return {
            "type": self.type,
            "enabled": self.enabled,
            "settings": self.settings,
            "pool": self.pool_settings,
        }

    def validate(self) -> List[str]:
        """
        Validate configuration.

        Returns:
            List[str]: List of validation errors (empty if valid)
        """
        errors = []

        # Validate connection type
        if not self.type:
            errors.append("Connection type is required")
        elif not isinstance(self.type, str):
            errors.append("Connection type must be a string")

        # Validate enabled field
        if not isinstance(self.enabled, bool):
            errors.append("Enabled must be a boolean")

        # Validate pool parameters
        errors.extend(self._validate_pool_settings())

        # Validate connection type specific settings
        errors.extend(self._validate_connection_type_settings())

        # Validate security settings
        errors.extend(self._validate_security_settings())

        return errors

    def validate_strict(self) -> None:
        """
        Validate configuration strictly, raising exception on any error.

        Raises:
            ConfigValidationError: If configuration is invalid
        """
        errors = self.validate()
        if errors:
            raise ConfigValidationError(
                f"Configuration validation failed: {'; '.join(errors)}"
            )

    def _validate_pool_settings(self) -> List[str]:
        """Validate pool parameters."""
        errors = []

        min_size = self.pool_settings.get("min_size")
        max_size = self.pool_settings.get("max_size")
        max_idle_time = self.pool_settings.get("max_idle_time")
        max_lifetime = self.pool_settings.get("max_lifetime")
        wait_timeout = self.pool_settings.get("wait_timeout")
        health_check_interval = self.pool_settings.get("health_check_interval")

        # Validate min_size
        if min_size is not None:
            if not isinstance(min_size, int) or min_size < 0:
                errors.append("min_size must be a non-negative integer")
            elif min_size > 100:
                errors.append("min_size cannot exceed 100")

        # Validate max_size
        if max_size is not None:
            if not isinstance(max_size, int) or max_size < 1:
                errors.append("max_size must be a positive integer")
            elif max_size > 1000:
                errors.append("max_size cannot exceed 1000")

        # Validate min_size <= max_size
        if (
            min_size is not None
            and max_size is not None
            and min_size > max_size
        ):
            errors.append("min_size cannot be greater than max_size")

        # Validate timeout values
        for field_name, value in [
            ("max_idle_time", max_idle_time),
            ("max_lifetime", max_lifetime),
            ("wait_timeout", wait_timeout),
            ("health_check_interval", health_check_interval),
        ]:
            if value is not None:
                if not isinstance(value, (int, float)):
                    errors.append(f"{field_name} must be a number")
                elif value < 0:
                    errors.append(f"{field_name} must be non-negative")
                elif value > 86400:  # 24 hours
                    errors.append(f"{field_name} cannot exceed 86400 seconds (24 hours)")

        # Validate enable_dynamic_resize
        enable_dynamic_resize = self.pool_settings.get("enable_dynamic_resize")
        if enable_dynamic_resize is not None and not isinstance(enable_dynamic_resize, bool):
            errors.append("enable_dynamic_resize must be a boolean")

        return errors

    def _validate_connection_type_settings(self) -> List[str]:
        """Validate connection type specific settings."""
        if self.type == "postgresql":
            return self._validate_postgresql_settings()
        elif self.type == "neo4j":
            return self._validate_neo4j_settings()
        elif self.type == "httpx":
            return self._validate_httpx_settings()
        elif self.type == "boto3":
            return self._validate_boto3_settings()
        else:
            return [f"Unknown connection type: {self.type}"]

    def _validate_postgresql_settings(self) -> List[str]:
        """Validate PostgreSQL-specific settings."""
        errors = []
        settings = self.settings

        # Required fields
        required_fields = ["host", "database"]
        for field in required_fields:
            if field not in settings or not settings[field]:
                errors.append(f"PostgreSQL settings missing required field: {field}")

        # Validate host format
        host = settings.get("host")
        if host and isinstance(host, str):
            # Check for invalid characters
            if " " in host:
                errors.append("PostgreSQL host cannot contain spaces")
            # Check for valid hostname or IP
            if not self._is_valid_host(host):
                errors.append(f"PostgreSQL host '{host}' has invalid format")

        # Validate port
        port = settings.get("port")
        if port is not None:
            if not isinstance(port, int) or port < 1 or port > 65535:
                errors.append("PostgreSQL port must be an integer between 1 and 65535")

        # Validate database name
        database = settings.get("database")
        if database and isinstance(database, str):
            if len(database) > 63:
                errors.append("PostgreSQL database name cannot exceed 63 characters")
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', database):
                errors.append("PostgreSQL database name contains invalid characters")

        # Validate username
        username = settings.get("username")
        if username and isinstance(username, str):
            if len(username) > 63:
                errors.append("PostgreSQL username cannot exceed 63 characters")

        # Validate SSL mode
        ssl_mode = settings.get("ssl_mode")
        if ssl_mode and isinstance(ssl_mode, str):
            valid_ssl_modes = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
            if ssl_mode.lower() not in valid_ssl_modes:
                errors.append(f"PostgreSQL ssl_mode must be one of: {', '.join(valid_ssl_modes)}")

        return errors

    def _validate_neo4j_settings(self) -> List[str]:
        """Validate Neo4j-specific settings."""
        errors = []
        settings = self.settings

        # Required fields
        required_fields = ["uri"]
        for field in required_fields:
            if field not in settings or not settings[field]:
                errors.append(f"Neo4j settings missing required field: {field}")

        # Validate URI format
        uri = settings.get("uri", "")
        if uri and isinstance(uri, str):
            valid_schemes = {"bolt", "bolt+s", "bolt+ssc", "neo4j", "neo4j+s", "neo4j+ssc"}
            scheme_match = re.match(r'^([a-z+]+)://', uri)
            if not scheme_match:
                errors.append("Neo4j URI must have a valid scheme (e.g., bolt://, neo4j://)")
            elif scheme_match.group(1) not in valid_schemes:
                errors.append(f"Neo4j URI scheme must be one of: {', '.join(valid_schemes)}")

        # Validate connection timeout
        connection_timeout = settings.get("connection_timeout")
        if connection_timeout is not None:
            if not isinstance(connection_timeout, (int, float)) or connection_timeout <= 0:
                errors.append("Neo4j connection_timeout must be a positive number")

        return errors

    def _validate_httpx_settings(self) -> List[str]:
        """Validate HTTPX-specific settings."""
        errors = []
        settings = self.settings

        # Validate base_url
        base_url = settings.get("base_url") or settings.get("url")
        if base_url and isinstance(base_url, str):
            if not re.match(r'^https?://', base_url):
                errors.append("HTTPX base_url must start with http:// or https://")

        # Validate timeout
        timeout = settings.get("timeout")
        if timeout is not None:
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                errors.append("HTTPX timeout must be a positive number")
            elif timeout > 300:
                errors.append("HTTPX timeout cannot exceed 300 seconds")

        # Validate pool_limits
        pool_limits = settings.get("pool_limits")
        if pool_limits and isinstance(pool_limits, dict):
            max_connections = pool_limits.get("max_connections")
            if max_connections is not None:
                if not isinstance(max_connections, int) or max_connections < 1:
                    errors.append("HTTPX pool_limits.max_connections must be a positive integer")

        return errors

    def _validate_boto3_settings(self) -> List[str]:
        """Validate Boto3-specific settings."""
        errors = []
        settings = self.settings

        # Required fields
        if "service_name" not in settings or not settings["service_name"]:
            errors.append("Boto3 settings missing required field: service_name")

        # Validate service name
        service_name = settings.get("service_name")
        if service_name and isinstance(service_name, str):
            valid_services = {
                "s3", "dynamodb", "sqs", "sns", "lambda", "ec2", "rds",
                "cloudwatch", "logs", "events", "secretsmanager", "ssm",
            }
            if service_name.lower() not in valid_services:
                errors.append(f"Boto3 service_name '{service_name}' may not be valid")

        # Validate region
        region = settings.get("region_name") or settings.get("region")
        if region and isinstance(region, str):
            # Basic AWS region format validation
            if not re.match(r'^[a-z]{2}-[a-z]+-\d$', region):
                errors.append(f"Boto3 region '{region}' has invalid format")

        # Validate max_pool_connections
        max_pool_connections = settings.get("max_pool_connections")
        if max_pool_connections is not None:
            if not isinstance(max_pool_connections, int) or max_pool_connections < 1:
                errors.append("Boto3 max_pool_connections must be a positive integer")
            elif max_pool_connections > 1000:
                errors.append("Boto3 max_pool_connections cannot exceed 1000")

        return errors

    def _validate_security_settings(self) -> List[str]:
        """Validate security-related settings."""
        errors = []
        warnings = []

        # Check for hardcoded credentials
        sensitive_patterns = [
            (r'password', 'password'),
            (r'secret', 'secret'),
            (r'api[_-]?key', 'api_key'),
            (r'access[_-]?key', 'access_key'),
            (r'private[_-]?key', 'private_key'),
        ]

        def check_for_hardcoded(obj: Any, path: str = "") -> None:
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    for pattern, name in sensitive_patterns:
                        if re.search(pattern, key, re.IGNORECASE):
                            if isinstance(value, str) and value and len(value) > 8:
                                if not value.startswith(("$", "{", "env:")):
                                    warnings.append(
                                        f"Field '{current_path}' may contain hardcoded {name}. "
                                        f"Consider using environment variables"
                                    )
                    check_for_hardcoded(value, current_path)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    check_for_hardcoded(item, f"{path}[{i}]")

        check_for_hardcoded(self.settings, "settings")
        check_for_hardcoded(self.pool_settings, "pool")

        # Log warnings but don't add as errors
        return errors

    def _is_valid_host(self, host: str) -> bool:
        """Check if host is a valid hostname or IP address."""
        if not host:
            return False

        # Check for localhost
        if host in ("localhost", "127.0.0.1", "::1"):
            return True

        # Check for IP address
        if re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', host):
            return all(0 <= int(x) <= 255 for x in host.split("."))

        # Check for hostname (simplified)
        if re.match(r'^[a-zA-Z0-9][-a-zA-Z0-9.]*[a-zA-Z0-9]$', host):
            return True

        return False

    def _get_postgresql_rules(self) -> List[ValidationRule]:
        """Get PostgreSQL validation rules."""
        return [
            ValidationRule("host", required=True, field_type=str),
            ValidationRule("port", required=False, field_type=int, min_value=1, max_value=65535),
            ValidationRule("database", required=True, field_type=str),
            ValidationRule("username", required=False, field_type=str),
            ValidationRule("password", required=False, field_type=str),
        ]

    def _get_neo4j_rules(self) -> List[ValidationRule]:
        """Get Neo4j validation rules."""
        return [
            ValidationRule("uri", required=True, field_type=str),
            ValidationRule("username", required=False, field_type=str),
            ValidationRule("password", required=False, field_type=str),
        ]

    def _get_httpx_rules(self) -> List[ValidationRule]:
        """Get HTTPX validation rules."""
        return [
            ValidationRule("base_url", required=False, field_type=str),
            ValidationRule("timeout", required=False, field_type=(int, float), min_value=0.1, max_value=300),
        ]

    def _get_boto3_rules(self) -> List[ValidationRule]:
        """Get Boto3 validation rules."""
        return [
            ValidationRule("service_name", required=True, field_type=str),
            ValidationRule("region_name", required=False, field_type=str),
            ValidationRule("endpoint_url", required=False, field_type=str),
        ]

    def get_pool_setting(self, name: str, default: Any = None) -> Any:
        """
        Get a pool parameter.

        Args:
            name: Parameter name
            default: Default value if not found

        Returns:
            Any: Parameter value
        """
        return self.pool_settings.get(name, default)

    def get_setting(self, name: str, default: Any = None) -> Any:
        """
        Get a connection parameter.

        Args:
            name: Parameter name
            default: Default value if not found

        Returns:
            Any: Parameter value
        """
        return self.settings.get(name, default)

    def is_valid(self) -> bool:
        """
        Quick check if configuration is valid.

        Returns:
            bool: True if valid
        """
        return len(self.validate()) == 0


class ConfigManager:
    """
    Configuration manager for connection pools.

    Provides centralized configuration management with validation
    and default value handling.
    """

    # Default pool parameters
    DEFAULT_POOL_SETTINGS = {
        "min_size": 2,
        "max_size": 10,
        "max_idle_time": 300.0,
        "max_lifetime": 3600.0,
        "wait_timeout": 10.0,
        "health_check_interval": 30.0,
        "enable_dynamic_resize": True,
    }

    def __init__(self):
        """Initialize the configuration manager."""
        self._configs: Dict[str, ConnectionConfig] = {}

    def add_config(self, name: str, config: ConnectionConfig) -> None:
        """
        Add a connection configuration.

        Args:
            name: Configuration name
            config: Configuration object
        """
        self._configs[name] = config

    def get_config(self, name: str) -> Optional[ConnectionConfig]:
        """
        Get a connection configuration.

        Args:
            name: Configuration name

        Returns:
            Optional[ConnectionConfig]: Configuration or None
        """
        return self._configs.get(name)

    def get_config_safe(self, name: str) -> ConnectionConfig:
        """
        Get a connection configuration, raising exception if not found.

        Args:
            name: Configuration name

        Returns:
            ConnectionConfig: Configuration

        Raises:
            ConfigNotFoundError: If configuration not found
        """
        config = self.get_config(name)
        
        if config is None:
            raise ConfigNotFoundError(f"Configuration '{name}' not found")
        return config

    def remove_config(self, name: str) -> bool:
        """
        Remove a configuration.

        Args:
            name: Configuration name

        Returns:
            bool: True if removed
        """
        if name in self._configs:
            del self._configs[name]
            return True
        return False

    def load_from_dict(self, config_dict: Dict[str, Any]) -> List[str]:
        """
        Load configurations from dictionary.

        Args:
            config_dict: Configuration dictionary with format:
                {
                    'pools': {
                        'pool_name': {
                            'type': 'postgresql',
                            'enabled': True,
                            'settings': {...},
                            'pool': {...}
                        }
                    }
                }

        Returns:
            List[str]: List of loaded configuration names
        """
        loaded = []
        pools_config = config_dict.get("pools", {})

        for name, pool_config in pools_config.items():
            try:
                config = ConnectionConfig.from_dict(pool_config)
                self.add_config(name, config)
                loaded.append(name)
            except ConfigValidationError as e:
                # Log error but continue loading other configs
                print(f"Failed to load config '{name}': {e}")

        return loaded

    def get_all_configs(self) -> Dict[str, ConnectionConfig]:
        """
        Get all configurations.

        Returns:
            Dict[str, ConnectionConfig]: Dictionary of name to config
        """
        return self._configs.copy()

    def apply_defaults(self, config: ConnectionConfig) -> ConnectionConfig:
        """
        Apply default values to configuration.

        Args:
            config: Configuration to apply defaults to

        Returns:
            ConnectionConfig: Configuration with defaults applied
        """
        # Apply default pool parameters
        for key, value in self.DEFAULT_POOL_SETTINGS.items():
            if key not in config.pool_settings:
                config.pool_settings[key] = value

        return config

    def clear(self) -> None:
        """Clear all configurations."""
        self._configs.clear()
