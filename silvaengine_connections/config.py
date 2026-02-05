#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Configuration management for silvaengine_connections.

Provides configuration parsing, validation, and management for connection pools.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .exceptions import ConfigNotFoundError, ConfigValidationError


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

        if not self.type:
            errors.append("Connection type is required")

        if not isinstance(self.enabled, bool):
            errors.append("Enabled must be a boolean")

        # Validate pool parameters
        errors.extend(self._validate_pool_settings())

        return errors

    def _validate_pool_settings(self) -> List[str]:
        """Validate pool parameters."""
        errors = []

        min_size = self.pool_settings.get("min_size")
        max_size = self.pool_settings.get("max_size")

        if min_size is not None:
            if not isinstance(min_size, int) or min_size < 0:
                errors.append("min_size must be a non-negative integer")

        if max_size is not None:
            if not isinstance(max_size, int) or max_size < 1:
                errors.append("max_size must be a positive integer")

        if min_size is not None and max_size is not None:
            if min_size > max_size:
                errors.append("min_size cannot be greater than max_size")

        return errors

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
