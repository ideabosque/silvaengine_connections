#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Configuration Hot Loading for silvaengine_connections.

Provides runtime configuration reloading without application restart,
supporting file-based and callback-based configuration updates.
"""

import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from .config import ConfigManager, ConnectionConfig


class ConfigChangeType(Enum):
    """Configuration change type enumeration."""

    ADDED = "added"
    MODIFIED = "modified"
    REMOVED = "removed"


@dataclass
class ConfigChange:
    """
    Configuration change information.

    Attributes:
        change_type: Type of change (ADDED, MODIFIED, REMOVED)
        key: Configuration key
        old_value: Old configuration value (None for ADDED)
        new_value: New configuration value (None for REMOVED)
        timestamp: Change timestamp
    """

    change_type: ConfigChangeType
    key: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class HotReloadConfig:
    """
    Hot reload configuration.

    Attributes:
        enabled: Enable hot reload
        watch_paths: List of file paths to watch
        check_interval: Check interval in seconds
        validate_on_reload: Validate config before applying
        backup_on_reload: Create backup before reload
    """

    enabled: bool = True
    watch_paths: List[str] = field(default_factory=list)
    check_interval: float = 5.0
    validate_on_reload: bool = True
    backup_on_reload: bool = True


class ConfigChangeListener:
    """
    Configuration change listener.

    Callback interface for configuration changes.
    """

    def on_config_changed(self, changes: List[ConfigChange]) -> None:
        """
        Called when configuration changes are detected.

        Args:
            changes: List of configuration changes
        """
        pass


class ConfigFileWatcher:
    """
    File-based configuration watcher.

    Monitors configuration files for changes and triggers reload.
    """

    def __init__(
        self,
        file_paths: List[str],
        check_interval: float = 5.0,
    ) -> None:
        """
        Initialize the watcher.

        Args:
            file_paths: List of file paths to watch
            check_interval: Check interval in seconds
        """
        self._file_paths = [Path(p) for p in file_paths]
        self._check_interval = check_interval
        self._file_hashes: Dict[str, str] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

        for path in self._file_paths:
            if path.exists():
                self._file_hashes[str(path)] = self._compute_hash(path)

    def _compute_hash(self, path: Path) -> str:
        """
        Compute file content hash.

        Args:
            path: File path

        Returns:
            str: MD5 hash of file content
        """
        try:
            with open(path, "rb") as f:
                content = f.read()
                return hashlib.md5(content).hexdigest()
        except Exception:
            return ""

    def start(self, callback: Callable[[List[str]], None]) -> None:
        """
        Start watching files.

        Args:
            callback: Callback function called when files change
        """
        if self._running:
            return

        self._running = True
        self._callback = callback

        def watch_loop():
            changed_files: List[str] = []
            while self._running:
                try:
                    with self._lock:
                        for path in self._file_paths:
                            if not path.exists():
                                continue

                            current_hash = self._compute_hash(path)
                            old_hash = self._file_hashes.get(str(path), "")

                            if current_hash != old_hash:
                                changed_files.append(str(path))
                                self._file_hashes[str(path)] = current_hash

                    if changed_files:
                        self._callback(changed_files)
                        changed_files = []

                except Exception as e:
                    logging.getLogger(__name__).error(f"Watch error: {e}")

                time.sleep(self._check_interval)

        self._thread = threading.Thread(target=watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop watching files."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None


class ConfigHotReloader:
    """
    Configuration hot reloader.

    Provides runtime configuration reloading with validation
    and rollback support.
    """

    def __init__(
        self,
        config_manager: ConfigManager,
        hot_reload_config: Optional[HotReloadConfig] = None,
    ) -> None:
        """
        Initialize the reloader.

        Args:
            config_manager: Configuration manager
            hot_reload_config: Hot reload configuration
        """
        self._config_manager = config_manager
        self._config = hot_reload_config or HotReloadConfig()
        self._listeners: List[ConfigChangeListener] = []
        self._file_watcher: Optional[ConfigFileWatcher] = None
        self._lock = threading.Lock()
        self._running = False
        self._history: List[Dict[str, ConnectionConfig]] = []
        self._max_history = 10

        self._logger = logging.getLogger(f"{__name__}.ConfigHotReloader")

    def add_listener(self, listener: ConfigChangeListener) -> None:
        """
        Add a change listener.

        Args:
            listener: Listener to add
        """
        with self._lock:
            self._listeners.append(listener)

    def remove_listener(self, listener: ConfigChangeListener) -> None:
        """
        Remove a change listener.

        Args:
            listener: Listener to remove
        """
        with self._lock:
            if listener in self._listeners:
                self._listeners.remove(listener)

    def start(self) -> bool:
        """
        Start hot reload monitoring.

        Returns:
            bool: True if started successfully
        """
        if not self._config.enabled:
            self._logger.info("Hot reload is disabled")
            return False

        if not self._config.watch_paths:
            self._logger.warning("No watch paths configured")
            return False

        self._running = True
        self._file_watcher = ConfigFileWatcher(
            file_paths=self._config.watch_paths,
            check_interval=self._config.check_interval,
        )

        self._file_watcher.start(self._on_files_changed)
        self._logger.info(f"Hot reload started: {self._config.watch_paths}")
        return True

    def stop(self) -> None:
        """Stop hot reload monitoring."""
        self._running = False
        if self._file_watcher:
            self._file_watcher.stop()
            self._file_watcher = None
        self._logger.info("Hot reload stopped")

    def reload_config(
        self,
        config_dict: Dict[str, Any],
    ) -> bool:
        """
        Reload configuration.

        Args:
            config_dict: New configuration dictionary

        Returns:
            bool: True if reload succeeded
        """
        try:
            changes = self._detect_changes(config_dict)

            if not changes:
                self._logger.debug("No configuration changes detected")
                return True

            self._logger.info(f"Detected {len(changes)} configuration changes")

            if self._config.validate_on_reload:
                validation_errors = self._validate_config(config_dict)
                if validation_errors:
                    self._logger.error(f"Validation errors: {validation_errors}")
                    return False

            if self._config.backup_on_reload:
                self._backup_current_config()

            self._apply_config(config_dict)

            self._notify_listeners(changes)

            return True

        except Exception as e:
            self._logger.error(f"Failed to reload configuration: {e}")
            self._rollback()
            return False

    def _on_files_changed(self, changed_files: List[str]) -> None:
        """
        Handle file change event.

        Args:
            changed_files: List of changed file paths
        """
        self._logger.info(f"Configuration files changed: {changed_files}")

        for file_path in changed_files:
            try:
                config_dict = self._load_config_from_file(file_path)
                if config_dict:
                    self.reload_config(config_dict)
            except Exception as e:
                self._logger.error(f"Failed to reload from {file_path}: {e}")

    def _load_config_from_file(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        Load configuration from file.

        Args:
            file_path: Configuration file path

        Returns:
            Optional[Dict[str, Any]]: Configuration dictionary
        """
        path = Path(file_path)
        if not path.exists():
            return None

        with open(path, "r") as f:
            if path.suffix == ".json":
                return json.load(f)
            return {}

    def _detect_changes(
        self,
        new_config: Dict[str, Any],
    ) -> List[ConfigChange]:
        """
        Detect configuration changes.

        Args:
            new_config: New configuration

        Returns:
            List[ConfigChange]: List of changes
        """
        changes: List[ConfigChange] = []
        current_configs = self._config_manager.get_all_configs()

        current_keys = set(current_configs.keys())
        new_keys = set(new_config.get("pools", {}).keys())

        for key in new_keys - current_keys:
            changes.append(ConfigChange(
                change_type=ConfigChangeType.ADDED,
                key=key,
                new_value=new_config.get("pools", {}).get(key),
            ))

        for key in current_keys - new_keys:
            changes.append(ConfigChange(
                change_type=ConfigChangeType.REMOVED,
                key=key,
                old_value=current_configs.get(key),
            ))

        for key in current_keys & new_keys:
            old_value = current_configs[key].to_dict()
            new_value = new_config.get("pools", {}).get(key)
            if old_value != new_value:
                changes.append(ConfigChange(
                    change_type=ConfigChangeType.MODIFIED,
                    key=key,
                    old_value=old_value,
                    new_value=new_value,
                ))

        return changes

    def _validate_config(self, config_dict: Dict[str, Any]) -> List[str]:
        """
        Validate configuration.

        Args:
            config_dict: Configuration to validate

        Returns:
            List[str]: List of validation errors
        """
        errors: List[str] = []
        pools = config_dict.get("pools", {})

        for name, pool_config in pools.items():
            try:
                config = ConnectionConfig.from_dict(pool_config)
                validation_errors = config.validate()
                errors.extend([f"{name}: {e}" for e in validation_errors])
            except Exception as e:
                errors.append(f"{name}: {e}")

        return errors

    def _backup_current_config(self) -> None:
        """Backup current configuration."""
        current_configs = self._config_manager.get_all_configs()
        self._history.append(current_configs.copy())

        if len(self._history) > self._max_history:
            self._history.pop(0)

    def _apply_config(self, config_dict: Dict[str, Any]) -> None:
        """
        Apply new configuration.

        Args:
            config_dict: Configuration to apply
        """
        self._config_manager.load_from_dict(config_dict)
        self._logger.info("Configuration applied successfully")

    def _rollback(self) -> None:
        """Rollback to previous configuration."""
        if not self._history:
            self._logger.warning("No backup to rollback")
            return

        previous_config = self._history.pop()
        self._config_manager._configs.clear()
        self._config_manager._configs.update(previous_config)

        self._logger.info("Configuration rolled back")

    def _notify_listeners(self, changes: List[ConfigChange]) -> None:
        """
        Notify listeners of changes.

        Args:
            changes: List of changes
        """
        for listener in self._listeners:
            try:
                listener.on_config_changed(changes)
            except Exception as e:
                self._logger.error(f"Listener notification failed: {e}")

    def get_history(self) -> List[Dict[str, ConnectionConfig]]:
        """
        Get configuration history.

        Returns:
            List[Dict[str, ConnectionConfig]]: Configuration history
        """
        return self._history.copy()


class PoolConfigReloader:
    """
    Connection pool configuration reloader.

    Specialized reloader for connection pools with support
    for dynamic pool resizing and recreation.
    """

    def __init__(
        self,
        pool_manager: Any,
        hot_reloader: ConfigHotReloader,
    ) -> None:
        """
        Initialize the reloader.

        Args:
            pool_manager: Connection pool manager
            hot_reloader: Configuration hot reloader
        """
        self._pool_manager = pool_manager
        self._hot_reloader = hot_reloader
        self._logger = logging.getLogger(f"{__name__}.PoolConfigReloader")

    def reload_pool(
        self,
        pool_name: str,
        new_config: ConnectionConfig,
    ) -> bool:
        """
        Reload a specific pool configuration.

        Args:
            pool_name: Pool name
            new_config: New configuration

        Returns:
            bool: True if reload succeeded
        """
        try:
            pool = self._pool_manager.get_pool(pool_name)
            if not pool:
                self._logger.warning(f"Pool '{pool_name}' not found")
                return False

            pool.resize(
                new_min_size=new_config.pool_settings.get("min_size", 2),
                new_max_size=new_config.pool_settings.get("max_size", 10),
            )

            self._logger.info(f"Pool '{pool_name}' reloaded")
            return True

        except Exception as e:
            self._logger.error(f"Failed to reload pool '{pool_name}': {e}")
            return False


__all__ = [
    "ConfigChangeType",
    "ConfigChange",
    "HotReloadConfig",
    "ConfigChangeListener",
    "ConfigFileWatcher",
    "ConfigHotReloader",
    "PoolConfigReloader",
]
