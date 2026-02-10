#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Plugin Registry for silvaengine_connections.

Manages dynamic registration and discovery of connection types.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type
import threading

from .connection import BaseConnection
from .connection_pool import BaseConnectionPool
from .exceptions import PluginNotFoundError, PluginAlreadyExistsError


@dataclass
class ConnectionPlugin:
    """Connection plugin data class."""
    type_name: str
    pool_class: Type[BaseConnectionPool]
    connection_class: Type[BaseConnection]


class PluginRegistry:
    """
    Plugin registry for managing connection types.
    
    Provides dynamic registration and discovery of connection types,
    enabling hot-pluggable connection pool management.
    
    Example:
        ```python
        registry = PluginRegistry()
        
        # Register a new connection type
        registry.register(
            type_name="postgresql",
            pool_class=PostgreSQLPool,
            connection_class=PostgreSQLConnection
        )
        
        # Get plugin
        plugin = registry.get("postgresql")
        ```
    """
    
    def __init__(self):
        """Initialize the plugin registry."""
        self._plugins: Dict[str, ConnectionPlugin] = {}
        self._lock = threading.RLock()
    
    def register(
        self,
        type_name: str,
        pool_class: Type[BaseConnectionPool],
        connection_class: Type[BaseConnection],
    ) -> None:
        """
        Register a connection type.
        
        Args:
            type_name: Connection type name (e.g., 'postgresql', 'neo4j')
            pool_class: Connection pool class
            connection_class: Connection class
            
        Raises:
            PluginAlreadyExistsError: If type is already registered
        """
        with self._lock:
            if type_name in self._plugins:
                raise PluginAlreadyExistsError(
                    f"Connection type '{type_name}' is already registered",
                    type_name=type_name
                )
            
            self._plugins[type_name] = ConnectionPlugin(
                type_name=type_name,
                pool_class=pool_class,
                connection_class=connection_class,
            )
    
    def unregister(self, type_name: str) -> bool:
        """
        Unregister a connection type.
        
        Args:
            type_name: Type name to unregister
            
        Returns:
            bool: True if unregistered, False if not found
        """
        with self._lock:
            if type_name in self._plugins:
                del self._plugins[type_name]
                return True
            return False
    
    def get(self, type_name: str) -> Optional[ConnectionPlugin]:
        """
        Get a plugin by type name.
        
        Args:
            type_name: Type name
            
        Returns:
            Optional[ConnectionPlugin]: Plugin or None if not found
        """
        return self._plugins.get(type_name)
    
    def get_safe(self, type_name: str) -> ConnectionPlugin:
        """
        Get a plugin by type name, raising exception if not found.
        
        Args:
            type_name: Type name
            
        Returns:
            ConnectionPlugin: Plugin
            
        Raises:
            PluginNotFoundError: If type is not registered
        """
        plugin = self.get(type_name)
        
        if plugin is None:
            raise PluginNotFoundError(
                f"Connection type '{type_name}' is not registered",
                type_name=type_name
            )
        return plugin
    
    def is_registered(self, type_name: str) -> bool:
        """
        Check if a type is registered.
        
        Args:
            type_name: Type name
            
        Returns:
            bool: True if registered
        """
        return type_name in self._plugins
    
    def get_all_types(self) -> List[str]:
        """
        Get all registered type names.
        
        Returns:
            List[str]: List of type names
        """
        with self._lock:
            return list(self._plugins.keys())
    
    def get_all_plugins(self) -> Dict[str, ConnectionPlugin]:
        """
        Get all registered plugins.
        
        Returns:
            Dict[str, ConnectionPlugin]: Dictionary of type to plugin
        """
        with self._lock:
            return self._plugins.copy()
    
    def clear(self) -> None:
        """Clear all registered plugins."""
        with self._lock:
            self._plugins.clear()
