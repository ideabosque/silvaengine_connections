"""
Connection Types Module

Provides implementations of various connection types, including:
- PostgreSQL: Relational database connection
- Neo4j: Graph database connection
- HTTPX: HTTP client connection
- Boto3: AWS service connection

Optional dependencies (neo4j, httpx, boto3) are imported with try/except
guarding so that a missing optional dependency does not break the entire
connections package — each connection type is independently available.
"""

__all__ = [
    # PostgreSQL
    'PostgreSQLConnection',
    'PostgreSQLConnectionPool',
    'register_postgresql_plugin',
    # Neo4j
    'Neo4jConnection',
    'Neo4jConnectionPool',
    'register_neo4j_plugin',
    # HTTPX
    'HTTPXConnection',
    'HTTPXConnectionPool',
    'register_httpx_plugin',
    # Boto3
    'Boto3Connection',
    'Boto3ConnectionPool',
    'register_boto3_plugin',
]

from .postgresql import PostgreSQLConnection, PostgreSQLConnectionPool, register_postgresql_plugin

try:
    from .neo4j import Neo4jConnection, Neo4jConnectionPool, register_neo4j_plugin
except ImportError:
    Neo4jConnection = None
    Neo4jConnectionPool = None
    register_neo4j_plugin = None

try:
    from .httpx import HTTPXConnection, HTTPXConnectionPool, register_httpx_plugin
except ImportError:
    HTTPXConnection = None
    HTTPXConnectionPool = None
    register_httpx_plugin = None

try:
    from .boto3 import Boto3Connection, Boto3ConnectionPool, register_boto3_plugin
except ImportError:
    Boto3Connection = None
    Boto3ConnectionPool = None
    register_boto3_plugin = None
