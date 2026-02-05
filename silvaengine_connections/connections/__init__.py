"""
Connection Types Module

Provides implementations of various connection types, including:
- PostgreSQL: Relational database connection
- Neo4j: Graph database connection
- HTTPX: HTTP client connection
- Boto3: AWS service connection
"""

from .postgresql import PostgreSQLConnection, PostgreSQLConnectionPool, register_postgresql_plugin
from .neo4j import Neo4jConnection, Neo4jConnectionPool, register_neo4j_plugin
from .httpx import HTTPXConnection, HTTPXConnectionPool, register_httpx_plugin
from .boto3 import Boto3Connection, Boto3ConnectionPool, register_boto3_plugin

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
