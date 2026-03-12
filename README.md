# silvaengine_connections

A comprehensive hot-pluggable connection pool management system for AWS Lambda, supporting PostgreSQL, Neo4j, HTTPX, and Boto3 connections.

## Features

- **Hot-pluggable connection type registration** - Dynamically register/unregister connection types at runtime
- **Context manager support** - Safe connection handling with `with` statements
- **Dynamic pool resizing** - Automatically adjust pool size based on load
- **Health monitoring** - Built-in health checks and circuit breaker pattern
- **Comprehensive metrics** - Detailed statistics for monitoring and debugging
- **Thread-safe operations** - All operations are thread-safe for concurrent access
- **Lifecycle management** - Complete connection lifecycle management with events
- **Configuration validation** - Strict validation for all connection configurations

## Supported Connection Types

| Connection Type | File Path                   | Dependencies         | Description                                 |
| --------------- | --------------------------- | -------------------- | ------------------------------------------- |
| **PostgreSQL**  | `connections/postgresql.py` | SQLAlchemy, psycopg2 | SQLAlchemy-based PostgreSQL connection pool |
| **Neo4j**       | `connections/neo4j.py`      | neo4j-driver         | Neo4j graph database connection pool        |
| **HTTPX**       | `connections/httpx.py`      | httpx[http2]         | Async HTTP client connection pool           |
| **Boto3**       | `connections/boto3.py`      | boto3                | AWS service connection pool                 |

## Installation

```bash
pip install silvaengine_connections
```

For specific connection types, install optional dependencies:

```bash
# PostgreSQL support
pip install silvaengine_connections[postgresql]

# Neo4j support
pip install silvaengine_connections[neo4j]

# HTTPX support
pip install silvaengine_connections[httpx]

# Boto3 support
pip install silvaengine_connections[boto3]

# All connection types
pip install silvaengine_connections[all]
```

## Quick Start

### Basic Usage

```python
from silvaengine_connections import ConnectionPoolManager

# Get the singleton manager instance
manager = ConnectionPoolManager()

# Initialize from configuration
config = {
    "postgres_main": {
        "type": "postgresql",
        "enabled": True,
        "settings": {
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "username": "user",
            "password": "pass"
        },
        "pool": {
            "min_size": 2,
            "max_size": 10,
            "max_lifetime": 3600
        }
    }
}

# Create pools from configuration
manager.create_pools_from_config(config)

# Use connection with context manager
with manager.connection("postgres_main") as conn:
    result = conn.execute("SELECT * FROM users")
    print(result.fetchall())
```

### Using the Convenience `init()` Function

```python
from silvaengine_connections import init

config = {
    "postgresql": {
        "type": "postgresql",
        "enabled": True,
        "settings": {
            "host": "localhost",
            "port": 5432,
            "database": "mydb",
            "username": "user",
            "password": "pass"
        },
        "pool": {
            "min_size": 2,
            "max_size": 10
        }
    }
}

manager = init(config)
pool = manager.get_pool("postgresql")

with pool.connection() as conn:
    result = conn.execute("SELECT 1")
```

## Architecture

### Core Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    ConnectionPoolManager                        │
│              (Singleton, Hot-pluggable Controller)              │
├─────────────────────────────────────────────────────────────────┤
│  ┌───────────────┐  ┌───────────────┐  ┌─────────────────────┐  │
│  │PluginRegistry │  │ ConfigManager │  │   CircuitBreaker    │  │
│  │ (Plugin Hub)  │  │  (Config Mgr) │  │  (Fault Tolerance)  │  │
│  └───────┬───────┘  └───────┬───────┘  └──────────┬──────────┘  │
│          │                  │                     │             │
│          └──────────────────┼─────────────────────┘             │
│                             │                                   │
│                             ▼                                   │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │              BaseConnectionPool (Abstract)                │  │
│  │         (Connection Pool Abstract Base Class)             │  │
│  └─────────┬─────────────────────┬─────────────────────┬─────┘  │
│            │                     │                     │        │
│            ▼                     ▼                     ▼        │
│  ┌─────────────────┐       ┌────────────┐       ┌────────────┐  │
│  │ PostgreSQL Pool │       │ Neo4j Pool │       │ HTTPX Pool │  │
│  └─────────────────┘       └────────────┘       └────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Module Structure

| Module                  | File Path                 | Responsibility              | Key Classes                                                    |
| ----------------------- | ------------------------- | --------------------------- | -------------------------------------------------------------- |
| **Connection Abstract** | `connection.py`           | Define connection interface | `BaseConnection`                                               |
| **Pool Abstract**       | `connection_pool.py`      | Define pool interface       | `BaseConnectionPool`, `PoolMetrics`, `PoolStatus`              |
| **Pool Manager**        | `pool_manager.py`         | Hot-pluggable core          | `ConnectionPoolManager`                                        |
| **Plugin Registry**     | `plugin_registry.py`      | Dynamic type registration   | `PluginRegistry`, `ConnectionPlugin`                           |
| **Configuration**       | `config.py`               | Parse and validate config   | `ConnectionConfig`, `ConfigManager`                            |
| **Lifecycle**           | `lifecycle.py`            | Connection lifecycle        | `ConnectionLifecycleManager`, `ConnectionPoolLifecycleManager` |
| **Circuit Breaker**     | `circuit_breaker.py`      | Fault tolerance             | `CircuitBreaker`, `CircuitBreakerConfig`                       |
| **Exceptions**          | `exceptions.py`           | Exception hierarchy         | `ConnectionError`, `PoolError`                                 |
| **Integration**         | `integration.py`          | PluginManager integration   | `ConnectionPluginIntegration`                                  |
| **Validators**          | `connection_validator.py` | Config validation           | `BaseConnectionValidator`                                      |

## API Reference

### ConnectionPoolManager

The central singleton class for managing connection pools.

```python
from silvaengine_connections import ConnectionPoolManager

# Get singleton instance
manager = ConnectionPoolManager()
# or
manager = ConnectionPoolManager.get_instance()
```

#### Methods

**register_connection_type(type_name, pool_class, connection_class)**

Register a new connection type dynamically.

```python
from silvaengine_connections.connections.postgresql import PostgreSQLConnectionPool, PostgreSQLConnection

manager.register_connection_type(
    type_name="postgresql",
    pool_class=PostgreSQLConnectionPool,
    connection_class=PostgreSQLConnection
)
```

**create_pool(name, config)**

Create a connection pool from configuration.

```python
from silvaengine_connections import ConnectionConfig

config = ConnectionConfig(
    type="postgresql",
    enabled=True,
    settings={"host": "localhost", "database": "mydb"},
    pool_settings={"min_size": 2, "max_size": 10}
)

pool = manager.create_pool("postgres_main", config)
```

**get_pool(name)**

Get a pool by name.

```python
pool = manager.get_pool("postgres_main")
if pool:
    with pool.connection() as conn:
        result = conn.execute("SELECT 1")
```

**get_pool_safe(name)**

Get a pool, raising exception if not found.

```python
try:
    pool = manager.get_pool_safe("postgres_main")
except PoolNotFoundError:
    print("Pool not configured")
```

**connection(pool_name=None)**

Get a connection context manager directly from the manager.

```python
# Use first available pool
with manager.connection() as conn:
    result = conn.execute("SELECT 1")

# Use specific pool
with manager.connection("postgres_main") as conn:
    result = conn.execute("SELECT 1")
```

**create_pools_from_config(pools_config)**

Batch create pools from configuration dictionary.

```python
pools_config = {
    "postgres_main": {
        "type": "postgresql",
        "enabled": True,
        "settings": {...},
        "pool": {...}
    },
    "neo4j_main": {
        "type": "neo4j",
        "enabled": True,
        "settings": {...}
    }
}

created = manager.create_pools_from_config(pools_config)
print(f"Created pools: {created}")
```

**remove_pool(name)**

Remove a pool and close all its connections.

```python
success = manager.remove_pool("postgres_main")
```

**shutdown_all()**

Close all pools and release resources.

```python
manager.shutdown_all()
```

**get_all_metrics()**

Get metrics for all pools.

```python
metrics = manager.get_all_metrics()
for name, pool_metrics in metrics.items():
    print(f"{name}: {pool_metrics.active_connections} active")
```

### BaseConnectionPool

Abstract base class for all connection pools.

```python
from silvaengine_connections import BaseConnectionPool

# Acquire and release manually
conn = pool.acquire()
try:
    result = conn.execute("SELECT 1")
finally:
    pool.release(conn)

# Use context manager (recommended)
with pool.connection() as conn:
    result = conn.execute("SELECT 1")
```

#### Properties

- `name` - Pool name
- `status` - Pool status (INITIALIZING, READY, PAUSED, SHUTDOWN)
- `metrics` - Pool metrics (PoolMetrics)
- `circuit_breaker` - Circuit breaker instance

#### Methods

**acquire()**

Acquire a connection from the pool.

**release(connection)**

Release a connection back to the pool.

**connection()**

Context manager for automatic connection management.

**resize(new_min_size, new_max_size)**

Dynamically resize the pool.

```python
pool.resize(min_size=5, max_size=20)
```

### BaseConnection

Abstract base class for all connections.

#### Properties

- `connection_id` - Unique connection identifier
- `is_used` - Whether connection is in use
- `is_closed` - Whether connection is closed
- `raw_connection` - Underlying connection object
- `config` - Connection configuration

#### Methods

**connect()**

Establish the underlying connection.

**close()**

Close the connection and release resources.

**is_healthy()**

Check if the connection is healthy.

**reset()**

Reset connection state for return to pool.

**get_lifetime()**

Get total lifetime of the connection.

**get_idle_time()**

Get idle time of the connection.

### Circuit Breaker

Fault tolerance mechanism for connection operations.

```python
from silvaengine_connections import CircuitBreaker, CircuitBreakerConfig

config = CircuitBreakerConfig(
    failure_threshold=5,    # Open after 5 consecutive failures
    success_threshold=3,    # Close after 3 consecutive successes
    timeout=60.0,           # Try half-open after 60 seconds
    half_open_max_calls=3   # Max calls in half-open state
)

breaker = CircuitBreaker("postgres_breaker", config)

# Use circuit breaker
result = breaker.call(some_function, arg1, arg2)
```

## Configuration

### Common Configuration Parameters

| Parameter               | Type  | Required | Default | Description                                    |
| ----------------------- | ----- | -------- | ------- | ---------------------------------------------- |
| `type`                  | str   | Yes      | -       | Connection type (postgresql/neo4j/httpx/boto3) |
| `enabled`               | bool  | No       | True    | Whether to enable the pool                     |
| `min_size`              | int   | No       | 2       | Minimum number of connections                  |
| `max_size`              | int   | No       | 10      | Maximum number of connections                  |
| `max_idle_time`         | float | No       | 300.0   | Maximum idle time (seconds)                    |
| `max_lifetime`          | float | No       | 3600.0  | Maximum connection lifetime (seconds)          |
| `wait_timeout`          | float | No       | 10.0    | Connection acquisition timeout (seconds)       |
| `health_check_interval` | float | No       | 30.0    | Health check interval (seconds)                |
| `enable_dynamic_resize` | bool  | No       | True    | Enable dynamic resizing                        |

### PostgreSQL Configuration

```python
config = {
    "type": "postgresql",
    "enabled": True,
    "settings": {
        "host": "localhost",
        "port": 5432,
        "database": "mydb",
        "username": "user",
        "password": "pass",
        "ssl_mode": "prefer"  # Optional: disable/allow/prefer/require/verify-ca/verify-full
    },
    "pool": {
        "min_size": 2,
        "max_size": 10,
        "max_lifetime": 3600,
        "wait_timeout": 30
    }
}
```

### Neo4j Configuration

```python
config = {
    "type": "neo4j",
    "enabled": True,
    "settings": {
        "uri": "bolt://localhost:7687",
        "username": "neo4j",
        "password": "password"
    },
    "pool": {
        "min_size": 2,
        "max_size": 10
    }
}
```

### HTTPX Configuration

```python
config = {
    "type": "httpx",
    "enabled": True,
    "settings": {
        "base_url": "https://api.example.com",
        "timeout": 30.0,
        "http2": True
    },
    "pool": {
        "min_size": 5,
        "max_size": 20
    }
}
```

### Boto3 Configuration

```python
config = {
    "type": "boto3",
    "enabled": True,
    "settings": {
        "service_name": "s3",
        "region_name": "us-east-1",
        "endpoint_url": "https://s3.amazonaws.com"  # Optional
    },
    "pool": {
        "min_size": 2,
        "max_size": 10
    }
}
```

## Exception Handling

### Exception Hierarchy

```
Exception
├── ConnectionError
│   ├── ConnectionTimeoutError
│   ├── ConnectionFailedError
│   ├── AuthenticationError
│   ├── ConnectionNotFoundError
│   └── ConnectionClosedError
├── PoolError
│   ├── PoolExhaustedError
│   ├── PoolNotReadyError
│   ├── PoolNotFoundError
│   └── PoolAlreadyExistsError
├── PoolManagerError
│   ├── PluginNotFoundError
│   └── PluginAlreadyExistsError
├── ConfigurationError
│   ├── ConfigValidationError
│   └── ConfigNotFoundError
└── HealthCheckError
```

### Usage Example

```python
from silvaengine_connections import (
    ConnectionPoolManager,
    PoolNotFoundError,
    PoolExhaustedError,
    ConnectionError
)

manager = ConnectionPoolManager()

try:
    with manager.connection("postgres_main") as conn:
        result = conn.execute("SELECT * FROM users")
except PoolNotFoundError as e:
    print(f"Pool not found: {e.details['pool_name']}")
except PoolExhaustedError as e:
    print(f"Pool exhausted: {e}")
except ConnectionError as e:
    print(f"Connection error: {e.error_code} - {e}")
```

## PluginManager Integration

For integration with silvaengine_base's PluginManager:

```python
# In your plugin configuration
{
    "type": "connection_pool",
    "module_name": "silvaengine_connections",
    "function_name": "init",
    "enabled": True,
    "config": {
        "postgresql": {
            "type": "postgresql",
            "enabled": True,
            "settings": {
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "username": "user",
                "password": "pass"
            },
            "pool": {
                "min_size": 2,
                "max_size": 10
            }
        }
    }
}
```

The `init()` function will return a `ConnectionPoolManager` instance that can be accessed through the plugin context.

## Lifecycle Management

### Connection Lifecycle Events

```python
from silvaengine_connections import ConnectionLifecycleManager

lifecycle = ConnectionLifecycleManager()

# Add event handlers
def on_connection_created(event):
    print(f"Connection created: {event.connection_name}")

def on_connection_error(event):
    print(f"Connection error: {event.error}")

lifecycle.add_event_handler("created", on_connection_created)
lifecycle.add_event_handler("error", on_connection_error)

# Create and manage connections
context = lifecycle.create_connection_context("postgres_main", config)
lifecycle.initialize_connection("postgres_main")

# Use connection
conn = lifecycle.acquire_connection("postgres_main")
# ... use connection ...
lifecycle.release_connection("postgres_main")

# Close connection
lifecycle.close_connection("postgres_main")
```

## Metrics and Monitoring

### PoolMetrics

```python
from silvaengine_connections import PoolMetrics

# Get metrics
metrics = pool.metrics

print(f"Total created: {metrics.total_created}")
print(f"Total destroyed: {metrics.total_destroyed}")
print(f"Active connections: {metrics.active_connections}")
print(f"Idle connections: {metrics.idle_connections}")
print(f"Wait time avg: {metrics.wait_time_avg}")
print(f"Wait time max: {metrics.wait_time_max}")
print(f"Health check failures: {metrics.health_check_failures}")
```

### Health Check

```python
# Check pool health
status = pool.health_check()
print(f"Status: {status['status']}")
print(f"Active: {status['active_connections']}")
print(f"Idle: {status['idle_connections']}")
print(f"Unhealthy: {status['unhealthy_connections']}")
```

## Testing

Run the test suite:

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=silvaengine_connections

# Run specific test file
pytest tests/test_connection_pool.py
```

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests.

## Support

For issues and feature requests, please use the GitHub issue tracker.
