# silvaengine_connections 热插拔式连接池管理功能设计方案

## 一、项目说明

### 1.1 架构概述

### 1.2 设计目标

实现企业级热插拔式连接池管理，支持：

1. **PostgreSQL + SQLAlchemy** 连接池
2. **Neo4j** 连接池
3. **HTTPX** 异步 HTTP 客户端连接池
4. **Boto3** AWS 服务连接池
5. **热插拔机制**：动态注册、加载、卸载连接类型
6. **上下文管理器**：支持 `with` 语句安全借用归还

---

## 二、系统架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ConnectionPoolManager                            │
│                    (单例模式，热插拔核心控制器)                              │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │  PluginRegistry  │  │  ConfigManager   │  │   HealthMonitor         │  │
│  │  (插件注册中心)   │  │  (配置管理器)     │  │   (健康检查监控器)        │  │
│  └────────┬────────┘  └────────┬────────┘  └────────────┬────────────┘  │
│           │                    │                        │               │
│           └────────────────────┼────────────────────────┘               │
│                                │                                        │
│                                ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    ConnectionPool (Abstract)                     │   │
│  │              (连接池抽象基类，定义统一接口)                        │   │
│  └────────┬────────┬────────┬────────┬─────────────────────────────┘   │
│           │        │        │        │                                  │
│           ▼        ▼        ▼        ▼                                  │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                   │
│  │PostgreSQL│ │  Neo4j   │ │  HTTPX   │ │  Boto3   │                   │
│  │   Pool   │ │   Pool   │ │   Pool   │ │   Pool   │                   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘                   │
│       │            │            │            │                          │
│       ▼            ▼            ▼            ▼                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐                   │
│  │SQLAlchemy│ │Neo4j     │ │  HTTPX   │ │  Boto3   │                   │
│  │  Engine  │ │  Driver  │ │  Client  │ │  Client  │                   │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘                   │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ 外部调用
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         使用示例 (with 语句)                              │
│                                                                         │
│   with pool_manager.get_pool("postgres_main").connection() as conn:     │
│       result = conn.execute("SELECT * FROM users")                      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 模块交互关系图

```
                    外部调用
                       │
                       ▼
            ┌─────────────────────┐
            │ ConnectionPoolManager│
            │    (入口 Facade)      │
            └──────────┬──────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
        ▼              ▼              ▼
┌──────────────┐ ┌──────────┐ ┌──────────────┐
│PluginRegistry│ │ConfigMgr │ │HealthMonitor │
└──────┬───────┘ └────┬─────┘ └──────┬───────┘
       │              │              │
       │    ┌─────────┴─────────┐    │
       │    │                   │    │
       ▼    ▼                   ▼    ▼
┌─────────────────────────────────────────────┐
│           ConnectionPool (Base)             │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐       │
│  │ acquire │ │ release │ │  health │       │
│  └─────────┘ └─────────┘ └─────────┘       │
└─────────────────────────────────────────────┘
       │              │              │
       ▼              ▼              ▼
┌──────────┐   ┌──────────┐   ┌──────────┐
│PostgreSQL│   │  Neo4j   │   │  HTTPX   │
│   Pool   │   │   Pool   │   │   Pool   │
└──────────┘   └──────────┘   └──────────┘
```

---

## 三、模块划分

### 3.1 核心模块职责

| 模块             | 文件路径             | 职责                 | 关键类                                |
| ---------------- | -------------------- | -------------------- | ------------------------------------- |
| **连接抽象**     | `connection.py`      | 定义连接接口规范     | `BaseConnection`                      |
| **连接池抽象**   | `connection_pool.py` | 定义连接池接口规范   | `BaseConnectionPool`                  |
| **连接池管理器** | `pool_manager.py`    | 热插拔核心，单例管理 | `ConnectionPoolManager`               |
| **插件注册表**   | `plugin_registry.py` | 动态注册连接类型     | `PluginRegistry`                      |
| **配置管理**     | `config.py`          | 解析验证配置         | `ConnectionConfig`, `ConfigValidator` |
| **健康监控**     | `health_monitor.py`  | 连接健康检查         | `HealthMonitor`, `HealthChecker`      |
| **统计监控**     | `metrics.py`         | 连接池指标收集       | `PoolMetrics`, `MetricsCollector`     |
| **异常定义**     | `exceptions.py`      | 统一异常体系         | `ConnectionError`, `PoolError`        |

### 3.2 连接适配器模块

| 模块           | 文件路径                    | 支持类型   | 依赖库               |
| -------------- | --------------------------- | ---------- | -------------------- |
| **PostgreSQL** | `connections/postgresql.py` | postgresql | sqlalchemy, psycopg2 |
| **Neo4j**      | `connections/neo4j.py`      | neo4j      | neo4j-driver         |
| **HTTPX**      | `connections/httpx.py`      | httpx      | httpx[http2]         |
| **Boto3**      | `connections/boto3.py`      | boto3      | boto3                |

---

## 四、接口定义

### 4.1 基础连接接口

```python
# connection.py

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, TypeVar, Generic
from types import TracebackType

T = TypeVar('T')

class BaseConnection(ABC, Generic[T]):
    """
    连接抽象基类，所有具体连接类型必须继承此类。

    Type Parameters:
        T: 底层连接对象的类型（如 sqlalchemy.Engine, neo4j.Driver）
    """

    def __init__(self, conn_id: int, config: Dict[str, Any]) -> None:
        """
        初始化连接。

        Args:
            conn_id: 连接唯一标识符
            config: 连接配置字典
        """
        self._conn_id = conn_id
        self._config = config
        self._is_used = False
        self._created_at = time.time()
        self._last_used_at = time.time()
        self._raw_connection: Optional[T] = None

    @property
    def conn_id(self) -> int:
        """获取连接ID。"""
        return self._conn_id

    @property
    def is_used(self) -> bool:
        """检查连接是否正在被使用。"""
        return self._is_used

    @is_used.setter
    def is_used(self, value: bool) -> None:
        """设置连接使用状态。"""
        self._is_used = value
        if value:
            self._last_used_at = time.time()

    @property
    def raw_connection(self) -> Optional[T]:
        """获取底层原始连接对象。"""
        return self._raw_connection

    @abstractmethod
    def connect(self) -> T:
        """
        建立底层连接。

        Returns:
            T: 底层连接对象

        Raises:
            ConnectionError: 连接失败时抛出
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        关闭底层连接，释放资源。
        """
        pass

    @abstractmethod
    def is_healthy(self) -> bool:
        """
        检查连接健康状态。

        Returns:
            bool: 连接健康返回 True
        """
        pass

    def reset(self) -> None:
        """
        重置连接状态，准备归还到连接池。
        子类可覆盖此方法执行自定义重置逻辑。
        """
        self._is_used = False

    def get_lifetime(self) -> float:
        """
        获取连接存活时间。

        Returns:
            float: 存活时间（秒）
        """
        return time.time() - self._created_at

    def get_idle_time(self) -> float:
        """
        获取连接空闲时间。

        Returns:
            float: 空闲时间（秒）
        """
        return time.time() - self._last_used_at

    def __enter__(self) -> 'BaseConnection[T]':
        """上下文管理器入口。"""
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType]
    ) -> None:
        """上下文管理器出口，自动关闭连接。"""
        self.close()
```

### 4.2 连接池接口

````python
# connection_pool.py

from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Any, Dict, Generic, Iterator, Optional, Type, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import threading
import queue
import time

from .connection import BaseConnection
from .exceptions import PoolError, PoolExhaustedError

T = TypeVar('T')
C = TypeVar('C', bound=BaseConnection)

class PoolStatus(Enum):
    """连接池状态枚举。"""
    INITIALIZING = "initializing"
    READY = "ready"
    PAUSED = "paused"
    SHUTDOWN = "shutdown"

@dataclass
class PoolMetrics:
    """连接池指标数据类。"""
    total_created: int = 0
    total_destroyed: int = 0
    total_borrowed: int = 0
    total_returned: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    wait_time_avg: float = 0.0
    wait_time_max: float = 0.0
    health_check_failures: int = 0
    last_health_check: Optional[float] = None

class BaseConnectionPool(ABC, Generic[C]):
    """
    连接池抽象基类，定义连接池通用接口。

    Type Parameters:
        C: 连接类型，必须是 BaseConnection 的子类
    """

    def __init__(
        self,
        name: str,
        connection_class: Type[C],
        min_size: int = 2,
        max_size: int = 10,
        max_idle_time: float = 300.0,
        max_lifetime: float = 3600.0,
        wait_timeout: float = 10.0,
        health_check_interval: float = 30.0,
        enable_dynamic_resize: bool = True,
    ) -> None:
        """
        初始化连接池。

        Args:
            name: 连接池名称
            connection_class: 连接类类型
            min_size: 最小连接数
            max_size: 最大连接数
            max_idle_time: 最大空闲时间（秒）
            max_lifetime: 最大存活时间（秒）
            wait_timeout: 获取连接超时时间（秒）
            health_check_interval: 健康检查间隔（秒）
            enable_dynamic_resize: 是否启用动态调整
        """
        self._name = name
        self._connection_class = connection_class
        self._min_size = min_size
        self._max_size = max_size
        self._max_idle_time = max_idle_time
        self._max_lifetime = max_lifetime
        self._wait_timeout = wait_timeout
        self._health_check_interval = health_check_interval
        self._enable_dynamic_resize = enable_dynamic_resize

        self._status = PoolStatus.INITIALIZING
        self._metrics = PoolMetrics()
        self._lock = threading.RLock()
        self._conn_counter = 0

        # 连接存储
        self._idle_connections: queue.Queue[C] = queue.Queue(maxsize=max_size)
        self._active_connections: Dict[int, C] = {}

        self._initialize_pool()

    @property
    def name(self) -> str:
        """获取连接池名称。"""
        return self._name

    @property
    def status(self) -> PoolStatus:
        """获取连接池状态。"""
        return self._status

    @property
    def metrics(self) -> PoolMetrics:
        """获取连接池指标（返回副本）。"""
        with self._lock:
            return PoolMetrics(**self._metrics.__dict__)

    @abstractmethod
    def get_pool_type(self) -> str:
        """
        获取连接池类型标识。

        Returns:
            str: 类型标识（如 'postgresql', 'neo4j', 'httpx', 'boto3'）
        """
        pass

    @abstractmethod
    def _create_connection(self) -> C:
        """
        创建新连接实例。

        Returns:
            C: 新创建的连接对象
        """
        pass

    def _initialize_pool(self) -> None:
        """初始化连接池，创建最小连接数。"""
        with self._lock:
            for _ in range(self._min_size):
                conn = self._create_connection()
                self._idle_connections.put(conn)
                self._metrics.total_created += 1

            self._metrics.idle_connections = self._min_size
            self._status = PoolStatus.READY

    def acquire(self) -> C:
        """
        从连接池获取连接。

        Returns:
            C: 连接对象

        Raises:
            PoolExhaustedError: 连接池耗尽且无法创建新连接
            PoolError: 连接池未就绪
        """
        if self._status != PoolStatus.READY:
            raise PoolError(f"Pool {self._name} is not ready (status: {self._status.value})")

        start_time = time.time()

        with self._lock:
            # 尝试从空闲队列获取
            try:
                conn = self._idle_connections.get(block=False)
                if self._is_connection_valid(conn):
                    conn.is_used = True
                    self._active_connections[conn.conn_id] = conn
                    self._metrics.total_borrowed += 1
                    self._metrics.active_connections += 1
                    self._metrics.idle_connections -= 1
                    return conn
                else:
                    # 连接无效，销毁并创建新连接
                    self._destroy_connection(conn)
            except queue.Empty:
                pass

            # 检查是否可创建新连接
            total_conns = len(self._active_connections) + self._idle_connections.qsize()
            if total_conns < self._max_size:
                conn = self._create_connection()
                conn.is_used = True
                self._active_connections[conn.conn_id] = conn
                self._metrics.total_created += 1
                self._metrics.total_borrowed += 1
                self._metrics.active_connections += 1
                return conn

        # 等待可用连接
        try:
            conn = self._idle_connections.get(timeout=self._wait_timeout)
            wait_time = time.time() - start_time
            self._update_wait_time_metrics(wait_time)

            if self._is_connection_valid(conn):
                with self._lock:
                    conn.is_used = True
                    self._active_connections[conn.conn_id] = conn
                    self._metrics.total_borrowed += 1
                    self._metrics.active_connections += 1
                    self._metrics.idle_connections -= 1
                return conn
            else:
                self._destroy_connection(conn)
                return self.acquire()  # 递归重试

        except queue.Empty:
            raise PoolExhaustedError(
                f"Pool {self._name} exhausted, unable to acquire connection within {self._wait_timeout}s"
            )

    def release(self, conn: C) -> None:
        """
        归还连接到连接池。

        Args:
            conn: 要归还的连接对象
        """
        if conn is None:
            return

        with self._lock:
            if conn.conn_id in self._active_connections:
                del self._active_connections[conn.conn_id]
                self._metrics.active_connections -= 1

            if self._status == PoolStatus.SHUTDOWN:
                self._destroy_connection(conn)
                return

            if self._is_connection_valid(conn):
                conn.reset()
                try:
                    self._idle_connections.put(conn, block=False)
                    self._metrics.total_returned += 1
                    self._metrics.idle_connections += 1
                except queue.Full:
                    self._destroy_connection(conn)
            else:
                self._destroy_connection(conn)

    @contextmanager
    def connection(self) -> Iterator[C]:
        """
        上下文管理器，自动获取和归还连接。

        Example:
            ```python
            with pool.connection() as conn:
                result = conn.execute("SELECT 1")
            ```
        """
        conn = None
        try:
            conn = self.acquire()
            yield conn
        finally:
            if conn:
                self.release(conn)

    def _is_connection_valid(self, conn: C) -> bool:
        """
        检查连接是否有效。

        Args:
            conn: 连接对象

        Returns:
            bool: 连接有效返回 True
        """
        # 检查存活时间
        if conn.get_lifetime() > self._max_lifetime:
            return False

        # 检查空闲时间
        if not conn.is_used and conn.get_idle_time() > self._max_idle_time:
            return False

        # 执行健康检查
        return conn.is_healthy()

    def _destroy_connection(self, conn: C) -> None:
        """销毁连接。"""
        try:
            conn.close()
        except Exception:
            pass
        finally:
            with self._lock:
                self._metrics.total_destroyed += 1

    def close(self) -> None:
        """
        关闭连接池，释放所有资源。
        """
        with self._lock:
            self._status = PoolStatus.SHUTDOWN

            # 关闭活跃连接
            for conn in list(self._active_connections.values()):
                self._destroy_connection(conn)
            self._active_connections.clear()

            # 关闭空闲连接
            while not self._idle_connections.empty():
                try:
                    conn = self._idle_connections.get(block=False)
                    self._destroy_connection(conn)
                except queue.Empty:
                    break

            self._metrics.active_connections = 0
            self._metrics.idle_connections = 0

    def _update_wait_time_metrics(self, wait_time: float) -> None:
        """更新等待时间指标。"""
        self._metrics.wait_time_max = max(self._metrics.wait_time_max, wait_time)
        # 使用移动平均
        alpha = 0.1
        self._metrics.wait_time_avg = (
            alpha * wait_time + (1 - alpha) * self._metrics.wait_time_avg
        )
````

### 4.3 连接池管理器接口

```python
# pool_manager.py

from typing import Any, Callable, Dict, List, Optional, Type, TypeVar
import threading
import logging

from .connection_pool import BaseConnectionPool
from .plugin_registry import PluginRegistry
from .config import ConnectionConfig
from .exceptions import PoolManagerError, PoolNotFoundError

T = TypeVar('T', bound=BaseConnectionPool)

class ConnectionPoolManager:
    """
    连接池管理器，实现热插拔功能的核心类。

    采用单例模式确保全局唯一实例，提供：
    - 连接池生命周期管理
    - 插件动态注册/卸载
    - 配置驱动初始化
    - 健康监控
    - 统计聚合
    """

    _instance: Optional['ConnectionPoolManager'] = None
    _lock: threading.Lock = threading.Lock()

    def __new__(cls, *args, **kwargs) -> 'ConnectionPoolManager':
        """单例模式实现。"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        """初始化管理器（仅执行一次）。"""
        if self._initialized:
            return

        self._logger = logger or logging.getLogger(__name__)
        self._pools: Dict[str, BaseConnectionPool] = {}
        self._pool_locks: Dict[str, threading.RLock] = {}
        self._manager_lock = threading.RLock()
        self._plugin_registry = PluginRegistry()

        self._initialized = True
        self._logger.info("ConnectionPoolManager initialized")

    @classmethod
    def get_instance(cls) -> 'ConnectionPoolManager':
        """获取管理器实例。"""
        return cls()

    def register_connection_type(
        self,
        type_name: str,
        pool_class: Type[BaseConnectionPool],
        connection_class: Type[BaseConnection],
    ) -> None:
        """
        注册新的连接类型（热插拔入口）。

        Args:
            type_name: 连接类型名称（如 'postgresql', 'neo4j'）
            pool_class: 连接池类
            connection_class: 连接类
        """
        self._plugin_registry.register(
            type_name=type_name,
            pool_class=pool_class,
            connection_class=connection_class,
        )
        self._logger.info(f"Registered connection type: {type_name}")

    def unregister_connection_type(self, type_name: str) -> bool:
        """
        注销连接类型。

        Args:
            type_name: 连接类型名称

        Returns:
            bool: 成功返回 True
        """
        # 先关闭该类型的所有连接池
        pools_to_remove = [
            name for name, pool in self._pools.items()
            if pool.get_pool_type() == type_name
        ]
        for name in pools_to_remove:
            self.remove_pool(name)

        result = self._plugin_registry.unregister(type_name)
        if result:
            self._logger.info(f"Unregistered connection type: {type_name}")
        return result

    def create_pool(
        self,
        name: str,
        config: ConnectionConfig,
    ) -> BaseConnectionPool:
        """
        根据配置创建连接池。

        Args:
            name: 连接池名称
            config: 连接配置

        Returns:
            BaseConnectionPool: 创建的连接池
        """
        with self._manager_lock:
            if name in self._pools:
                raise PoolManagerError(f"Pool '{name}' already exists")

            # 获取注册的类
            plugin = self._plugin_registry.get(config.type)
            if not plugin:
                raise PoolManagerError(f"Unknown connection type: {config.type}")

            # 创建连接池
            pool = plugin.pool_class(
                name=name,
                connection_class=plugin.connection_class,
                **config.pool_params,
            )

            self._pools[name] = pool
            self._pool_locks[name] = threading.RLock()

            self._logger.info(f"Created pool: {name} (type: {config.type})")
            return pool

    def remove_pool(self, name: str) -> bool:
        """
        移除连接池。

        Args:
            name: 连接池名称

        Returns:
            bool: 成功返回 True
        """
        with self._manager_lock:
            if name not in self._pools:
                return False

            pool = self._pools[name]
            pool.close()

            del self._pools[name]
            del self._pool_locks[name]

            self._logger.info(f"Removed pool: {name}")
            return True

    def get_pool(self, name: str) -> Optional[BaseConnectionPool]:
        """
        获取连接池。

        Args:
            name: 连接池名称

        Returns:
            Optional[BaseConnectionPool]: 连接池或 None
        """
        return self._pools.get(name)

    def get_pool_safe(self, name: str) -> BaseConnectionPool:
        """
        安全获取连接池，不存在时抛出异常。

        Args:
            name: 连接池名称

        Returns:
            BaseConnectionPool: 连接池

        Raises:
            PoolNotFoundError: 连接池不存在
        """
        pool = self.get_pool(name)
        if pool is None:
            raise PoolNotFoundError(f"Pool '{name}' not found")
        return pool

    def initialize_from_config(self, config_dict: Dict[str, Any]) -> List[str]:
        """
        从配置字典批量初始化连接池。

        Args:
            config_dict: 配置字典，格式：
                {
                    "pools": {
                        "pool_name": {
                            "type": "postgresql",
                            "enabled": true,
                            "params": {...}
                        }
                    }
                }

        Returns:
            List[str]: 成功创建的连接池名称列表
        """
        created = []
        pools_config = config_dict.get('pools', {})

        for name, pool_config in pools_config.items():
            if not pool_config.get('enabled', True):
                self._logger.debug(f"Skipping disabled pool: {name}")
                continue

            try:
                config = ConnectionConfig.from_dict(pool_config)
                self.create_pool(name, config)
                created.append(name)
            except Exception as e:
                self._logger.error(f"Failed to create pool '{name}': {e}")

        return created

    def get_all_pools(self) -> Dict[str, BaseConnectionPool]:
        """获取所有连接池。"""
        return self._pools.copy()

    def get_pool_names(self) -> List[str]:
        """获取所有连接池名称。"""
        return list(self._pools.keys())

    def get_connection_types(self) -> List[str]:
        """获取所有注册的连接类型。"""
        return self._plugin_registry.get_all_types()

    def shutdown_all(self) -> None:
        """关闭所有连接池。"""
        with self._manager_lock:
            for name, pool in list(self._pools.items()):
                try:
                    pool.close()
                    self._logger.info(f"Shutdown pool: {name}")
                except Exception as e:
                    self._logger.error(f"Error shutting down pool '{name}': {e}")

            self._pools.clear()
            self._pool_locks.clear()

    def get_all_metrics(self) -> Dict[str, Any]:
        """获取所有连接池的指标。"""
        return {
            name: pool.metrics for name, pool in self._pools.items()
        }
```

---

## 五、配置参数说明

### 5.1 通用配置参数

| 参数名                  | 类型  | 必填 | 默认值 | 说明                                     |
| ----------------------- | ----- | ---- | ------ | ---------------------------------------- |
| `type`                  | str   | 是   | -      | 连接类型（postgresql/neo4j/httpx/boto3） |
| `enabled`               | bool  | 否   | True   | 是否启用该连接池                         |
| `min_size`              | int   | 否   | 2      | 最小连接数                               |
| `max_size`              | int   | 否   | 10     | 最大连接数                               |
| `max_idle_time`         | float | 否   | 300.0  | 最大空闲时间（秒）                       |
| `max_lifetime`          | float | 否   | 3600.0 | 最大存活时间（秒）                       |
| `wait_timeout`          | float | 否   | 10.0   | 获取连接超时（秒）                       |
| `health_check_interval` | float | 否   | 30.0   | 健康检查间隔（秒）                       |
| `enable_dynamic_resize` | bool  | 否   | True   | 启用动态调整                             |

### 5.2 PostgreSQL 特有配置

| 参数名          | 类型 | 必填 | 默认值 | 说明                 |
| --------------- | ---- | ---- | ------ | -------------------- |
| `url`           | str  | 是   | -      | 连接URL              |
| `pool_size`     | int  | 否   | 5      | SQLAlchemy pool_size |
| `max_overflow`  | int  | 否   | 10     | 最大溢出连接         |
| `pool_pre_ping` | bool | 否   | True   | 连接前ping检测       |
| `echo`          | bool | 否   | False  | 打印SQL语句          |

### 5.3 Neo4j 特有配置

| 参数名                     | 类型 | 必填 | 默认值 | 说明           |
| -------------------------- | ---- | ---- | ------ | -------------- |
| `uri`                      | str  | 是   | -      | Neo4j URI      |
| `user`                     | str  | 是   | -      | 用户名         |
| `password`                 | str  | 是   | -      | 密码           |
| `max_connection_pool_size` | int  | 否   | 50     | 最大连接池大小 |

### 5.4 HTTPX 特有配置

| 参数名                      | 类型 | 必填 | 默认值 | 说明           |
| --------------------------- | ---- | ---- | ------ | -------------- |
| `max_keepalive_connections` | int  | 否   | 20     | 最大保持连接数 |
| `max_connections`           | int  | 否   | 100    | 最大连接数     |
| `http2`                     | bool | 否   | True   | 启用HTTP/2     |

### 5.5 Boto3 特有配置

| 参数名                 | 类型 | 必填 | 默认值    | 说明                       |
| ---------------------- | ---- | ---- | --------- | -------------------------- |
| `service_name`         | str  | 是   | -         | AWS服务名（s3/dynamodb等） |
| `region_name`          | str  | 否   | us-east-1 | AWS区域                    |
| `max_pool_connections` | int  | 否   | 10        | 最大连接数                 |

---

## 六、热插拔实现机制

### 6.1 插件注册表实现

```python
# plugin_registry.py

from dataclasses import dataclass
from typing import Any, Dict, Optional, Type
import threading

from .connection import BaseConnection
from .connection_pool import BaseConnectionPool

@dataclass
class ConnectionPlugin:
    """连接插件数据类。"""
    type_name: str
    pool_class: Type[BaseConnectionPool]
    connection_class: Type[BaseConnection]

class PluginRegistry:
    """插件注册表，管理连接类型的动态注册。"""

    def __init__(self):
        self._plugins: Dict[str, ConnectionPlugin] = {}
        self._lock = threading.RLock()

    def register(
        self,
        type_name: str,
        pool_class: Type[BaseConnectionPool],
        connection_class: Type[BaseConnection],
    ) -> None:
        """注册连接类型。"""
        with self._lock:
            self._plugins[type_name] = ConnectionPlugin(
                type_name=type_name,
                pool_class=pool_class,
                connection_class=connection_class,
            )

    def unregister(self, type_name: str) -> bool:
        """注销连接类型。"""
        with self._lock:
            if type_name in self._plugins:
                del self._plugins[type_name]
                return True
            return False

    def get(self, type_name: str) -> Optional[ConnectionPlugin]:
        """获取插件。"""
        return self._plugins.get(type_name)

    def get_all_types(self) -> List[str]:
        """获取所有类型。"""
        return list(self._plugins.keys())
```

### 6.2 热插拔流程

```
1. 注册新连接类型
   ┌─────────────┐
   │ 调用 register_connection_type()
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ PluginRegistry.register()
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ 存储 type_name -> (pool_class, connection_class)
   └─────────────┘

2. 创建连接池
   ┌─────────────┐
   │ 调用 create_pool()
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ 从 PluginRegistry 获取 pool_class
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ 实例化连接池
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ 加入 _pools 字典
   └─────────────┘

3. 动态卸载
   ┌─────────────┐
   │ 调用 unregister_connection_type()
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ 关闭该类型所有连接池
   └──────┬──────┘
          ▼
   ┌─────────────┐
   │ 从 PluginRegistry 移除
   └─────────────┘
```

---

## 七、错误处理方案

### 7.1 异常体系

```python
# exceptions.py

class ConnectionError(Exception):
    """连接基础异常。"""
    def __init__(self, message: str, error_code: str = None):
        super().__init__(message)
        self.error_code = error_code

class ConnectionTimeoutError(ConnectionError):
    """连接超时异常。"""
    ERROR_CODE = "CONN_TIMEOUT"

class ConnectionFailedError(ConnectionError):
    """连接失败异常。"""
    ERROR_CODE = "CONN_FAILED"

class AuthenticationError(ConnectionError):
    """认证失败异常。"""
    ERROR_CODE = "AUTH_FAILED"

class PoolError(Exception):
    """连接池基础异常。"""
    pass

class PoolExhaustedError(PoolError):
    """连接池耗尽异常。"""
    pass

class PoolManagerError(Exception):
    """管理器异常。"""
    pass

class PoolNotFoundError(PoolManagerError):
    """连接池不存在异常。"""
    pass
```

### 7.2 错误处理流程

```
获取连接流程:
┌─────────────┐
│ 调用 acquire()
└──────┬──────┘
       ▼
┌─────────────┐     ┌─────────────────┐
│ 检查池状态   │────▶│ PoolError(未就绪)│
└──────┬──────┘     └─────────────────┘
       │ 正常
       ▼
┌─────────────┐     ┌─────────────────┐
│ 尝试获取连接 │────▶│ PoolExhaustedError
└──────┬──────┘     └─────────────────┘
       │ 成功
       ▼
┌─────────────┐
│ 健康检查     │
└──────┬──────┘
       │ 不健康
       ▼
┌─────────────┐
│ 销毁并重建   │
└──────┬──────┘
       ▼
┌─────────────┐
│ 返回连接     │
└─────────────┘
```

---

## 八、性能优化措施

### 8.1 动态调整策略

```python
def _dynamic_resize(self) -> None:
    """
    基于负载动态调整连接池大小。

    策略：
    - 当活跃连接数 > max_size * 0.8 且未达到 max_size 时，增加连接
    - 当空闲连接数 > min_size * 2 且持续 60 秒时，减少连接
    """
    with self._lock:
        utilization = self._metrics.active_connections / self._max_size

        # 扩容
        if utilization > 0.8 and self._metrics.total_created < self._max_size:
            needed = min(2, self._max_size - self._metrics.total_created)
            for _ in range(needed):
                conn = self._create_connection()
                self._idle_connections.put(conn)
                self._metrics.total_created += 1

        # 缩容
        elif (self._metrics.idle_connections > self._min_size * 2 and
              self._should_shrink()):
            to_remove = min(2, self._metrics.idle_connections - self._min_size)
            for _ in range(to_remove):
                try:
                    conn = self._idle_connections.get(block=False)
                    self._destroy_connection(conn)
                except queue.Empty:
                    break
```

### 8.2 资源占用控制

- **连接数限制**：严格限制 max_size，防止资源耗尽
- **空闲超时**：自动回收长时间空闲连接
- **存活限制**：强制回收超过 max_lifetime 的连接
- **内存监控**：定期统计并报告连接池内存占用

---

## 九、测试计划

### 9.1 单元测试

| 测试类                    | 测试方法             | 验证内容     |
| ------------------------- | -------------------- | ------------ |
| TestBaseConnection        | test_connect         | 连接创建     |
|                           | test_close           | 连接关闭     |
|                           | test_context_manager | 上下文管理器 |
| TestBaseConnectionPool    | test_acquire_release | 获取归还连接 |
|                           | test_pool_exhausted  | 连接池耗尽   |
|                           | test_health_check    | 健康检查     |
| TestConnectionPoolManager | test_singleton       | 单例模式     |
|                           | test_register_plugin | 插件注册     |
|                           | test_hot_reload      | 热重载       |

### 9.2 集成测试

| 测试场景        | 测试内容             |
| --------------- | -------------------- |
| PostgreSQL 连接 | 真实数据库连接测试   |
| Neo4j 连接      | 真实图数据库连接测试 |
| HTTPX 连接      | HTTP 请求测试        |
| Boto3 连接      | AWS Mock 测试        |
| 配置初始化      | 从配置创建连接池     |

### 9.3 性能测试

| 指标         | 目标值          |
| ------------ | --------------- |
| 连接获取延迟 | P99 < 10ms      |
| 并发连接数   | 支持 1000+ 并发 |
| 连接创建时间 | < 50ms          |
| 内存占用     | 每连接 < 1MB    |

---

## 十、实施步骤

### Phase 1: 基础框架

1. 重构 `connection.py` - 完善 BaseConnection
2. 重构 `connection_pool.py` - 完善 BaseConnectionPool
3. 创建 `exceptions.py` - 异常体系
4. 创建 `plugin_registry.py` - 插件注册表

### Phase 2: 连接池管理器

1. 创建 `pool_manager.py` - 管理器核心
2. 重构 `config.py` - 配置解析
3. 创建 `metrics.py` - 指标收集

### Phase 3: 连接适配器

1. 重构 `connections/postgresql.py`
2. 重构 `connections/neo4j.py`
3. 创建 `connections/httpx.py`
4. 创建 `connections/boto3.py`

### Phase 4: 测试

1. 编写单元测试
2. 编写集成测试
3. 性能测试

---

## 十一、使用示例

```python
# 初始化管理器
from silvaengine_connections import ConnectionPoolManager

manager = ConnectionPoolManager()

# 从配置初始化
config = {
    "pools": {
        "postgres_main": {
            "type": "postgresql",
            "enabled": True,
            "params": {
                "url": "postgresql://user:pass@localhost/db",
                "min_size": 2,
                "max_size": 10,
            }
        },
        "neo4j_graph": {
            "type": "neo4j",
            "enabled": True,
            "params": {
                "uri": "bolt://localhost:7687",
                "user": "neo4j",
                "password": "password",
            }
        }
    }
}

manager.initialize_from_config(config)

# 使用连接池
with manager.get_pool("postgres_main").connection() as conn:
    result = conn.execute("SELECT * FROM users")

# 热重载配置
manager.get_pool("postgres_main").update_config(new_config)

# 动态注册新类型
from silvaengine_connections.connections.custom import CustomPool, CustomConnection
manager.register_connection_type("custom", CustomPool, CustomConnection)
```
