"""
Boto3 Connection Pool Implementation

AWS service connection pool implementation based on Boto3, supporting hot-plugging.
Supported services include: S3, DynamoDB, SQS, SNS, Lambda, EC2, etc.
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, TypeVar

import boto3
from botocore.client import BaseClient
from botocore.config import Config as BotoConfig
from botocore.exceptions import (
    ClientError,
    ConnectTimeoutError,
    EndpointConnectionError,
    NoCredentialsError,
    PartialCredentialsError,
    ReadTimeoutError,
)

from ..config import ConnectionConfig
from ..connection import BaseConnection
from ..connection_pool import BaseConnectionPool
from ..exceptions import (
    AuthenticationError,
    ConfigValidationError,
    ConnectionError,
    ConnectionTimeoutError,
    HealthCheckError,
    PoolError,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseClient)


class Boto3Connection(BaseConnection[BaseClient]):
    """
    Boto3 Connection Wrapper Class

    Uses Boto3 Client as the underlying connection object, providing AWS service connection management and health check functionality.

    Attributes:
        _client: Boto3 client instance
        _service_name: AWS service name
        _region_name: AWS region
        _client_config: Boto3 client configuration
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize Boto3 Connection

        Args:
            config: Connection configuration dictionary, containing the following key fields:
                - service_name: AWS service name (required, e.g., 's3', 'dynamodb', 'sqs', etc.)
                - region_name: AWS region (default: us-east-1)
                - aws_access_key_id: AWS access key ID
                - aws_secret_access_key: AWS secret access key
                - aws_session_token: AWS session token (optional)
                - endpoint_url: Custom endpoint URL (optional, for local testing)
                - use_ssl: Whether to use SSL (default: True)
                - verify: SSL certificate verification (default: True)
                - max_pool_connections: Maximum connection pool size (default: 10)
                - connect_timeout: Connection timeout in seconds (default: 5)
                - read_timeout: Read timeout in seconds (default: 60)
                - retries: Retry configuration
                    - max_attempts: Maximum number of retry attempts
                    - mode: Retry mode (legacy/standard/adaptive)
                - config: Other Boto3 configuration parameters
        """
        super().__init__(config)
        self._client: Optional[BaseClient] = None
        self._service_name = config.get("service_name")
        self._region_name = config.get("region_name", "us-east-1")
        self._client_config = self._build_client_config()

        if not self._service_name:
            raise ConfigValidationError(
                "Boto3 configuration missing required field: service_name",
                config_key="boto3.connection.service_name",
            )

    def _build_client_config(self) -> Dict[str, Any]:
        """
        Build Boto3 Client Configuration

        Returns:
            Boto3 client configuration dictionary
        """
        config = {
            "region_name": self._region_name,
        }

        # Authentication information
        if "aws_access_key_id" in self._config:
            config["aws_access_key_id"] = self._config["aws_access_key_id"]
        if "aws_secret_access_key" in self._config:
            config["aws_secret_access_key"] = self._config["aws_secret_access_key"]
        if "aws_session_token" in self._config:
            config["aws_session_token"] = self._config["aws_session_token"]

        # Endpoint configuration
        if "endpoint_url" in self._config:
            config["endpoint_url"] = self._config["endpoint_url"]
        if "use_ssl" in self._config:
            config["use_ssl"] = self._config["use_ssl"]
        if "verify" in self._config:
            config["verify"] = self._config["verify"]

        # Build BotoConfig
        boto_config_params = {}

        # Connection pool configuration
        if "max_pool_connections" in self._config:
            boto_config_params["max_pool_connections"] = self._config[
                "max_pool_connections"
            ]

        # Timeout configuration
        timeouts = {}

        if "connect_timeout" in self._config:
            timeouts["connect_timeout"] = self._config["connect_timeout"]
        if "read_timeout" in self._config:
            timeouts["read_timeout"] = self._config["read_timeout"]
        if timeouts:
            boto_config_params["connect_timeout"] = timeouts.get("connect_timeout", 5)
            boto_config_params["read_timeout"] = timeouts.get("read_timeout", 60)

        # Retry configuration
        if "retries" in self._config:
            retries = self._config["retries"]
            boto_config_params["retries"] = {
                "max_attempts": retries.get("max_attempts", 3),
                "mode": retries.get("mode", "standard"),
            }

        # Other configurations
        if "config" in self._config:
            boto_config_params.update(self._config["config"])

        if boto_config_params:
            config["config"] = BotoConfig(**boto_config_params)

        return config

    def connect(self) -> BaseClient:
        """
        Establish AWS Service Connection

        Returns:
            Boto3 Client instance

        Raises:
            ConnectionError: When connection fails
            AuthenticationError: When authentication fails
        """
        try:
            self._client = boto3.client(self._service_name, **self._client_config)

            self._is_closed = False
            logger.debug(
                f"Boto3 client created [connection_id={self._connection_id}, "
                f"service={self._service_name}, region={self._region_name}]"
            )
            return self._client

        except NoCredentialsError as e:
            raise AuthenticationError(
                f"AWS credentials not found: {str(e)}",
                connection_type="boto3",
                original_error=e,
            )
        except PartialCredentialsError as e:
            raise AuthenticationError(
                f"AWS credentials incomplete: {str(e)}",
                connection_type="boto3",
                original_error=e,
            )
        except EndpointConnectionError as e:
            raise ConnectionError(
                f"AWS service endpoint connection failed: {str(e)}",
                details={"connection_type": "boto3", "original_error": str(e)},
            )
        except (ConnectTimeoutError, ReadTimeoutError) as e:
            raise ConnectionTimeoutError(
                f"AWS service connection timeout: {str(e)}",
                connection_type="boto3",
                original_error=e,
            )
        except ClientError as e:
            raise ConnectionError(
                f"AWS client error: {str(e)}", details={"connection_type": "boto3", "original_error": str(e)}
            )

    def close(self) -> None:
        """
        Close AWS Service Connection
        """
        if self._client and not self._is_closed:
            try:
                self._client.close()
                self._is_closed = True

                logger.debug(
                    f"Boto3 client closed [connection_id={self._connection_id}, "
                    f"service={self._service_name}]"
                )
            except Exception as e:
                logger.warning(
                    f"Error closing Boto3 client [connection_id={self._connection_id}]: {e}"
                )

    def is_healthy(self) -> bool:
        """
        Check Connection Health Status

        Check connection status by calling a simple API of the service

        Returns:
            True if connection is healthy, False if connection is abnormal
        """
        if not self._client or self._is_closed:
            return False

        try:
            # Perform different health checks based on service type
            if self._service_name == "s3":
                self._client.list_buckets()
            elif self._service_name == "dynamodb":
                self._client.list_tables()
            elif self._service_name == "sqs":
                self._client.list_queues()
            elif self._service_name == "sns":
                self._client.list_topics()
            elif self._service_name == "lambda":
                self._client.list_functions()
            elif self._service_name == "ec2":
                self._client.describe_instances(MaxResults=5)
            elif self._service_name == "sts":
                self._client.get_caller_identity()
            else:
                # For other services, assume connection is healthy
                return True
            return True
        except Exception:
            return False

    def call(self, operation: str, **kwargs) -> Dict[str, Any]:
        """
        Call AWS Service Operation

        Args:
            operation: Operation name
            **kwargs: Operation parameters

        Returns:
            API response dictionary

        Raises:
            ConnectionError: When call fails
        """
        if not self._client or self._is_closed:
            raise ConnectionError(
                "Boto3 client not created or already closed", details={"connection_type": "boto3"}
            )

        try:
            method = getattr(self._client, operation, None)

            if not method:
                raise ConnectionError(
                    f"Operation does not exist: {operation}", details={"connection_type": "boto3"}
                )

            response = method(**kwargs)
            return response

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            error_message = e.response["Error"]["Message"]

            if error_code in ["RequestTimeout", "TimeoutError"]:
                raise ConnectionTimeoutError(
                    f"AWS operation timeout: {error_message}",
                    connection_type="boto3",
                    original_error=e,
                )

            raise ConnectionError(
                f"AWS operation failed [{error_code}]: {error_message}",
                details={"connection_type": "boto3", "original_error": str(e)},
            )
        except Exception as e:
            raise ConnectionError(
                f"AWS operation call failed: {str(e)}",
                details={"connection_type": "boto3", "original_error": str(e)},
            )

    @property
    def client(self) -> Optional[BaseClient]:
        """Get Boto3 client instance"""
        return self._client

    @property
    def service_name(self) -> str:
        """Get AWS service name"""
        return self._service_name

    @property
    def region_name(self) -> str:
        """Get AWS region"""
        return self._region_name


class Boto3ConnectionPool(BaseConnectionPool[Boto3Connection]):
    """
    Boto3 Connection Pool Implementation

    Boto3-specific connection pool implementation based on the base connection pool, providing AWS service connection management and monitoring functionality.

    Attributes:
        _name: Connection pool name
        _config: Connection configuration
        _health_check_interval: Health check interval (seconds)
        _last_health_check: Last health check time
        _service_name: AWS service name
    """

    def __init__(
        self,
        name: str,
        config: ConnectionConfig,
        min_size: int = 2,
        max_size: int = 10,
        max_idle_time: float = 300.0,
        max_lifetime: float = 3600.0,
        health_check_interval: float = 60.0,
    ) -> None:
        """
        Initialize Boto3 Connection Pool

        Args:
            name: Connection pool unique identifier name
            config: Connection configuration object
            min_size: Minimum number of connections
            max_size: Maximum number of connections
            max_idle_time: Maximum idle time (seconds)
            max_lifetime: Maximum connection lifetime (seconds)
            health_check_interval: Health check interval (seconds)
        """
        # Build connection configuration dictionary
        conn_config = {
            "service_name": config.settings.get("service_name"),
            "region_name": config.settings.get("region_name", "us-east-1"),
            "aws_access_key_id": config.settings.get("aws_access_key_id"),
            "aws_secret_access_key": config.settings.get("aws_secret_access_key"),
            "aws_session_token": config.settings.get("aws_session_token"),
            "endpoint_url": config.settings.get("endpoint_url"),
            "use_ssl": config.settings.get("use_ssl", True),
            "verify": config.settings.get("verify", True),
            "max_pool_connections": max_size,
            "connect_timeout": config.settings.get("connect_timeout", 5),
            "read_timeout": config.settings.get("read_timeout", 60),
            "retries": config.settings.get("retries", {}),
            "config": config.settings.get("config", {}),
        }

        super().__init__(
            name=name,
            connection_class=Boto3Connection,
            min_size=min_size,
            max_size=max_size,
            max_idle_time=max_idle_time,
            max_lifetime=max_lifetime,
        )

        self._config = conn_config
        self._health_check_interval = health_check_interval
        self._last_health_check = 0.0
        self._last_health_status: Dict[str, Any] = {}
        self._service_name = config.settings.get("service_name", "unknown")

    def get_pool_type(self) -> str:
        """Get pool type identifier."""
        return "boto3"

    def _create_connection(self) -> Boto3Connection:
        """
        Create New Boto3 Connection

        Returns:
            Newly created Boto3 connection instance

        Raises:
            PoolError: When connection creation fails
        """
        try:
            connection = Boto3Connection(self._config)
            connection.connect()
            return connection
        except ConnectionError:
            raise
        except Exception as e:
            raise PoolError(
                f"Failed to create Boto3 connection: {str(e)}",
                details={"pool_name": self._name, "original_error": str(e)}
            )

    def _validate_connection(self, connection: Boto3Connection) -> bool:
        """
        Validate Connection Validity

        Args:
            connection: Connection to be validated

        Returns:
            True if connection is valid, False if invalid
        """
        return connection.is_healthy()

    def _destroy_connection(self, connection: Boto3Connection) -> None:
        """
        Destroy Connection

        Args:
            connection: Connection to be destroyed
        """
        try:
            connection.close()
        except Exception as e:
            logger.warning(f"Error destroying Boto3 connection: {e}")

    def health_check(self) -> Dict[str, Any]:
        """
        Perform Connection Pool Health Check

        Returns:
            Health check result dictionary
        """
        current_time = time.time()

        # Check if health check needs to be performed
        if current_time - self._last_health_check < self._health_check_interval:
            return self._last_health_status

        with self._lock:
            unhealthy_count = 0

            # Check active connections
            for conn_id, connection in list(self._active_connections.items()):
                if not connection.is_healthy():
                    unhealthy_count += 1

            # Check idle connections
            idle_conns = []
            while not self._idle_connections.empty():
                try:
                    connection = self._idle_connections.get_nowait()

                    if connection.is_healthy():
                        idle_conns.append(connection)
                    else:
                        unhealthy_count += 1
                        self._destroy_connection(connection)
                except Exception:
                    break

            # Put healthy connections back into the queue
            for connection in idle_conns:
                try:
                    self._idle_connections.put_nowait(connection)
                except Exception:
                    self._destroy_connection(connection)

            status = {
                "status": "unhealthy" if unhealthy_count > 0 else "healthy",
                "active_connections": len(self._active_connections),
                "idle_connections": self._idle_connections.qsize(),
                "total_connections": len(self._active_connections)
                + self._idle_connections.qsize(),
                "unhealthy_connections": unhealthy_count,
                "pool_name": self._name,
                "connection_type": "boto3",
                "service_name": self._service_name,
            }

            self._last_health_check = current_time
            self._last_health_status = status

            if unhealthy_count > 0:
                logger.warning(
                    f"Boto3 connection pool health check found {unhealthy_count} unhealthy connections"
                )

            return status

    def get_stats(self) -> Dict[str, Any]:
        """
        Get Connection Pool Statistics

        Returns:
            Statistics dictionary
        """
        with self._lock:
            return {
                "pool_name": self._name,
                "connection_type": "boto3",
                "service_name": self._service_name,
                "min_size": self._min_size,
                "max_size": self._max_size,
                "active_connections": len(self._active_connections),
                "idle_connections": self._idle_connections.qsize(),
                "total_connections": len(self._active_connections)
                + self._idle_connections.qsize(),
                "wait_timeout": self._wait_timeout,
                "max_idle_time": self._max_idle_time,
                "max_lifetime": self._max_lifetime,
                "health_check_interval": self._health_check_interval,
            }

    def call(self, operation: str, **kwargs) -> Dict[str, Any]:
        """
        Convenient Call Method

        Automatically acquire connection, execute call, and release connection

        Args:
            operation: AWS operation name
            **kwargs: Operation parameters

        Returns:
            API response dictionary
        """
        with self.connection() as connection:
            return connection.call(operation, **kwargs)

    # S3 Convenience Methods
    def s3_list_buckets(self) -> List[Dict[str, Any]]:
        """List S3 buckets"""
        response = self.call("list_buckets")
        return response.get("Buckets", [])

    def s3_list_objects(
        self, bucket: str, prefix: str = "", max_keys: int = 1000
    ) -> List[Dict[str, Any]]:
        """List S3 objects"""
        response = self.call(
            "list_objects_v2", Bucket=bucket, Prefix=prefix, MaxKeys=max_keys
        )
        return response.get("Contents", [])

    def s3_get_object(self, bucket: str, key: str) -> Dict[str, Any]:
        """Get S3 object"""
        return self.call("get_object", Bucket=bucket, Key=key)

    def s3_put_object(
        self, bucket: str, key: str, body: Any, **kwargs
    ) -> Dict[str, Any]:
        """Upload S3 object"""
        return self.call("put_object", Bucket=bucket, Key=key, Body=body, **kwargs)

    def s3_delete_object(self, bucket: str, key: str) -> Dict[str, Any]:
        """Delete S3 object"""
        return self.call("delete_object", Bucket=bucket, Key=key)

    # DynamoDB Convenience Methods
    def dynamodb_list_tables(self) -> List[str]:
        """List DynamoDB tables"""
        response = self.call("list_tables")
        return response.get("TableNames", [])

    def dynamodb_get_item(
        self, table: str, key: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Get DynamoDB item"""
        response = self.call("get_item", TableName=table, Key=key)
        return response.get("Item")

    def dynamodb_put_item(self, table: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """Write DynamoDB item"""
        return self.call("put_item", TableName=table, Item=item)

    def dynamodb_delete_item(self, table: str, key: Dict[str, Any]) -> Dict[str, Any]:
        """Delete DynamoDB item"""
        return self.call("delete_item", TableName=table, Key=key)

    def dynamodb_query(self, table: str, **kwargs) -> List[Dict[str, Any]]:
        """Query DynamoDB"""
        response = self.call("query", TableName=table, **kwargs)
        return response.get("Items", [])

    def dynamodb_scan(self, table: str, **kwargs) -> List[Dict[str, Any]]:
        """Scan DynamoDB"""
        response = self.call("scan", TableName=table, **kwargs)
        return response.get("Items", [])

    # SQS Convenience Methods
    def sqs_list_queues(self, queue_name_prefix: str = "") -> List[str]:
        """List SQS queues"""
        kwargs = {}
        if queue_name_prefix:
            kwargs["QueueNamePrefix"] = queue_name_prefix
        response = self.call("list_queues", **kwargs)
        return response.get("QueueUrls", [])

    def sqs_send_message(
        self, queue_url: str, message_body: str, **kwargs
    ) -> Dict[str, Any]:
        """Send SQS message"""
        return self.call(
            "send_message", QueueUrl=queue_url, MessageBody=message_body, **kwargs
        )

    def sqs_receive_message(self, queue_url: str, **kwargs) -> List[Dict[str, Any]]:
        """Receive SQS messages"""
        response = self.call("receive_message", QueueUrl=queue_url, **kwargs)
        return response.get("Messages", [])

    def sqs_delete_message(self, queue_url: str, receipt_handle: str) -> Dict[str, Any]:
        """Delete SQS message"""
        return self.call(
            "delete_message", QueueUrl=queue_url, ReceiptHandle=receipt_handle
        )

    # SNS Convenience Methods
    def sns_list_topics(self) -> List[Dict[str, Any]]:
        """List SNS topics"""
        response = self.call("list_topics")
        return response.get("Topics", [])

    def sns_publish(self, topic_arn: str, message: str, **kwargs) -> Dict[str, Any]:
        """Publish SNS message"""
        return self.call("publish", TopicArn=topic_arn, Message=message, **kwargs)

    # Lambda Convenience Methods
    def lambda_list_functions(self) -> List[Dict[str, Any]]:
        """List Lambda functions"""
        response = self.call("list_functions")
        return response.get("Functions", [])

    def lambda_invoke(
        self, function_name: str, payload: Any, **kwargs
    ) -> Dict[str, Any]:
        """Invoke Lambda function"""
        import base64
        import json

        if isinstance(payload, dict):
            payload = json.dumps(payload)

        response = self.call(
            "invoke", FunctionName=function_name, Payload=payload, **kwargs
        )

        # Parse response
        payload_body = response.get("Payload", b"").read()
        if payload_body:
            try:
                response["PayloadBody"] = json.loads(payload_body)
            except json.JSONDecodeError:
                response["PayloadBody"] = payload_body.decode("utf-8")

        return response

    # STS Convenience Methods
    def sts_get_caller_identity(self) -> Dict[str, Any]:
        """Get caller identity"""
        return self.call("get_caller_identity")


# Register Boto3 Connection Type
from ..plugin_registry import PluginRegistry


def register_boto3_plugin(registry: PluginRegistry) -> None:
    """
    Register Boto3 Plugin to Registry

    Args:
        registry: Plugin registry instance
    """
    registry.register(
        type_name='boto3',
        pool_class=Boto3ConnectionPool,
        connection_class=Boto3Connection
    )
    logger.info("Boto3 plugin registered")


# Alias for backward compatibility
Boto3Pool = Boto3ConnectionPool
