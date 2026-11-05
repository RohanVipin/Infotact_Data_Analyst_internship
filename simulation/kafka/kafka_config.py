"""Kafka configuration loader and validator for AtmoSync.

Validates broker addresses, topic names, delivery timeouts, security options, and retry
parameters. Supports loading from environment variables and provides credential masking.
"""

from dataclasses import dataclass
import os
import re
from typing import Any, Dict, Optional, Set, Union

from simulation.kafka.exceptions import KafkaConfigurationError

VALID_TOPIC_REGEX = re.compile(r"^[a-zA-Z0-9._-]+$")
VALID_COMPRESSION_TYPES: Set[str] = {"none", "gzip", "snappy", "lz4", "zstd"}
VALID_SECURITY_PROTOCOLS: Set[str] = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
VALID_SASL_MECHANISMS: Set[str] = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI", "OAUTHBEARER"}


@dataclass(frozen=True)
class KafkaConfig:
    """Immutable, validated configuration for AtmoSync Kafka producers.

    Attributes:
        bootstrap_servers: Comma-separated list of host:port broker addresses.
        topic: Destination Kafka topic name.
        client_id: Producer client identifier string.
        acks: Acknowledgement level ('all', '-1', '1', '0').
        enable_idempotence: When True, ensures exactly-once delivery per partition on retries
            and prevents message re-ordering (requires acks='all', retries > 0).
        max_in_flight_requests_per_connection: Maximum unacknowledged requests per connection
            (must be <= 5 when enable_idempotence is True to preserve ordering).
        delivery_timeout_ms: Maximum time in milliseconds for end-to-end message delivery.
        request_timeout_ms: Timeout in milliseconds for broker network requests.
        retries: Number of retries for transient errors.
        retry_backoff_ms: Milliseconds to wait before retrying a request.
        queue_buffering_max_messages: Maximum messages allowed in local producer queue.
        queue_buffering_max_kbytes: Maximum memory allowed in local queue in kilobytes.
        queue_full_retries: Bounded retries to attempt when producer buffer is full.
        queue_full_backoff_seconds: Seconds to poll/wait when producer buffer is full.
        compression_type: Compression codec ('none', 'gzip', 'snappy', 'lz4', 'zstd').
        security_protocol: Security protocol ('PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL').
        sasl_mechanism: Optional SASL mechanism if security_protocol uses SASL.
        sasl_username: Optional SASL username.
        sasl_password: Optional SASL password (masked in string representations).
        ssl_ca_location: Optional path to SSL CA certificate file.
    """

    bootstrap_servers: str = "localhost:9092"
    topic: str = "atmosync.telemetry.v1"
    client_id: str = "atmosync-sensor-producer"
    acks: Union[str, int] = "all"
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 5
    delivery_timeout_ms: int = 30000
    request_timeout_ms: int = 5000
    retries: int = 5
    retry_backoff_ms: int = 100
    queue_buffering_max_messages: int = 100000
    queue_buffering_max_kbytes: int = 1048576
    queue_full_retries: int = 3
    queue_full_backoff_seconds: float = 0.1
    compression_type: str = "none"
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: Optional[str] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate all configuration attributes strictly."""
        self._validate_bootstrap_servers(self.bootstrap_servers)
        self._validate_topic(self.topic)
        self._validate_client_id(self.client_id)
        self._validate_acks(self.acks)
        self._validate_idempotence_and_in_flight()
        self._validate_timeouts_and_retries()
        self._validate_queue_limits()
        self._validate_compression()
        self._validate_security()

    @staticmethod
    def _validate_bootstrap_servers(servers: str) -> None:
        if not isinstance(servers, str) or not servers.strip():
            raise KafkaConfigurationError("bootstrap_servers must be a non-empty string")
        for server in servers.split(","):
            cleaned = server.strip()
            if not cleaned:
                raise KafkaConfigurationError(f"Invalid empty broker address in '{servers}'")
            if ":" not in cleaned:
                raise KafkaConfigurationError(
                    f"Broker address '{cleaned}' must be in 'host:port' format"
                )
            host, port_str = cleaned.rsplit(":", 1)
            # Handle bracketed IPv6 (e.g. '[::1]')
            if host.startswith("[") and host.endswith("]"):
                host = host[1:-1]
            if not host.strip():
                raise KafkaConfigurationError(f"Missing host in broker address '{cleaned}'")
            try:
                port = int(port_str)
                if not (1 <= port <= 65535):
                    raise ValueError
            except ValueError:
                raise KafkaConfigurationError(
                    f"Broker port '{port_str}' in '{cleaned}' must be an integer between 1 and 65535"
                )

    @staticmethod
    def _validate_topic(topic: str) -> None:
        if not isinstance(topic, str) or not topic.strip():
            raise KafkaConfigurationError("topic must be a non-empty string")
        cleaned = topic.strip()
        if len(cleaned) > 249:
            raise KafkaConfigurationError(f"topic name exceeds 249 characters: {len(cleaned)}")
        if cleaned in {".", ".."}:
            raise KafkaConfigurationError(f"topic cannot be '.' or '..': {repr(cleaned)}")
        if not VALID_TOPIC_REGEX.match(cleaned):
            raise KafkaConfigurationError(
                f"topic '{cleaned}' contains invalid characters; must contain only letters, numbers, '.', '_', or '-'"
            )

    @staticmethod
    def _validate_client_id(client_id: str) -> None:
        if not isinstance(client_id, str) or not client_id.strip():
            raise KafkaConfigurationError("client_id must be a non-empty string")

    @staticmethod
    def _validate_acks(acks: Union[str, int]) -> None:
        valid_acks = {"all", "-1", "1", "0", -1, 1, 0}
        if acks not in valid_acks:
            raise KafkaConfigurationError(
                f"acks must be one of {valid_acks}, got {repr(acks)}"
            )

    def _validate_idempotence_and_in_flight(self) -> None:
        if not isinstance(self.enable_idempotence, bool):
            raise KafkaConfigurationError(
                f"enable_idempotence must be a boolean, got {type(self.enable_idempotence).__name__}"
            )
        if (
            isinstance(self.max_in_flight_requests_per_connection, bool)
            or not isinstance(self.max_in_flight_requests_per_connection, int)
            or self.max_in_flight_requests_per_connection <= 0
        ):
            raise KafkaConfigurationError(
                f"max_in_flight_requests_per_connection must be a positive integer, got {repr(self.max_in_flight_requests_per_connection)}"
            )
        if self.enable_idempotence:
            # librdkafka requires max.in.flight.requests.per.connection <= 5 when idempotence is enabled
            if self.max_in_flight_requests_per_connection > 5:
                raise KafkaConfigurationError(
                    f"max_in_flight_requests_per_connection must be <= 5 when enable_idempotence is True (got {self.max_in_flight_requests_per_connection})"
                )
            if self.acks not in {"all", "-1", -1}:
                raise KafkaConfigurationError(
                    f"acks must be 'all' or -1 when enable_idempotence is True (got {repr(self.acks)})"
                )

    def _validate_timeouts_and_retries(self) -> None:
        if isinstance(self.delivery_timeout_ms, bool) or not isinstance(self.delivery_timeout_ms, int) or self.delivery_timeout_ms <= 0:
            raise KafkaConfigurationError(
                f"delivery_timeout_ms must be a positive integer, got {repr(self.delivery_timeout_ms)}"
            )
        if isinstance(self.request_timeout_ms, bool) or not isinstance(self.request_timeout_ms, int) or self.request_timeout_ms <= 0:
            raise KafkaConfigurationError(
                f"request_timeout_ms must be a positive integer, got {repr(self.request_timeout_ms)}"
            )
        if self.delivery_timeout_ms < self.request_timeout_ms:
            raise KafkaConfigurationError(
                f"delivery_timeout_ms ({self.delivery_timeout_ms}) must be >= request_timeout_ms ({self.request_timeout_ms})"
            )
        if isinstance(self.retries, bool) or not isinstance(self.retries, int) or self.retries < 0:
            raise KafkaConfigurationError(
                f"retries must be a non-negative integer, got {repr(self.retries)}"
            )
        if isinstance(self.retry_backoff_ms, bool) or not isinstance(self.retry_backoff_ms, int) or self.retry_backoff_ms <= 0:
            raise KafkaConfigurationError(
                f"retry_backoff_ms must be a positive integer, got {repr(self.retry_backoff_ms)}"
            )

    def _validate_queue_limits(self) -> None:
        if isinstance(self.queue_buffering_max_messages, bool) or not isinstance(self.queue_buffering_max_messages, int) or self.queue_buffering_max_messages <= 0:
            raise KafkaConfigurationError(
                f"queue_buffering_max_messages must be a positive integer, got {repr(self.queue_buffering_max_messages)}"
            )
        if isinstance(self.queue_buffering_max_kbytes, bool) or not isinstance(self.queue_buffering_max_kbytes, int) or self.queue_buffering_max_kbytes <= 0:
            raise KafkaConfigurationError(
                f"queue_buffering_max_kbytes must be a positive integer, got {repr(self.queue_buffering_max_kbytes)}"
            )
        if isinstance(self.queue_full_retries, bool) or not isinstance(self.queue_full_retries, int) or self.queue_full_retries < 0:
            raise KafkaConfigurationError(
                f"queue_full_retries must be a non-negative integer, got {repr(self.queue_full_retries)}"
            )
        if (
            isinstance(self.queue_full_backoff_seconds, bool)
            or not isinstance(self.queue_full_backoff_seconds, (int, float))
            or self.queue_full_backoff_seconds <= 0
        ):
            raise KafkaConfigurationError(
                f"queue_full_backoff_seconds must be a positive number, got {repr(self.queue_full_backoff_seconds)}"
            )

    def _validate_compression(self) -> None:
        if self.compression_type not in VALID_COMPRESSION_TYPES:
            raise KafkaConfigurationError(
                f"compression_type must be one of {sorted(VALID_COMPRESSION_TYPES)}, got {repr(self.compression_type)}"
            )

    def _validate_security(self) -> None:
        if self.security_protocol not in VALID_SECURITY_PROTOCOLS:
            raise KafkaConfigurationError(
                f"security_protocol must be one of {sorted(VALID_SECURITY_PROTOCOLS)}, got {repr(self.security_protocol)}"
            )
        if self.security_protocol in {"SASL_PLAINTEXT", "SASL_SSL"}:
            if not self.sasl_mechanism or self.sasl_mechanism not in VALID_SASL_MECHANISMS:
                raise KafkaConfigurationError(
                    f"sasl_mechanism must be one of {sorted(VALID_SASL_MECHANISMS)} when security_protocol is {self.security_protocol}"
                )
            if not self.sasl_username or not self.sasl_username.strip():
                raise KafkaConfigurationError(
                    f"sasl_username is required when security_protocol is {self.security_protocol}"
                )
            if not self.sasl_password:
                raise KafkaConfigurationError(
                    f"sasl_password is required when security_protocol is {self.security_protocol}"
                )

    @classmethod
    def from_env(cls, **overrides: Any) -> "KafkaConfig":
        """Load Kafka configuration from environment variables with optional explicit overrides.

        Environment variables:
            ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS
            ATMOSYNC_KAFKA_TOPIC
            ATMOSYNC_KAFKA_CLIENT_ID
            ATMOSYNC_KAFKA_ACKS
            ATMOSYNC_KAFKA_ENABLE_IDEMPOTENCE
            ATMOSYNC_KAFKA_MAX_IN_FLIGHT
            ATMOSYNC_KAFKA_DELIVERY_TIMEOUT_MS
            ATMOSYNC_KAFKA_REQUEST_TIMEOUT_MS
            ATMOSYNC_KAFKA_RETRIES
            ATMOSYNC_KAFKA_RETRY_BACKOFF_MS
            ATMOSYNC_KAFKA_QUEUE_MAX_MESSAGES
            ATMOSYNC_KAFKA_QUEUE_MAX_KBYTES
            ATMOSYNC_KAFKA_QUEUE_FULL_RETRIES
            ATMOSYNC_KAFKA_QUEUE_FULL_BACKOFF_SECONDS
            ATMOSYNC_KAFKA_COMPRESSION_TYPE
            ATMOSYNC_KAFKA_SECURITY_PROTOCOL
            ATMOSYNC_KAFKA_SASL_MECHANISM
            ATMOSYNC_KAFKA_SASL_USERNAME
            ATMOSYNC_KAFKA_SASL_PASSWORD
            ATMOSYNC_KAFKA_SSL_CA_LOCATION
        """
        def _get_int(key: str, default: int) -> int:
            val = os.getenv(key)
            if val is not None and val.strip():
                try:
                    return int(val.strip())
                except ValueError:
                    raise KafkaConfigurationError(f"Environment variable {key} must be an integer, got {repr(val)}")
            return default

        def _get_float(key: str, default: float) -> float:
            val = os.getenv(key)
            if val is not None and val.strip():
                try:
                    return float(val.strip())
                except ValueError:
                    raise KafkaConfigurationError(f"Environment variable {key} must be a number, got {repr(val)}")
            return default

        def _get_bool(key: str, default: bool) -> bool:
            val = os.getenv(key)
            if val is not None and val.strip():
                normalized = val.strip().lower()
                if normalized in {"true", "1", "yes", "on"}:
                    return True
                if normalized in {"false", "0", "no", "off"}:
                    return False
                raise KafkaConfigurationError(f"Environment variable {key} must be a boolean ('true'/'false'), got {repr(val)}")
            return default

        config_dict: Dict[str, Any] = {
            "bootstrap_servers": os.getenv("ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            "topic": os.getenv("ATMOSYNC_KAFKA_TOPIC", "atmosync.telemetry.v1"),
            "client_id": os.getenv("ATMOSYNC_KAFKA_CLIENT_ID", "atmosync-sensor-producer"),
            "acks": os.getenv("ATMOSYNC_KAFKA_ACKS", "all"),
            "enable_idempotence": _get_bool("ATMOSYNC_KAFKA_ENABLE_IDEMPOTENCE", True),
            "max_in_flight_requests_per_connection": _get_int("ATMOSYNC_KAFKA_MAX_IN_FLIGHT", 5),
            "delivery_timeout_ms": _get_int("ATMOSYNC_KAFKA_DELIVERY_TIMEOUT_MS", 30000),
            "request_timeout_ms": _get_int("ATMOSYNC_KAFKA_REQUEST_TIMEOUT_MS", 5000),
            "retries": _get_int("ATMOSYNC_KAFKA_RETRIES", 5),
            "retry_backoff_ms": _get_int("ATMOSYNC_KAFKA_RETRY_BACKOFF_MS", 100),
            "queue_buffering_max_messages": _get_int("ATMOSYNC_KAFKA_QUEUE_MAX_MESSAGES", 100000),
            "queue_buffering_max_kbytes": _get_int("ATMOSYNC_KAFKA_QUEUE_MAX_KBYTES", 1048576),
            "queue_full_retries": _get_int("ATMOSYNC_KAFKA_QUEUE_FULL_RETRIES", 3),
            "queue_full_backoff_seconds": _get_float("ATMOSYNC_KAFKA_QUEUE_FULL_BACKOFF_SECONDS", 0.1),
            "compression_type": os.getenv("ATMOSYNC_KAFKA_COMPRESSION_TYPE", "none"),
            "security_protocol": os.getenv("ATMOSYNC_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "sasl_mechanism": os.getenv("ATMOSYNC_KAFKA_SASL_MECHANISM"),
            "sasl_username": os.getenv("ATMOSYNC_KAFKA_SASL_USERNAME"),
            "sasl_password": os.getenv("ATMOSYNC_KAFKA_SASL_PASSWORD"),
            "ssl_ca_location": os.getenv("ATMOSYNC_KAFKA_SSL_CA_LOCATION"),
        }

        # Apply programmatic overrides
        config_dict.update(overrides)

        return cls(**config_dict)

    def to_confluent_config(self) -> Dict[str, Any]:
        """Convert validated settings into a configuration dictionary for confluent-kafka."""
        cfg: Dict[str, Any] = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "acks": str(self.acks),
            "enable.idempotence": self.enable_idempotence,
            "max.in.flight.requests.per.connection": self.max_in_flight_requests_per_connection,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "request.timeout.ms": self.request_timeout_ms,
            "retries": self.retries,
            "retry.backoff.ms": self.retry_backoff_ms,
            "queue.buffering.max.messages": self.queue_buffering_max_messages,
            "queue.buffering.max.kbytes": self.queue_buffering_max_kbytes,
            "compression.type": self.compression_type,
            "security.protocol": self.security_protocol,
        }

        if self.sasl_mechanism:
            cfg["sasl.mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            cfg["sasl.username"] = self.sasl_username
        if self.sasl_password:
            cfg["sasl.password"] = self.sasl_password
        if self.ssl_ca_location:
            cfg["ssl.ca.location"] = self.ssl_ca_location

        return cfg

    def __repr__(self) -> str:
        """Return representation with sensitive credentials masked."""
        masked_pwd = "***REDACTED***" if self.sasl_password else None
        return (
            f"KafkaConfig(bootstrap_servers={repr(self.bootstrap_servers)}, "
            f"topic={repr(self.topic)}, client_id={repr(self.client_id)}, "
            f"acks={repr(self.acks)}, enable_idempotence={self.enable_idempotence}, "
            f"max_in_flight_requests_per_connection={self.max_in_flight_requests_per_connection}, "
            f"delivery_timeout_ms={self.delivery_timeout_ms}, "
            f"request_timeout_ms={self.request_timeout_ms}, retries={self.retries}, "
            f"compression_type={repr(self.compression_type)}, "
            f"security_protocol={repr(self.security_protocol)}, "
            f"sasl_mechanism={repr(self.sasl_mechanism)}, "
            f"sasl_username={repr(self.sasl_username)}, "
            f"sasl_password={repr(masked_pwd)})"
        )
