"""Unit tests for KafkaConfig and configuration validation.

Tests:
- Default settings and custom values.
- Broker address validation (IPv4, hostnames, IPv6 bracketed format, port range, missing hosts).
- Topic name validation (alphanumeric, dot, underscore, hyphen, max length, rejection of '.' and '..').
- Timeout and retry constraints (delivery_timeout >= request_timeout, non-negative retries).
- Idempotence and max in-flight requests validation (librdkafka requirements).
- Queue buffer limits and bounded backpressure retry settings.
- Compression and security protocol validation.
- SASL credentials requirements.
- Environment variable overrides and parsing.
- Credential masking in string representation.
- Conversion to confluent-kafka configuration dictionary.
"""

import os
import pytest

from simulation.kafka.exceptions import KafkaConfigurationError
from simulation.kafka.kafka_config import KafkaConfig


class TestKafkaConfigDefaults:
    """Test default values and successful custom construction."""

    def test_default_configuration(self) -> None:
        """Default configuration contains sensible local defaults."""
        cfg = KafkaConfig()
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.topic == "atmosync.telemetry.v1"
        assert cfg.client_id == "atmosync-sensor-producer"
        assert cfg.acks == "all"
        assert cfg.enable_idempotence is True
        assert cfg.max_in_flight_requests_per_connection == 5
        assert cfg.delivery_timeout_ms == 30000
        assert cfg.request_timeout_ms == 5000
        assert cfg.retries == 5
        assert cfg.retry_backoff_ms == 100
        assert cfg.queue_buffering_max_messages == 100000
        assert cfg.queue_buffering_max_kbytes == 1048576
        assert cfg.queue_full_retries == 3
        assert cfg.queue_full_backoff_seconds == 0.1
        assert cfg.compression_type == "none"
        assert cfg.security_protocol == "PLAINTEXT"
        assert cfg.sasl_mechanism is None
        assert cfg.sasl_username is None
        assert cfg.sasl_password is None

    def test_custom_valid_configuration(self) -> None:
        """Custom valid configuration parameters are accepted."""
        cfg = KafkaConfig(
            bootstrap_servers="broker1.cloud:9092,broker2.cloud:9092",
            topic="custom.telemetry-topic_01",
            client_id="custom-producer",
            acks="all",
            enable_idempotence=True,
            max_in_flight_requests_per_connection=1,
            delivery_timeout_ms=15000,
            request_timeout_ms=3000,
            retries=10,
            retry_backoff_ms=250,
            compression_type="lz4",
        )
        assert cfg.bootstrap_servers == "broker1.cloud:9092,broker2.cloud:9092"
        assert cfg.topic == "custom.telemetry-topic_01"
        assert cfg.acks == "all"
        assert cfg.max_in_flight_requests_per_connection == 1
        assert cfg.compression_type == "lz4"

    def test_ipv6_bootstrap_servers_accepted(self) -> None:
        """Bracketed IPv6 addresses are accepted."""
        cfg = KafkaConfig(bootstrap_servers="[::1]:9092,[fe80::1]:9094")
        assert cfg.bootstrap_servers == "[::1]:9092,[fe80::1]:9094"


class TestKafkaConfigValidation:
    """Test strict configuration validation rules."""

    @pytest.mark.parametrize(
        "bad_servers",
        [
            "",
            "   ",
            "localhost",
            "localhost:",
            ":9092",
            "localhost:notaport",
            "localhost:999999",
            "localhost:-1",
            "broker1:9092,",
        ],
    )
    def test_invalid_bootstrap_servers(self, bad_servers: str) -> None:
        """Invalid bootstrap server formats raise KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(bootstrap_servers=bad_servers)

    @pytest.mark.parametrize(
        "bad_topic",
        [
            "",
            "   ",
            ".",
            "..",
            "topic with spaces",
            "topic$invalid#char",
            "a" * 250,  # Exceeds max 249 chars
        ],
    )
    def test_invalid_topic_names(self, bad_topic: str) -> None:
        """Invalid topic names raise KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(topic=bad_topic)

    @pytest.mark.parametrize("bad_client_id", ["", "   "])
    def test_invalid_client_id(self, bad_client_id: str) -> None:
        """Empty or blank client_id raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(client_id=bad_client_id)

    @pytest.mark.parametrize("bad_acks", ["invalid", 2, -2, None])
    def test_invalid_acks(self, bad_acks: any) -> None:
        """Invalid acks options raise KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(acks=bad_acks)

    def test_idempotence_requires_acks_all(self) -> None:
        """enable_idempotence=True requires acks='all' or -1."""
        with pytest.raises(KafkaConfigurationError, match="acks must be 'all'"):
            KafkaConfig(acks="1", enable_idempotence=True)

    def test_idempotence_max_in_flight_exceeded(self) -> None:
        """enable_idempotence=True requires max_in_flight_requests_per_connection <= 5."""
        with pytest.raises(KafkaConfigurationError, match="max_in_flight_requests_per_connection must be <= 5"):
            KafkaConfig(enable_idempotence=True, max_in_flight_requests_per_connection=6)

    @pytest.mark.parametrize("bad_in_flight", [0, -1, "five", True])
    def test_invalid_max_in_flight(self, bad_in_flight: any) -> None:
        """Invalid max_in_flight_requests_per_connection raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(max_in_flight_requests_per_connection=bad_in_flight)

    def test_delivery_timeout_less_than_request_timeout(self) -> None:
        """delivery_timeout_ms must be >= request_timeout_ms."""
        with pytest.raises(KafkaConfigurationError, match="must be >="):
            KafkaConfig(delivery_timeout_ms=3000, request_timeout_ms=5000)

    @pytest.mark.parametrize(
        "bad_timeout",
        [0, -100, "fast", True, False, 5.5],
    )
    def test_non_positive_timeouts(self, bad_timeout: any) -> None:
        """Non-positive or non-integer timeouts raise KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(delivery_timeout_ms=bad_timeout)
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(request_timeout_ms=bad_timeout)

    @pytest.mark.parametrize("bad_retries", [-1, "five", True])
    def test_invalid_retries(self, bad_retries: any) -> None:
        """Negative or non-integer retries raise KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(retries=bad_retries)

    @pytest.mark.parametrize("bad_queue_retries", [-1, "three", True])
    def test_invalid_queue_full_retries(self, bad_queue_retries: any) -> None:
        """Invalid queue_full_retries raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(queue_full_retries=bad_queue_retries)

    @pytest.mark.parametrize("bad_backoff", [0, -0.5, "fast", True])
    def test_invalid_queue_full_backoff(self, bad_backoff: any) -> None:
        """Invalid queue_full_backoff_seconds raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(queue_full_backoff_seconds=bad_backoff)

    @pytest.mark.parametrize("bad_codec", ["bzip2", "tar", "zip", 123])
    def test_invalid_compression(self, bad_codec: any) -> None:
        """Unsupported compression type raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError):
            KafkaConfig(compression_type=bad_codec)

    def test_invalid_security_protocol(self) -> None:
        """Unsupported security protocol raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError, match="security_protocol must be one of"):
            KafkaConfig(security_protocol="INVALID_SEC")

    def test_sasl_missing_credentials(self) -> None:
        """SASL protocol without mechanism, username, or password raises KafkaConfigurationError."""
        with pytest.raises(KafkaConfigurationError, match="sasl_mechanism"):
            KafkaConfig(security_protocol="SASL_PLAINTEXT")

        with pytest.raises(KafkaConfigurationError, match="sasl_username"):
            KafkaConfig(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_password="secret_password",
            )

        with pytest.raises(KafkaConfigurationError, match="sasl_password"):
            KafkaConfig(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_username="alice",
            )


class TestKafkaConfigEnvironmentAndMapping:
    """Test environment variable loading and confluent dictionary conversion."""

    def test_from_env_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """from_env() returns standard defaults when no env vars are set."""
        for key in list(os.environ.keys()):
            if key.startswith("ATMOSYNC_KAFKA_"):
                monkeypatch.delenv(key, raising=False)

        cfg = KafkaConfig.from_env()
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.topic == "atmosync.telemetry.v1"
        assert cfg.enable_idempotence is True
        assert cfg.max_in_flight_requests_per_connection == 5

    def test_from_env_with_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """from_env() parses environment variables correctly."""
        monkeypatch.setenv("ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "cluster.internal:9094")
        monkeypatch.setenv("ATMOSYNC_KAFKA_TOPIC", "env.telemetry.v1")
        monkeypatch.setenv("ATMOSYNC_KAFKA_CLIENT_ID", "env-producer")
        monkeypatch.setenv("ATMOSYNC_KAFKA_ACKS", "all")
        monkeypatch.setenv("ATMOSYNC_KAFKA_ENABLE_IDEMPOTENCE", "false")
        monkeypatch.setenv("ATMOSYNC_KAFKA_MAX_IN_FLIGHT", "1")
        monkeypatch.setenv("ATMOSYNC_KAFKA_DELIVERY_TIMEOUT_MS", "45000")
        monkeypatch.setenv("ATMOSYNC_KAFKA_REQUEST_TIMEOUT_MS", "10000")
        monkeypatch.setenv("ATMOSYNC_KAFKA_RETRIES", "8")
        monkeypatch.setenv("ATMOSYNC_KAFKA_COMPRESSION_TYPE", "zstd")

        cfg = KafkaConfig.from_env()
        assert cfg.bootstrap_servers == "cluster.internal:9094"
        assert cfg.topic == "env.telemetry.v1"
        assert cfg.client_id == "env-producer"
        assert cfg.enable_idempotence is False
        assert cfg.max_in_flight_requests_per_connection == 1
        assert cfg.delivery_timeout_ms == 45000
        assert cfg.request_timeout_ms == 10000
        assert cfg.retries == 8
        assert cfg.compression_type == "zstd"

    def test_from_env_invalid_boolean_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid boolean environment variable raises KafkaConfigurationError."""
        monkeypatch.setenv("ATMOSYNC_KAFKA_ENABLE_IDEMPOTENCE", "maybe")
        with pytest.raises(KafkaConfigurationError, match="must be a boolean"):
            KafkaConfig.from_env()

    def test_from_env_invalid_integer_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Invalid integer in environment variable raises KafkaConfigurationError."""
        monkeypatch.setenv("ATMOSYNC_KAFKA_DELIVERY_TIMEOUT_MS", "not-a-number")
        with pytest.raises(KafkaConfigurationError, match="must be an integer"):
            KafkaConfig.from_env()

    def test_credential_masking_in_repr(self) -> None:
        """SASL password must be masked in __repr__ to avoid leaking secrets."""
        cfg = KafkaConfig(
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="producer_agent",
            sasl_password="SuperSecretPassword123!",
        )
        repr_str = repr(cfg)
        assert "SuperSecretPassword123!" not in repr_str
        assert "***REDACTED***" in repr_str
        assert "producer_agent" in repr_str

    def test_to_confluent_config(self) -> None:
        """to_confluent_config() maps fields to librdkafka property names."""
        cfg = KafkaConfig(
            bootstrap_servers="localhost:9092",
            topic="telemetry.test",
            client_id="test-client",
            acks="all",
            enable_idempotence=True,
            max_in_flight_requests_per_connection=5,
            compression_type="gzip",
        )
        confluent_dict = cfg.to_confluent_config()
        assert confluent_dict["bootstrap.servers"] == "localhost:9092"
        assert confluent_dict["client.id"] == "test-client"
        assert confluent_dict["acks"] == "all"
        assert confluent_dict["enable.idempotence"] is True
        assert confluent_dict["max.in.flight.requests.per.connection"] == 5
        assert confluent_dict["compression.type"] == "gzip"
        assert confluent_dict["delivery.timeout.ms"] == 30000
