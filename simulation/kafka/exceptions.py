"""Custom exceptions for AtmoSync Kafka producer integration.

Provides domain-specific exceptions for configuration validation, publishing failures,
broker connectivity issues, and backpressure/queue overflow.
"""


class KafkaIntegrationError(Exception):
    """Base exception for all Kafka integration errors."""
    pass


class KafkaConfigurationError(KafkaIntegrationError, ValueError):
    """Raised when Kafka producer configuration is invalid, incomplete, or rejected."""
    pass


class KafkaPublishError(KafkaIntegrationError):
    """Raised when message serialization or publishing to Kafka fails."""
    pass


class KafkaBrokerError(KafkaIntegrationError, IOError):
    """Raised when the Kafka broker is unreachable or reports an unrecoverable cluster error."""
    pass


class KafkaBackpressureError(KafkaIntegrationError):
    """Raised when the producer internal message queue is full and cannot enqueue messages."""
    pass

