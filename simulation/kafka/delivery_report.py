"""Delivery reporting and operational statistics for AtmoSync Kafka publishing.

Provides structured delivery receipts and thread-safe publishing metrics for tracking
high-throughput telemetry streaming.
"""

from dataclasses import dataclass
import threading
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class DeliveryReport:
    """Receipt generated upon Kafka message delivery acknowledgement or failure.

    Attributes:
        event_id: The UUID of the SensorEvent that was published.
        container_id: Shipping container identifier from the event.
        sensor_type: Sensor modality ('temperature', 'humidity', 'vibration').
        topic: Kafka topic to which the message was published.
        partition: Assigned Kafka partition index, or None if delivery failed.
        offset: Assigned message offset within partition, or None if delivery failed.
        timestamp: Broker timestamp in milliseconds, or None if unavailable.
        error: Descriptive error message if delivery failed, None on success.
    """

    event_id: str
    container_id: str
    sensor_type: str
    topic: str
    partition: Optional[int] = None
    offset: Optional[int] = None
    timestamp: Optional[int] = None
    error: Optional[str] = None

    @property
    def is_success(self) -> bool:
        """Return True if message was successfully acknowledged by Kafka."""
        return self.error is None and self.partition is not None


class PublishingStats:
    """Thread-safe accumulator for Kafka publishing metrics.

    Maintains a strict distinction between:
    - attempted: Messages accepted by the local client queue.
    - delivered: Messages confirmed delivered by the Kafka broker.
    - failed: Messages rejected or timed out by the broker.
    - pending: Messages accepted by local queue but awaiting broker acknowledgement.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._attempted: int = 0
        self._delivered: int = 0
        self._failed: int = 0
        self._serialization_errors: int = 0
        self._flushes: int = 0

    @property
    def attempted(self) -> int:
        """Total number of messages successfully enqueued into the local producer buffer."""
        with self._lock:
            return self._attempted

    @property
    def delivered(self) -> int:
        """Total number of messages acknowledged as delivered by the Kafka broker."""
        with self._lock:
            return self._delivered

    @property
    def failed(self) -> int:
        """Total number of messages that failed delivery at the broker."""
        with self._lock:
            return self._failed

    @property
    def pending(self) -> int:
        """Number of messages currently awaiting broker acknowledgement."""
        with self._lock:
            return max(0, self._attempted - (self._delivered + self._failed))

    @property
    def serialization_errors(self) -> int:
        """Total number of events rejected due to serialization or validation errors."""
        with self._lock:
            return self._serialization_errors

    @property
    def flushes(self) -> int:
        """Total number of flush operations invoked."""
        with self._lock:
            return self._flushes

    def record_attempt(self) -> None:
        """Increment count of messages enqueued into the local client buffer."""
        with self._lock:
            self._attempted += 1

    def record_delivery(self, success: bool) -> None:
        """Record asynchronous delivery callback result from the Kafka broker."""
        with self._lock:
            if success:
                self._delivered += 1
            else:
                self._failed += 1

    def record_serialization_error(self) -> None:
        """Record event rejected before enqueueing."""
        with self._lock:
            self._serialization_errors += 1

    def record_flush(self) -> None:
        """Record invocation of flush()."""
        with self._lock:
            self._flushes += 1

    def reset(self) -> None:
        """Reset all metric counters to zero."""
        with self._lock:
            self._attempted = 0
            self._delivered = 0
            self._failed = 0
            self._serialization_errors = 0
            self._flushes = 0

    def to_dict(self) -> Dict[str, Any]:
        """Return a snapshot dictionary of current publishing metrics."""
        with self._lock:
            return {
                "attempted": self._attempted,
                "delivered": self._delivered,
                "failed": self._failed,
                "pending": max(0, self._attempted - (self._delivered + self._failed)),
                "serialization_errors": self._serialization_errors,
                "flushes": self._flushes,
            }
