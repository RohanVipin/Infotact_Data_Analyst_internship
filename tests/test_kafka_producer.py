"""Unit tests for SensorEventKafkaProducer and delivery reporting.

Uses a mockable Kafka producer interface to verify:
- Initialisation, configuration mapping, and broker error handling.
- Canonical 7-field JSON payload generation and UTF-8 encoding.
- Container-based message key strategy (strict FIFO routing per container).
- Multi-container and multi-sensor (temperature, humidity, vibration) publishing.
- Asynchronous delivery callback dispatching and metrics recording.
- Atomic in-memory pre-validation for batch publishing (zero messages on validation error).
- Mixed broker delivery results and partial failures.
- Bounded backpressure handling (recovering on poll drainage vs exhausting retries).
- Controlled flushing, flush timeouts, and safe context management.
- Concurrent multi-threaded publishing safety.
"""

import json
import threading
from typing import Any, Callable, Dict, List, Optional
import uuid
import pytest

from simulation.events.event_schema import SensorEvent
from simulation.kafka.delivery_report import DeliveryReport
from simulation.kafka.exceptions import (
    KafkaBackpressureError,
    KafkaBrokerError,
    KafkaPublishError,
)
from simulation.kafka.kafka_config import KafkaConfig
from simulation.kafka.kafka_producer import SensorEventKafkaProducer


class MockKafkaMessage:
    """Mock representing a confluent-kafka Message object."""

    def __init__(
        self,
        topic: str = "atmosync.telemetry.v1",
        partition: int = 0,
        offset: int = 101,
        timestamp: int = 1730790000000,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def timestamp(self) -> tuple:
        return (1, self._timestamp)


class MockKafkaProducer:
    """Mock implementation of confluent_kafka.Producer."""

    def __init__(
        self,
        simulate_buffer_error_count: int = 0,
        simulate_delivery_error: Optional[str] = None,
        auto_ack_on_poll: bool = True,
        remaining_on_flush: int = 0,
    ) -> None:
        self.simulate_buffer_error_count = simulate_buffer_error_count
        self.simulate_delivery_error = simulate_delivery_error
        self.auto_ack_on_poll = auto_ack_on_poll
        self.remaining_on_flush = remaining_on_flush

        self.produced_messages: List[Dict[str, Any]] = []
        self.pending_callbacks: List[Callable[[], None]] = []
        self.flush_call_count = 0
        self.poll_call_count = 0
        self._lock = threading.Lock()

    def produce(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Optional[Callable[[Any, Any], None]] = None,
    ) -> None:
        with self._lock:
            if self.simulate_buffer_error_count > 0:
                self.simulate_buffer_error_count -= 1
                raise BufferError("Local producer queue full")

            msg_data = {"topic": topic, "key": key, "value": value}
            self.produced_messages.append(msg_data)

            if on_delivery is not None:
                def cb_wrapper():
                    if self.simulate_delivery_error:
                        on_delivery(self.simulate_delivery_error, None)
                    else:
                        on_delivery(None, MockKafkaMessage(topic=topic))

                self.pending_callbacks.append(cb_wrapper)

    def poll(self, timeout: float = 0) -> int:
        with self._lock:
            self.poll_call_count += 1
            count = len(self.pending_callbacks)
            if self.auto_ack_on_poll:
                callbacks = list(self.pending_callbacks)
                self.pending_callbacks.clear()
                for cb in callbacks:
                    cb()
            return count

    def flush(self, timeout: Optional[float] = None) -> int:
        with self._lock:
            self.flush_call_count += 1
            callbacks = list(self.pending_callbacks)
            self.pending_callbacks.clear()
            for cb in callbacks:
                cb()
            return self.remaining_on_flush


@pytest.fixture
def sample_events() -> List[SensorEvent]:
    """Provide valid SensorEvents for 3 different containers and sensors."""
    return [
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_001",
            sensor_type="temperature",
            observed_at="2026-11-04T12:00:00.000Z",
            value=4.25,
            unit="celsius",
        ),
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_002",
            sensor_type="humidity",
            observed_at="2026-11-04T12:00:00.000Z",
            value=65.2,
            unit="percent",
        ),
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_003",
            sensor_type="vibration",
            observed_at="2026-11-04T12:00:00.000Z",
            value=0.045,
            unit="m_s2",
        ),
    ]


class TestSensorEventKafkaProducerPublishing:
    """Tests for single event and batch publishing."""

    def test_publish_single_event(self, sample_events: List[SensorEvent]) -> None:
        """Publishing a single event produces correct topic, key, and 7-field JSON value."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        event = sample_events[0]
        producer.publish_event(event)

        assert len(mock_producer.produced_messages) == 1
        msg = mock_producer.produced_messages[0]

        assert msg["topic"] == "atmosync.telemetry.v1"
        assert msg["key"] == b"CONTAINER_001"

        # Verify value adheres to canonical 7-field contract
        parsed = json.loads(msg["value"].decode("utf-8"))
        assert parsed["schema_version"] == 1
        assert parsed["event_id"] == event.event_id
        assert parsed["container_id"] == "CONTAINER_001"
        assert parsed["sensor_type"] == "temperature"
        assert parsed["value"] == 4.25
        assert parsed["unit"] == "celsius"

        # Operational stats check
        assert producer.stats.attempted == 1
        assert producer.stats.delivered == 1
        assert producer.stats.failed == 0
        assert producer.stats.pending == 0

    def test_container_based_key_strategy(self, sample_events: List[SensorEvent]) -> None:
        """Every message is keyed by its UTF-8 container_id ensuring partition FIFO affinity."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        for e in sample_events:
            producer.publish_event(e)

        keys = [m["key"] for m in mock_producer.produced_messages]
        assert keys == [b"CONTAINER_001", b"CONTAINER_002", b"CONTAINER_003"]

    def test_publish_batch_events(self, sample_events: List[SensorEvent]) -> None:
        """Batch publishing produces all events and returns the submitted count."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        count = producer.publish_batch(sample_events)
        assert count == 3
        assert len(mock_producer.produced_messages) == 3
        assert producer.stats.attempted == 3
        assert producer.stats.delivered == 3
        assert producer.stats.pending == 0

    def test_publish_empty_batch(self) -> None:
        """Publishing an empty batch returns 0 and attempts nothing."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        count = producer.publish_batch([])
        assert count == 0
        assert len(mock_producer.produced_messages) == 0
        assert producer.stats.attempted == 0

    def test_batch_atomic_validation_zero_messages_on_error(
        self, sample_events: List[SensorEvent]
    ) -> None:
        """If any event in a batch fails validation, ZERO messages are published."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        bad_event = {
            "schema_version": 1,
            "event_id": "not-a-valid-uuid",
            "container_id": "CONTAINER_001",
            "sensor_type": "temperature",
            "observed_at": "2026-11-04T12:00:00.000Z",
            "value": 4.25,
            "unit": "celsius",
        }

        batch = [sample_events[0], bad_event, sample_events[1]]

        with pytest.raises(KafkaPublishError, match="Batch rejected"):
            producer.publish_batch(batch)

        # Zero messages must have been enqueued to Kafka
        assert len(mock_producer.produced_messages) == 0
        assert producer.stats.attempted == 0
        assert producer.stats.serialization_errors == 1


class TestSensorEventKafkaProducerCallbacksAndErrors:
    """Tests for callbacks, delivery reports, backpressure, and errors."""

    def test_delivery_success_callback(self, sample_events: List[SensorEvent]) -> None:
        """Successful delivery invokes callback with a valid DeliveryReport."""
        mock_producer = MockKafkaProducer(auto_ack_on_poll=True)
        producer = SensorEventKafkaProducer(producer=mock_producer)

        reports: List[DeliveryReport] = []

        def on_delivery(report: DeliveryReport) -> None:
            reports.append(report)

        producer.publish_event(sample_events[0], on_delivery=on_delivery)

        assert len(reports) == 1
        r = reports[0]
        assert r.is_success is True
        assert r.error is None
        assert r.container_id == "CONTAINER_001"
        assert r.partition == 0
        assert r.offset == 101

    def test_delivery_failure_callback(self, sample_events: List[SensorEvent]) -> None:
        """Delivery failure invokes callback with error in DeliveryReport."""
        mock_producer = MockKafkaProducer(
            simulate_delivery_error="Broker: Message timed out",
            auto_ack_on_poll=True,
        )
        producer = SensorEventKafkaProducer(producer=mock_producer)

        reports: List[DeliveryReport] = []

        def on_delivery(report: DeliveryReport) -> None:
            reports.append(report)

        producer.publish_event(sample_events[0], on_delivery=on_delivery)

        assert len(reports) == 1
        r = reports[0]
        assert r.is_success is False
        assert "Message timed out" in r.error
        assert producer.stats.failed == 1
        assert producer.stats.delivered == 0
        assert producer.stats.pending == 0

    def test_mixed_broker_delivery_outcomes(self, sample_events: List[SensorEvent]) -> None:
        """Batch delivery tracks independent delivery and failure counts accurately."""
        # Custom mock where event 0 and 2 succeed, but event 1 fails
        class SelectiveFailProducer(MockKafkaProducer):
            def produce(self, topic, key, value, on_delivery=None):
                self.produced_messages.append({"key": key})
                if on_delivery:
                    if key == b"CONTAINER_002":
                        self.pending_callbacks.append(
                            lambda: on_delivery("Disk full on broker partition 1", None)
                        )
                    else:
                        self.pending_callbacks.append(
                            lambda: on_delivery(None, MockKafkaMessage())
                        )

        mock = SelectiveFailProducer(auto_ack_on_poll=True)
        producer = SensorEventKafkaProducer(producer=mock)

        producer.publish_batch(sample_events)

        assert producer.stats.attempted == 3
        assert producer.stats.delivered == 2
        assert producer.stats.failed == 1
        assert producer.stats.pending == 0

    def test_user_callback_exception_does_not_crash_producer(
        self, sample_events: List[SensorEvent]
    ) -> None:
        """Exceptions raised inside user callbacks are safely handled."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        def failing_cb(report: DeliveryReport) -> None:
            raise RuntimeError("Exploding callback")

        producer.publish_event(sample_events[0], on_delivery=failing_cb)
        assert producer.stats.delivered == 1

    def test_bounded_backpressure_recovery_on_poll(self, sample_events: List[SensorEvent]) -> None:
        """When queue is full, producer polls to drain callbacks and successfully retries."""
        # 1 buffer error, then succeeds on retry
        mock_producer = MockKafkaProducer(simulate_buffer_error_count=1)
        producer = SensorEventKafkaProducer(producer=mock_producer)

        producer.publish_event(sample_events[0])
        assert producer.stats.attempted == 1
        assert mock_producer.poll_call_count >= 1

    def test_bounded_backpressure_exhaustion_raises(self, sample_events: List[SensorEvent]) -> None:
        """When queue remains persistently full after all retries, KafkaBackpressureError is raised."""
        # More buffer errors than allowed retries
        mock_producer = MockKafkaProducer(simulate_buffer_error_count=10)
        cfg = KafkaConfig(queue_full_retries=2, queue_full_backoff_seconds=0.01)
        producer = SensorEventKafkaProducer(config=cfg, producer=mock_producer)

        with pytest.raises(KafkaBackpressureError, match="after 2 backpressure retry attempts"):
            producer.publish_event(sample_events[0])

    def test_serialization_failure_raises(self) -> None:
        """Invalid event raises KafkaPublishError and records stats."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        invalid_event = {"corrupted": "payload"}

        with pytest.raises(KafkaPublishError, match="Failed to serialize event"):
            producer.publish_event(invalid_event)

        assert producer.stats.serialization_errors == 1


class TestSensorEventKafkaProducerLifecycle:
    """Tests for flush, close, context manager, and post-close behavior."""

    def test_flush_invoked(self, sample_events: List[SensorEvent]) -> None:
        """flush() calls underlying producer flush and updates stats."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        producer.publish_event(sample_events[0])
        remaining = producer.flush()

        assert remaining == 0
        assert mock_producer.flush_call_count == 1
        assert producer.stats.flushes == 1

    def test_flush_with_raise_on_timeout(self, sample_events: List[SensorEvent]) -> None:
        """flush(raise_on_timeout=True) raises KafkaPublishError if messages remain."""
        mock_producer = MockKafkaProducer(remaining_on_flush=2)
        producer = SensorEventKafkaProducer(producer=mock_producer)

        # Without raise_on_timeout, returns count
        assert producer.flush(raise_on_timeout=False) == 2

        # With raise_on_timeout=True, raises error
        with pytest.raises(KafkaPublishError, match="Kafka flush timed out"):
            producer.flush(raise_on_timeout=True)

    def test_close_and_post_close_rejection(self, sample_events: List[SensorEvent]) -> None:
        """Closing the producer prevents further publishing."""
        mock_producer = MockKafkaProducer()
        producer = SensorEventKafkaProducer(producer=mock_producer)

        assert not producer.is_closed
        producer.close()
        assert producer.is_closed

        # Calling close again is safe
        assert producer.close() == 0

        # Publishing after close must raise KafkaPublishError
        with pytest.raises(KafkaPublishError, match="producer has been closed"):
            producer.publish_event(sample_events[0])

        with pytest.raises(KafkaPublishError, match="producer has been closed"):
            producer.publish_batch(sample_events)

    def test_context_manager_normal_exit(self, sample_events: List[SensorEvent]) -> None:
        """Using producer as context manager flushes and closes on normal exit."""
        mock_producer = MockKafkaProducer()

        with SensorEventKafkaProducer(producer=mock_producer) as p:
            p.publish_event(sample_events[0])
            assert not p.is_closed

        assert p.is_closed
        assert mock_producer.flush_call_count == 1

    def test_context_manager_exceptional_exit_preserves_error(
        self, sample_events: List[SensorEvent]
    ) -> None:
        """When an exception occurs inside context block, original exception propagates."""
        mock_producer = MockKafkaProducer()
        producer_ref = None

        with pytest.raises(ValueError, match="Simulated application crash"):
            with SensorEventKafkaProducer(producer=mock_producer) as p:
                producer_ref = p
                p.publish_event(sample_events[0])
                raise ValueError("Simulated application crash")

        assert producer_ref is not None
        assert producer_ref.is_closed

    def test_broker_init_failure_wraps_in_kafka_broker_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Underlying producer constructor failures are wrapped in KafkaBrokerError."""
        import simulation.kafka.kafka_producer as kp_module

        class CrashingProducer:
            def __init__(self, *args, **kwargs):
                raise RuntimeError("Failed to resolve brokers")

        monkeypatch.setattr(kp_module.confluent_kafka, "Producer", CrashingProducer)

        with pytest.raises(KafkaBrokerError, match="Failed to initialize Kafka producer"):
            SensorEventKafkaProducer()


class TestConcurrentPublishingSafety:
    """Test thread-safety when multiple worker threads publish concurrently."""

    def test_concurrent_publishing_stats_integrity(self, sample_events: List[SensorEvent]) -> None:
        """Concurrent publishing updates thread-safe PublishingStats accurately."""
        mock_producer = MockKafkaProducer(auto_ack_on_poll=True)
        producer = SensorEventKafkaProducer(producer=mock_producer)

        def worker():
            for _ in range(50):
                producer.publish_event(sample_events[0])

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert producer.stats.attempted == 200
        assert producer.stats.delivered == 200
        assert producer.stats.failed == 0
        assert producer.stats.pending == 0
