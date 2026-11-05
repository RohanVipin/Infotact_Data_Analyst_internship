"""AtmoSync Kafka Producer for high-throughput sensor telemetry streaming.

Publishes canonical 7-field SensorEvent instances to Apache Kafka using confluent-kafka.
Features:
- Option A: One independent Kafka message per virtual sensor reading.
- Container-based partition key strategy ensuring partition affinity per container.
- Configurable idempotence ensuring strict sequential message ordering and deduplication per partition.
- Non-blocking asynchronous publishing with delivery report callbacks.
- Bounded backpressure handling that polls to drain delivery callbacks on queue congestion.
- Batch publishing with atomic in-memory pre-validation (zero messages submitted on validation error).
- Comprehensive publishing statistics, controlled flushing, and safe context management.
"""

import logging
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

try:
    import confluent_kafka
    from confluent_kafka import KafkaException
except ImportError:  # pragma: no cover
    confluent_kafka = None  # type: ignore[assignment]
    KafkaException = Exception  # type: ignore[misc, assignment]

from simulation.events.event_schema import SensorEvent
from simulation.events.event_serializer import EventSerializationError, EventSerializer
from simulation.kafka.delivery_report import DeliveryReport, PublishingStats
from simulation.kafka.exceptions import (
    KafkaBackpressureError,
    KafkaBrokerError,
    KafkaPublishError,
)
from simulation.kafka.kafka_config import KafkaConfig

logger = logging.getLogger(__name__)


class SensorEventKafkaProducer:
    """Asynchronous Kafka producer for publishing canonical AtmoSync sensor events.

    Attributes:
        config: Validated KafkaConfig settings.
        stats: Thread-safe metrics accumulator for publishing operations.
    """

    def __init__(
        self,
        config: Optional[KafkaConfig] = None,
        producer: Optional[Any] = None,
    ) -> None:
        """Initialize the Kafka producer.

        Args:
            config: Optional KafkaConfig instance. If omitted, loaded from environment.
            producer: Optional pre-configured or mock confluent_kafka.Producer instance
                (used for dependency injection and testing without a live broker).

        Raises:
            KafkaBrokerError: If the Kafka client fails to initialize.
        """
        self._config = config or KafkaConfig.from_env()
        self._stats = PublishingStats()
        self._is_closed = False

        if producer is not None:
            self._producer = producer
        else:
            if confluent_kafka is None:
                raise KafkaBrokerError(
                    "confluent-kafka is not installed. Install it with: pip install confluent-kafka"
                )
            try:
                self._producer = confluent_kafka.Producer(self._config.to_confluent_config())
            except Exception as exc:
                raise KafkaBrokerError(f"Failed to initialize Kafka producer: {exc}") from exc

    @property
    def config(self) -> KafkaConfig:
        """Return the active Kafka configuration."""
        return self._config

    @property
    def stats(self) -> PublishingStats:
        """Return the publishing statistics accumulator."""
        return self._stats

    @property
    def is_closed(self) -> bool:
        """Return True if the producer has been shut down."""
        return self._is_closed

    @staticmethod
    def compute_message_key(event: Union[SensorEvent, Dict[str, Any]]) -> bytes:
        """Derive a stable Kafka message key based on shipping container identity.

        Key Strategy and Ordering Guarantees:
            1. Partition Affinity: Each message is keyed by its UTF-8 encoded `container_id`
               (e.g., b'CONTAINER_001'). In Kafka, messages with identical keys hash to the same
               topic partition.
            2. Intra-Partition Ordering: When `enable_idempotence=True` (default) and
               `max_in_flight_requests_per_connection <= 5`, the single producer instance guarantees
               strict sequential message ordering on that partition even across transient retries.
            3. Application Caveat: Across uncoordinated concurrent producer instances or distinct
               client processes, broker arrival order may vary. Downstream consumers should always
               use the event's UTC `observed_at` timestamp for absolute temporal sequencing.

        Args:
            event: SensorEvent instance or event dictionary.

        Returns:
            UTF-8 encoded container_id bytes.

        Raises:
            KafkaPublishError: If container_id is missing or empty.
        """
        container_id: Optional[str] = None
        if isinstance(event, SensorEvent):
            container_id = event.container_id
        elif isinstance(event, dict):
            container_id = event.get("container_id")

        if not container_id or not str(container_id).strip():
            raise KafkaPublishError(
                f"Cannot derive message key: missing or empty container_id in event: {event}"
            )

        return str(container_id).strip().encode("utf-8")

    def _create_delivery_callback(
        self,
        event_id: str,
        container_id: str,
        sensor_type: str,
        user_callback: Optional[Callable[[DeliveryReport], None]],
    ) -> Callable[[Any, Any], None]:
        """Generate an internal callback closure for confluent-kafka message delivery."""
        def callback(err: Any, msg: Any) -> None:
            error_str = str(err) if err is not None else None
            partition = msg.partition() if msg is not None else None
            offset = msg.offset() if msg is not None else None
            topic = msg.topic() if msg is not None else self._config.topic
            timestamp = None
            if msg is not None:
                try:
                    ts_tuple = msg.timestamp()
                    if ts_tuple and len(ts_tuple) > 1:
                        timestamp = ts_tuple[1]
                except Exception:
                    pass

            report = DeliveryReport(
                event_id=event_id,
                container_id=container_id,
                sensor_type=sensor_type,
                topic=topic,
                partition=partition,
                offset=offset,
                timestamp=timestamp,
                error=error_str,
            )

            # Record success or failure in operational metrics
            self._stats.record_delivery(success=report.is_success)

            if report.is_success:
                logger.debug(
                    "Message delivered: event_id=%s topic=%s partition=%s offset=%s",
                    event_id,
                    topic,
                    partition,
                    offset,
                )
            else:
                logger.error(
                    "Message delivery failed: event_id=%s topic=%s error=%s",
                    event_id,
                    topic,
                    error_str,
                )

            # Invoke user-supplied callback safely without breaking librdkafka dispatch loop
            if user_callback is not None:
                try:
                    user_callback(report)
                except Exception as cb_exc:
                    logger.exception("Error in user delivery callback for event %s: %s", event_id, cb_exc)

        return callback

    def _produce_with_backpressure(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        on_delivery: Callable[[Any, Any], None],
    ) -> None:
        """Attempt to produce a message, applying a bounded polling backpressure policy on BufferError."""
        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=on_delivery,
            )
            self._stats.record_attempt()
            self._producer.poll(0)
            return
        except BufferError:
            pass

        # Queue full: perform bounded retry loop with active callback polling
        retries = self._config.queue_full_retries
        backoff = self._config.queue_full_backoff_seconds

        for attempt in range(retries):
            logger.warning(
                "Producer queue full; polling for %.2fs to drain delivery callbacks (attempt %d/%d)",
                backoff,
                attempt + 1,
                retries,
            )
            self._producer.poll(backoff)
            try:
                self._producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    on_delivery=on_delivery,
                )
                self._stats.record_attempt()
                self._producer.poll(0)
                return
            except BufferError:
                continue

        # Exhausted bounded backpressure attempts
        raise KafkaBackpressureError(
            f"Producer queue is full (max messages: {self._config.queue_buffering_max_messages}) "
            f"after {retries} backpressure retry attempts"
        )

    def publish_event(
        self,
        event: Union[SensorEvent, Dict[str, Any]],
        on_delivery: Optional[Callable[[DeliveryReport], None]] = None,
    ) -> None:
        """Publish a single sensor event asynchronously to Kafka.

        Args:
            event: SensorEvent instance or event dictionary.
            on_delivery: Optional callback receiving a DeliveryReport upon delivery or failure.

        Raises:
            KafkaPublishError: If the producer is closed, or event fails serialization/validation.
            KafkaBackpressureError: If the internal producer buffer remains full after backpressure retries.
        """
        if self._is_closed:
            raise KafkaPublishError("Cannot publish event: producer has been closed")

        try:
            key_bytes = self.compute_message_key(event)
            json_str = EventSerializer.serialize(event, validate=True)
            value_bytes = json_str.encode("utf-8")
        except (EventSerializationError, KafkaPublishError) as exc:
            self._stats.record_serialization_error()
            raise KafkaPublishError(f"Failed to serialize event for publishing: {exc}") from exc

        event_id = event.event_id if isinstance(event, SensorEvent) else event["event_id"]
        container_id = event.container_id if isinstance(event, SensorEvent) else event["container_id"]
        sensor_type = event.sensor_type if isinstance(event, SensorEvent) else event["sensor_type"]

        cb = self._create_delivery_callback(event_id, container_id, sensor_type, on_delivery)

        try:
            self._produce_with_backpressure(
                topic=self._config.topic,
                key=key_bytes,
                value=value_bytes,
                on_delivery=cb,
            )
        except KafkaBackpressureError:
            raise
        except Exception as exc:
            raise KafkaPublishError(f"Failed to enqueue message to Kafka: {exc}") from exc

    def publish_batch(
        self,
        events: Sequence[Union[SensorEvent, Dict[str, Any]]],
        on_delivery: Optional[Callable[[DeliveryReport], None]] = None,
    ) -> int:
        """Publish a batch of sensor events to Kafka with atomic in-memory pre-validation.

        Guarantees that:
            1. All events in the batch are validated and serialized in memory FIRST.
            2. If ANY event fails validation or key extraction, an exception is raised immediately
               and ZERO messages from this batch are submitted to the Kafka buffer.
            3. Each valid event is published as an independent Kafka message.
            4. Once enqueued, individual message deliveries succeed or fail independently at the broker.

        Args:
            events: Sequence of SensorEvent or event dictionaries.
            on_delivery: Optional callback receiving DeliveryReport for each message.

        Returns:
            Number of events successfully enqueued.

        Raises:
            KafkaPublishError: If producer is closed or an event fails validation/serialization.
            KafkaBackpressureError: If the producer queue fills during submission.
        """
        if not events:
            return 0

        if self._is_closed:
            raise KafkaPublishError("Cannot publish batch: producer has been closed")

        # Phase 1: In-memory pre-validation and serialization
        prepared_messages: List[Tuple[bytes, bytes, str, str, str]] = []
        for item in events:
            try:
                key_bytes = self.compute_message_key(item)
                json_str = EventSerializer.serialize(item, validate=True)
                val_bytes = json_str.encode("utf-8")
                e_id = item.event_id if isinstance(item, SensorEvent) else item["event_id"]
                c_id = item.container_id if isinstance(item, SensorEvent) else item["container_id"]
                s_type = item.sensor_type if isinstance(item, SensorEvent) else item["sensor_type"]
                prepared_messages.append((key_bytes, val_bytes, e_id, c_id, s_type))
            except (EventSerializationError, KafkaPublishError) as exc:
                self._stats.record_serialization_error()
                raise KafkaPublishError(
                    f"Batch rejected: event validation/serialization failed: {exc}"
                ) from exc

        # Phase 2: Enqueue validated messages to Kafka producer buffer
        enqueued_count = 0
        for key_bytes, val_bytes, e_id, c_id, s_type in prepared_messages:
            cb = self._create_delivery_callback(e_id, c_id, s_type, on_delivery)
            try:
                self._produce_with_backpressure(
                    topic=self._config.topic,
                    key=key_bytes,
                    value=val_bytes,
                    on_delivery=cb,
                )
                enqueued_count += 1
            except KafkaBackpressureError:
                raise
            except Exception as exc:
                raise KafkaPublishError(f"Failed to produce message in batch: {exc}") from exc

        # Poll to service callbacks
        self._producer.poll(0)
        return enqueued_count

    def flush(
        self,
        timeout: Optional[float] = None,
        raise_on_timeout: bool = False,
    ) -> int:
        """Wait for all outstanding messages to be delivered and callbacks served.

        Args:
            timeout: Maximum seconds to wait. Defaults to config.delivery_timeout_ms / 1000.0.
            raise_on_timeout: If True and messages remain in queue after timeout,
                raises KafkaPublishError. Default False.

        Returns:
            Number of messages still remaining in queue (0 indicates full success).

        Raises:
            KafkaPublishError: If raise_on_timeout is True and messages remain in queue.
        """
        if timeout is None:
            timeout = self._config.delivery_timeout_ms / 1000.0

        self._stats.record_flush()
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning(
                "Kafka flush timed out after %.2fs: %d message(s) still in queue",
                timeout,
                remaining,
            )
            if raise_on_timeout:
                raise KafkaPublishError(
                    f"Kafka flush timed out after {timeout:.2f}s with {remaining} message(s) still in queue"
                )
        return remaining

    def close(
        self,
        timeout: Optional[float] = None,
        raise_on_timeout: bool = False,
    ) -> int:
        """Flush pending messages and mark producer as closed.

        Subsequent attempts to publish will raise KafkaPublishError. Safe to call multiple times.

        Args:
            timeout: Maximum seconds to wait during flush.
            raise_on_timeout: If True, raises KafkaPublishError if flush times out.

        Returns:
            Number of messages remaining in queue after flush.
        """
        if self._is_closed:
            return 0

        self._is_closed = True
        return self.flush(timeout, raise_on_timeout=raise_on_timeout)

    def __enter__(self) -> "SensorEventKafkaProducer":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, ensuring pending messages are flushed and producer is closed.

        Preserves any in-flight application exception that occurred within the with-block.
        """
        try:
            self.close()
        except Exception as exc:
            if exc_type is None:
                raise
            logger.error("Error during producer close on exceptional exit: %s", exc)
