"""Output sink architecture for AtmoSync sensor event dispatching.

Defines the OutputSink abstraction and concrete adapters for local append-only JSONL files
and Apache Kafka publishing, along with CompositeOutputSink for deterministic multi-destination
event broadcasting.

Architectural Design Principles:
1. Single Event Identity: Generated immutable SensorEvent instances are passed by reference
   to all active sinks without regeneration or data mutation.
2. Independent Sink Lifecycle: Each sink manages its own connection, buffer, flushing, and
   resource cleanup.
3. Fail-Fast Error Propagation: Synchronous write failures propagate immediately.
4. Honest Distributed Semantics: No false claims of distributed 2PC transactions or cross-sink
   rollback. If JSONL succeeds and Kafka fails, the JSONL record remains written.
"""

from abc import ABC, abstractmethod
import logging
from pathlib import Path
from typing import Any, Callable, List, Optional, Sequence, Union

from simulation.events.event_schema import SensorEvent
from simulation.output.jsonl_writer import JsonlWriter, JsonlWriterError
from simulation.kafka.delivery_report import DeliveryReport
from simulation.kafka.exceptions import KafkaPublishError
from simulation.kafka.kafka_config import KafkaConfig
from simulation.kafka.kafka_producer import SensorEventKafkaProducer

logger = logging.getLogger(__name__)


class OutputSink(ABC):
    """Abstract base class for all AtmoSync telemetry output destinations.

    Subclasses must implement write_event(). Buffered sinks should override flush()
    and close() to manage underlying network or file resources.
    """

    @abstractmethod
    def write_event(self, event: SensorEvent) -> None:
        """Write or publish a single canonical SensorEvent.

        Args:
            event: Canonical, immutable SensorEvent instance.

        Raises:
            Exception: If writing, serialization, or enqueueing fails.
        """
        pass

    def write_events(self, events: Sequence[SensorEvent]) -> None:
        """Write or publish a sequence of SensorEvent instances.

        Default implementation iterates over events and calls write_event.
        Subclasses may override for optimized batch operations.

        Args:
            events: Sequence of SensorEvent instances.

        Raises:
            Exception: If any event fails to write.
        """
        for event in events:
            self.write_event(event)

    def flush(self) -> None:
        """Flush buffered events to the destination.

        Default implementation is a no-op for unbuffered sinks.
        """
        pass

    def close(self) -> None:
        """Flush remaining buffers and release held resources.

        Default implementation calls flush().
        """
        self.flush()

    def __enter__(self) -> "OutputSink":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, invoking close() while preserving in-flight exceptions."""
        self.close()


class JsonlSink(OutputSink):
    """OutputSink adapter wrapping an append-only JsonlWriter.

    Guarantees:
        1. Delegates writing directly to the existing JsonlWriter without duplicating
           serialization logic or schema validation.
        2. Preserves line-delimited append-only file semantics.
        3. Propagates all file-system (JsonlWriterError) and serialization errors.
    """

    def __init__(
        self,
        writer: Optional[JsonlWriter] = None,
        output_path: Union[str, Path] = "data/telemetry.jsonl",
        validate_before_write: bool = True,
        auto_create_dir: bool = True,
    ) -> None:
        """Initialize the JSONL sink adapter.

        Args:
            writer: Optional existing JsonlWriter instance. If None, instantiates a new
                JsonlWriter using the provided output_path and options.
            output_path: Target file path (used only if writer is None).
            validate_before_write: If True, validates events against schema before write.
            auto_create_dir: If True, creates parent directories if missing.
        """
        if writer is not None:
            self._writer = writer
        else:
            self._writer = JsonlWriter(
                output_path=output_path,
                validate_before_write=validate_before_write,
                auto_create_dir=auto_create_dir,
            )
        self._is_closed = False

    @property
    def writer(self) -> JsonlWriter:
        """Return the underlying JsonlWriter instance."""
        return self._writer

    @property
    def output_path(self) -> Path:
        """Return the target JSONL file path."""
        return self._writer.output_path

    @property
    def is_closed(self) -> bool:
        """Return True if the sink has been closed."""
        return self._is_closed

    def write_event(self, event: SensorEvent) -> None:
        """Write a single SensorEvent to the JSONL file.

        Args:
            event: Canonical SensorEvent instance.

        Raises:
            JsonlWriterError: If the sink is closed or if an I/O error occurs.
            TypeError: If event is not a SensorEvent.
            EventSerializationError: If validation or JSON serialization fails.
        """
        if self._is_closed:
            raise JsonlWriterError("Cannot write event: JsonlSink is closed")
        if not isinstance(event, SensorEvent):
            raise TypeError(f"Expected SensorEvent, got {type(event).__name__}")

        self._writer.write_event(event)

    def write_events(self, events: Sequence[SensorEvent]) -> None:
        """Write a batch of SensorEvents to the JSONL file with atomic pre-validation.

        Args:
            events: Sequence of SensorEvent instances.

        Raises:
            JsonlWriterError: If the sink is closed or if an I/O error occurs.
            TypeError: If any item is not a SensorEvent.
            EventSerializationError: If any event fails validation.
        """
        if self._is_closed:
            raise JsonlWriterError("Cannot write events: JsonlSink is closed")
        for e in events:
            if not isinstance(e, SensorEvent):
                raise TypeError(f"Expected SensorEvent, got {type(e).__name__}")

        self._writer.write_events(events)

    def flush(self) -> None:
        """Flush the underlying file handle if open."""
        if self._writer._file_handle is not None and not self._writer._file_handle.closed:
            try:
                self._writer._file_handle.flush()
            except OSError as exc:
                raise JsonlWriterError(f"Failed to flush JSONL file: {exc}") from exc

    def close(self) -> None:
        """Flush and close the underlying file handle."""
        if self._is_closed:
            return
        self._is_closed = True
        if self._writer._file_handle is not None:
            handle = self._writer._file_handle
            self._writer._file_handle = None
            try:
                if not handle.closed:
                    handle.flush()
            finally:
                handle.close()

    def __enter__(self) -> "JsonlSink":
        """Enter context manager, opening file handle via underlying writer."""
        self._writer.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, flushing and closing file handle while preserving errors."""
        self._is_closed = True
        self._writer.__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self) -> str:
        return f"JsonlSink(path={self.output_path}, is_closed={self._is_closed})"


class KafkaSink(OutputSink):
    """OutputSink adapter wrapping a SensorEventKafkaProducer.

    Important Semantic Distinction:
        Calling write_event() asynchronously enqueues the message into the local
        producer client buffer and returns immediately. It does NOT block or guarantee
        broker delivery acknowledgement.
        Broker delivery acknowledgements are handled asynchronously via delivery callbacks
        and confirmed by calling flush().
    """

    def __init__(
        self,
        producer: Optional[SensorEventKafkaProducer] = None,
        config: Optional[KafkaConfig] = None,
        on_delivery: Optional[Callable[[DeliveryReport], None]] = None,
    ) -> None:
        """Initialize the Kafka sink adapter.

        Args:
            producer: Optional existing SensorEventKafkaProducer instance. If None,
                instantiates a new producer using the supplied config or defaults.
            config: Optional KafkaConfig instance (used only if producer is None).
            on_delivery: Optional default delivery callback for published messages.
        """
        if producer is not None:
            self._producer = producer
        else:
            self._producer = SensorEventKafkaProducer(config=config)
        self._on_delivery = on_delivery
        self._is_closed = False

    @property
    def producer(self) -> SensorEventKafkaProducer:
        """Return the underlying SensorEventKafkaProducer instance."""
        return self._producer

    @property
    def is_closed(self) -> bool:
        """Return True if the sink or underlying producer has been closed."""
        return self._is_closed or self._producer.is_closed

    def write_event(self, event: SensorEvent) -> None:
        """Publish a single SensorEvent asynchronously to Kafka.

        Args:
            event: Canonical SensorEvent instance.

        Raises:
            KafkaPublishError: If the sink is closed, or if serialization/key extraction fails.
            KafkaBackpressureError: If the producer buffer remains full after backpressure retries.
            TypeError: If event is not a SensorEvent.
        """
        if self._is_closed:
            raise KafkaPublishError("Cannot publish event: KafkaSink is closed")
        if not isinstance(event, SensorEvent):
            raise TypeError(f"Expected SensorEvent, got {type(event).__name__}")

        self._producer.publish_event(event, on_delivery=self._on_delivery)

    def write_events(self, events: Sequence[SensorEvent]) -> None:
        """Publish a batch of SensorEvents to Kafka with atomic in-memory pre-validation.

        Args:
            events: Sequence of SensorEvent instances.

        Raises:
            KafkaPublishError: If the sink is closed or an event fails validation.
            KafkaBackpressureError: If the producer queue fills during submission.
            TypeError: If any item is not a SensorEvent.
        """
        if self._is_closed:
            raise KafkaPublishError("Cannot publish events: KafkaSink is closed")
        for e in events:
            if not isinstance(e, SensorEvent):
                raise TypeError(f"Expected SensorEvent, got {type(e).__name__}")

        self._producer.publish_batch(events, on_delivery=self._on_delivery)

    def flush(self, timeout: Optional[float] = None, raise_on_timeout: bool = True) -> int:
        """Wait for all outstanding messages to be delivered and callbacks served.

        Args:
            timeout: Maximum seconds to wait.
            raise_on_timeout: If True, raises KafkaPublishError if messages remain in queue.

        Returns:
            Number of unsent messages remaining in queue (0 indicates full success).

        Raises:
            KafkaPublishError: If raise_on_timeout is True and queue does not drain.
        """
        return self._producer.flush(timeout=timeout, raise_on_timeout=raise_on_timeout)

    def close(self, timeout: Optional[float] = None, raise_on_timeout: bool = False) -> int:
        """Flush pending messages and mark the sink as closed."""
        if self._is_closed:
            return 0
        self._is_closed = True
        return self._producer.close(timeout=timeout, raise_on_timeout=raise_on_timeout)

    def __enter__(self) -> "KafkaSink":
        """Enter context manager."""
        self._producer.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, flushing and closing producer while preserving in-flight errors."""
        self._is_closed = True
        self._producer.__exit__(exc_type, exc_val, exc_tb)

    def __repr__(self) -> str:
        topic = getattr(self._producer.config, "topic", "unknown")
        return f"KafkaSink(topic={topic}, is_closed={self.is_closed})"


class CompositeOutputSink(OutputSink):
    """Coordinates dispatching the identical SensorEvent instance to multiple OutputSinks.

    Guarantees:
        1. Deterministic Order: Events are dispatched to sinks in the exact order specified.
        2. Zero Mutation / Regeneration: The exact same SensorEvent reference is forwarded to all sinks.
        3. Independent Sinks: Sinks maintain independent state, buffers, and metrics.

    Failure & Distributed Transaction Reality:
        CompositeOutputSink enforces FAIL_FAST by default. If Sink 1 succeeds and Sink 2 fails,
        the exception from Sink 2 propagates immediately. Sinks DO NOT participate in a distributed
        two-phase commit (2PC) or XA transaction; data written to Sink 1 is NOT rolled back.
    """

    def __init__(
        self,
        sinks: Sequence[OutputSink],
        fail_fast: bool = True,
    ) -> None:
        """Initialize the composite output sink.

        Args:
            sinks: Non-empty sequence of OutputSink instances.
            fail_fast: If True (default), an exception in any sink immediately propagates.

        Raises:
            ValueError: If sinks sequence is empty.
            TypeError: If any item in sinks is not an OutputSink instance.
        """
        if not sinks:
            raise ValueError("CompositeOutputSink requires at least one OutputSink instance")

        validated_sinks: List[OutputSink] = []
        for idx, sink in enumerate(sinks):
            if not isinstance(sink, OutputSink):
                raise TypeError(
                    f"Sink at index {idx} must be an instance of OutputSink, got {type(sink).__name__}"
                )
            validated_sinks.append(sink)

        self._sinks = tuple(validated_sinks)
        self._fail_fast = fail_fast
        self._is_closed = False

    @property
    def sinks(self) -> Sequence[OutputSink]:
        """Return the immutable sequence of configured child sinks."""
        return self._sinks

    @property
    def fail_fast(self) -> bool:
        """Return True if fail-fast error policy is active."""
        return self._fail_fast

    @property
    def is_closed(self) -> bool:
        """Return True if the composite sink has been closed."""
        return self._is_closed

    def write_event(self, event: SensorEvent) -> None:
        """Dispatch a single SensorEvent to all configured sinks in deterministic order.

        Args:
            event: Canonical SensorEvent instance.

        Raises:
            RuntimeError: If CompositeOutputSink is closed.
            TypeError: If event is not a SensorEvent.
            Exception: Propagates the exception from any failing sink when fail_fast is True.
        """
        if self._is_closed:
            raise RuntimeError("Cannot write event: CompositeOutputSink is closed")
        if not isinstance(event, SensorEvent):
            raise TypeError(f"Expected SensorEvent, got {type(event).__name__}")

        for sink in self._sinks:
            try:
                sink.write_event(event)
            except Exception as exc:
                logger.error("Error in sink %s: %s", type(sink).__name__, exc)
                if self._fail_fast:
                    raise

    def write_events(self, events: Sequence[SensorEvent]) -> None:
        """Dispatch a sequence of SensorEvents to all configured sinks in deterministic order.

        Args:
            events: Sequence of SensorEvent instances.

        Raises:
            RuntimeError: If CompositeOutputSink is closed.
            TypeError: If any item is not a SensorEvent.
            Exception: Propagates the exception from any failing sink when fail_fast is True.
        """
        if self._is_closed:
            raise RuntimeError("Cannot write events: CompositeOutputSink is closed")
        for e in events:
            if not isinstance(e, SensorEvent):
                raise TypeError(f"Expected SensorEvent, got {type(e).__name__}")

        for sink in self._sinks:
            try:
                sink.write_events(events)
            except Exception as exc:
                logger.error("Error in sink %s during batch write: %s", type(sink).__name__, exc)
                if self._fail_fast:
                    raise

    def flush(self) -> None:
        """Flush all configured sinks in order, ensuring all sinks attempt flush.

        Raises:
            Exception: The first exception encountered during flush, after all sinks have attempted flush.
        """
        errors: List[Exception] = []
        for sink in self._sinks:
            try:
                sink.flush()
            except Exception as exc:
                logger.error("Error flushing sink %s: %s", type(sink).__name__, exc)
                errors.append(exc)

        if errors:
            raise errors[0]

    def close(self) -> None:
        """Close all configured sinks in reverse order, ensuring all sinks attempt close.

        Raises:
            Exception: The first exception encountered during close, after all sinks have attempted close.
        """
        if self._is_closed:
            return
        self._is_closed = True

        errors: List[Exception] = []
        for sink in reversed(self._sinks):
            try:
                sink.close()
            except Exception as exc:
                logger.error("Error closing sink %s: %s", type(sink).__name__, exc)
                errors.append(exc)

        if errors:
            raise errors[0]

    def __enter__(self) -> "CompositeOutputSink":
        """Enter context manager, activating all sinks in order."""
        entered: List[OutputSink] = []
        try:
            for sink in self._sinks:
                sink.__enter__()
                entered.append(sink)
        except Exception:
            for s in reversed(entered):
                try:
                    s.__exit__(None, None, None)
                except Exception as cleanup_err:
                    logger.error("Error cleaning up sink %s: %s", type(s).__name__, cleanup_err)
            raise
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager, exiting all sinks in reverse order.

        Preserves the in-flight exception if one exists.
        """
        self._is_closed = True
        close_errors: List[Exception] = []

        for sink in reversed(self._sinks):
            try:
                sink.__exit__(exc_type, exc_val, exc_tb)
            except Exception as exc:
                logger.error("Error exiting sink %s: %s", type(sink).__name__, exc)
                close_errors.append(exc)

        # If an application exception occurred inside the with-block, preserve it without hiding
        if exc_type is not None:
            return

        if close_errors:
            raise close_errors[0]

    def __repr__(self) -> str:
        sinks_repr = ", ".join(repr(s) for s in self._sinks)
        return f"CompositeOutputSink(sinks=[{sinks_repr}], fail_fast={self._fail_fast})"
