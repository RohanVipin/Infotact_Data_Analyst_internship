"""Unit tests for AtmoSync Output Sink Architecture (Stage 6.1).

Verifies:
1. OutputSink abstract interface and default methods.
2. JsonlSink delegation, lifecycle, and error propagation.
3. KafkaSink delegation, delivery callback forwarding, lifecycle, and error propagation.
4. Single-event reuse across sinks without mutation or regeneration.
5. CompositeOutputSink deterministic dispatch order and fail-fast semantics.
6. Context manager cleanup and in-flight exception preservation.
7. Validation of empty or invalid sink configurations.
"""

from pathlib import Path
from unittest.mock import MagicMock, call
import uuid
import pytest

from simulation.events.event_schema import SensorEvent
from simulation.events.event_serializer import EventSerializer
from simulation.output.jsonl_writer import JsonlWriter, JsonlWriterError
from simulation.output.sink import (
    CompositeOutputSink,
    JsonlSink,
    KafkaSink,
    OutputSink,
)
from simulation.kafka.exceptions import (
    KafkaBackpressureError,
    KafkaPublishError,
)
from simulation.kafka.delivery_report import DeliveryReport


@pytest.fixture
def sample_event() -> SensorEvent:
    """Fixture providing a canonical SensorEvent instance."""
    return SensorEvent(
        schema_version=1,
        event_id=str(uuid.uuid4()),
        container_id="CONTAINER_001",
        sensor_type="temperature",
        observed_at="2026-11-05T12:00:00.123Z",
        value=4.25,
        unit="celsius",
    )


@pytest.fixture
def sample_events() -> list[SensorEvent]:
    """Fixture providing a list of multi-modal SensorEvents."""
    return [
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_001",
            sensor_type="temperature",
            observed_at="2026-11-05T12:00:00.100Z",
            value=4.25,
            unit="celsius",
        ),
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_001",
            sensor_type="humidity",
            observed_at="2026-11-05T12:00:00.200Z",
            value=65.2,
            unit="percent",
        ),
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_001",
            sensor_type="vibration",
            observed_at="2026-11-05T12:00:00.300Z",
            value=0.015,
            unit="m/s^2",
        ),
    ]


class TestOutputSinkBaseInterface:
    """Tests covering the abstract OutputSink interface and default behaviors."""

    def test_cannot_instantiate_abstract_sink(self) -> None:
        """Verify OutputSink cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class OutputSink"):
            OutputSink()  # type: ignore[abstract]

    def test_concrete_subclass_defaults(self, sample_event: SensorEvent) -> None:
        """Verify default implementations of write_events, flush, and close."""
        written: list[SensorEvent] = []

        class DummySink(OutputSink):
            def write_event(self, event: SensorEvent) -> None:
                written.append(event)

        sink = DummySink()
        sink.write_events([sample_event, sample_event])
        assert len(written) == 2
        assert written[0] is sample_event

        # Default flush and close do not fail
        sink.flush()
        sink.close()

    def test_context_manager_defaults(self, sample_event: SensorEvent) -> None:
        """Verify OutputSink context manager enters and closes."""
        close_called = False

        class DummySink(OutputSink):
            def write_event(self, event: SensorEvent) -> None:
                pass

            def close(self) -> None:
                nonlocal close_called
                close_called = True

        sink = DummySink()
        with sink as s:
            assert s is sink
            assert not close_called

        assert close_called is True


class TestJsonlSink:
    """Tests for JsonlSink adapter wrapping JsonlWriter."""

    def test_init_with_default_and_custom_path(self, tmp_path: Path) -> None:
        """Verify initialization with path creates a JsonlWriter."""
        target_file = tmp_path / "custom.jsonl"
        sink = JsonlSink(output_path=target_file)
        assert sink.output_path == target_file
        assert isinstance(sink.writer, JsonlWriter)
        assert not sink.is_closed

    def test_write_event_delegates_to_writer(self, sample_event: SensorEvent) -> None:
        """Verify write_event delegates directly to JsonlWriter."""
        mock_writer = MagicMock(spec=JsonlWriter)
        mock_writer.output_path = Path("mock.jsonl")
        sink = JsonlSink(writer=mock_writer)

        sink.write_event(sample_event)
        mock_writer.write_event.assert_called_once_with(sample_event)

    def test_write_events_batch_delegates_to_writer(self, sample_events: list[SensorEvent]) -> None:
        """Verify write_events delegates batch write to JsonlWriter."""
        mock_writer = MagicMock(spec=JsonlWriter)
        mock_writer.output_path = Path("mock.jsonl")
        sink = JsonlSink(writer=mock_writer)

        sink.write_events(sample_events)
        mock_writer.write_events.assert_called_once_with(sample_events)

    def test_write_event_rejects_non_sensor_event(self) -> None:
        """Verify non-SensorEvent is rejected with TypeError."""
        mock_writer = MagicMock(spec=JsonlWriter)
        sink = JsonlSink(writer=mock_writer)

        with pytest.raises(TypeError, match="Expected SensorEvent"):
            sink.write_event({"schema_version": 1})  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="Expected SensorEvent"):
            sink.write_events([{"schema_version": 1}])  # type: ignore[list-item]

    def test_error_propagation_from_writer(self, sample_event: SensorEvent) -> None:
        """Verify JsonlWriterError propagates immediately."""
        mock_writer = MagicMock(spec=JsonlWriter)
        mock_writer.write_event.side_effect = JsonlWriterError("Disk full")
        sink = JsonlSink(writer=mock_writer)

        with pytest.raises(JsonlWriterError, match="Disk full"):
            sink.write_event(sample_event)

    def test_close_and_post_close_rejection(self, sample_event: SensorEvent) -> None:
        """Verify closing sink closes file handle and rejects subsequent writes."""
        mock_writer = MagicMock(spec=JsonlWriter)
        mock_handle = MagicMock()
        mock_handle.closed = False
        mock_writer._file_handle = mock_handle
        mock_writer.output_path = Path("mock.jsonl")

        sink = JsonlSink(writer=mock_writer)
        sink.flush()
        mock_handle.flush.assert_called_once()

        sink.close()
        assert sink.is_closed is True
        mock_handle.close.assert_called_once()

        # Repeated close is safe no-op
        sink.close()

        with pytest.raises(JsonlWriterError, match="JsonlSink is closed"):
            sink.write_event(sample_event)

        with pytest.raises(JsonlWriterError, match="JsonlSink is closed"):
            sink.write_events([sample_event])

    def test_context_manager_lifecycle(self, tmp_path: Path, sample_event: SensorEvent) -> None:
        """Verify JsonlSink context manager writes real data to disk."""
        target_file = tmp_path / "telemetry.jsonl"
        with JsonlSink(output_path=target_file) as sink:
            sink.write_event(sample_event)

        assert target_file.is_file()
        content = target_file.read_text(encoding="utf-8").strip()
        deserialized = EventSerializer.deserialize(content)
        assert deserialized.event_id == sample_event.event_id
        assert deserialized.value == sample_event.value


class TestKafkaSink:
    """Tests for KafkaSink adapter wrapping SensorEventKafkaProducer."""

    def test_init_with_producer(self) -> None:
        """Verify KafkaSink wraps injected producer."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        sink = KafkaSink(producer=mock_producer)
        assert sink.producer is mock_producer
        assert not sink.is_closed

    def test_write_event_delegates_to_producer(self, sample_event: SensorEvent) -> None:
        """Verify write_event delegates to producer.publish_event."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        callback = MagicMock()

        sink = KafkaSink(producer=mock_producer, on_delivery=callback)
        sink.write_event(sample_event)

        mock_producer.publish_event.assert_called_once_with(sample_event, on_delivery=callback)

    def test_write_events_batch_delegates_to_producer(self, sample_events: list[SensorEvent]) -> None:
        """Verify write_events delegates to producer.publish_batch."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        sink = KafkaSink(producer=mock_producer)

        sink.write_events(sample_events)
        mock_producer.publish_batch.assert_called_once_with(sample_events, on_delivery=None)

    def test_write_event_rejects_non_sensor_event(self) -> None:
        """Verify non-SensorEvent is rejected with TypeError."""
        mock_producer = MagicMock()
        sink = KafkaSink(producer=mock_producer)

        with pytest.raises(TypeError, match="Expected SensorEvent"):
            sink.write_event({"key": "val"})  # type: ignore[arg-type]

        with pytest.raises(TypeError, match="Expected SensorEvent"):
            sink.write_events([{"key": "val"}])  # type: ignore[list-item]

    def test_kafka_publish_error_propagation(self, sample_event: SensorEvent) -> None:
        """Verify KafkaPublishError propagates immediately."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        mock_producer.publish_event.side_effect = KafkaPublishError("Serialization error")
        sink = KafkaSink(producer=mock_producer)

        with pytest.raises(KafkaPublishError, match="Serialization error"):
            sink.write_event(sample_event)

    def test_kafka_backpressure_error_propagation(self, sample_event: SensorEvent) -> None:
        """Verify KafkaBackpressureError propagates immediately."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        mock_producer.publish_event.side_effect = KafkaBackpressureError("Queue full")
        sink = KafkaSink(producer=mock_producer)

        with pytest.raises(KafkaBackpressureError, match="Queue full"):
            sink.write_event(sample_event)

    def test_flush_and_close_delegation(self) -> None:
        """Verify flush and close delegate to producer with arguments."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        mock_producer.flush.return_value = 0

        sink = KafkaSink(producer=mock_producer)
        rem = sink.flush(timeout=5.0, raise_on_timeout=True)
        assert rem == 0
        mock_producer.flush.assert_called_once_with(timeout=5.0, raise_on_timeout=True)

        sink.close(timeout=2.0, raise_on_timeout=False)
        assert sink.is_closed is True
        mock_producer.close.assert_called_once_with(timeout=2.0, raise_on_timeout=False)

    def test_post_close_write_rejected(self, sample_event: SensorEvent) -> None:
        """Verify publishing after close raises KafkaPublishError."""
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        sink = KafkaSink(producer=mock_producer)
        sink.close()

        with pytest.raises(KafkaPublishError, match="KafkaSink is closed"):
            sink.write_event(sample_event)

        with pytest.raises(KafkaPublishError, match="KafkaSink is closed"):
            sink.write_events([sample_event])


class TestCompositeOutputSink:
    """Tests for CompositeOutputSink coordinator dispatching to multiple sinks."""

    def test_empty_sinks_rejected(self) -> None:
        """Verify initializing with empty sequence raises ValueError."""
        with pytest.raises(ValueError, match="requires at least one OutputSink"):
            CompositeOutputSink(sinks=[])

    def test_invalid_sink_type_rejected(self) -> None:
        """Verify non-OutputSink item in sequence raises TypeError."""
        with pytest.raises(TypeError, match="must be an instance of OutputSink"):
            CompositeOutputSink(sinks=["invalid_sink"])  # type: ignore[list-item]

    def test_single_event_reference_reused_without_mutation(self, sample_event: SensorEvent) -> None:
        """Verify exact same SensorEvent instance reference is passed to both sinks."""
        received_sink1: list[SensorEvent] = []
        received_sink2: list[SensorEvent] = []

        class TrackingSink1(OutputSink):
            def write_event(self, event: SensorEvent) -> None:
                received_sink1.append(event)

        class TrackingSink2(OutputSink):
            def write_event(self, event: SensorEvent) -> None:
                received_sink2.append(event)

        composite = CompositeOutputSink(sinks=[TrackingSink1(), TrackingSink2()])
        composite.write_event(sample_event)

        assert len(received_sink1) == 1
        assert len(received_sink2) == 1
        # Critical assertion: Identical object identity (no re-generation, no copying)
        assert received_sink1[0] is sample_event
        assert received_sink2[0] is sample_event
        assert received_sink1[0] is received_sink2[0]

    def test_deterministic_dispatch_order(self, sample_event: SensorEvent) -> None:
        """Verify sinks are called in the exact order specified in configuration."""
        call_log: list[str] = []

        class LoggingSink(OutputSink):
            def __init__(self, name: str) -> None:
                self.name = name

            def write_event(self, event: SensorEvent) -> None:
                call_log.append(f"write:{self.name}")

        sink_a = LoggingSink("A")
        sink_b = LoggingSink("B")
        sink_c = LoggingSink("C")

        composite = CompositeOutputSink(sinks=[sink_a, sink_b, sink_c])
        composite.write_event(sample_event)

        assert call_log == ["write:A", "write:B", "write:C"]

    def test_batch_write_events_deterministic_order(self, sample_events: list[SensorEvent]) -> None:
        """Verify write_events dispatches batch in deterministic order."""
        call_log: list[str] = []

        class LoggingSink(OutputSink):
            def __init__(self, name: str) -> None:
                self.name = name

            def write_event(self, event: SensorEvent) -> None:
                pass

            def write_events(self, events: list[SensorEvent]) -> None:
                call_log.append(f"batch:{self.name}:{len(events)}")

        composite = CompositeOutputSink(sinks=[LoggingSink("JSONL"), LoggingSink("Kafka")])
        composite.write_events(sample_events)

        assert call_log == ["batch:JSONL:3", "batch:Kafka:3"]

    def test_fail_fast_first_sink_error_stops_subsequent_sinks(self, sample_event: SensorEvent) -> None:
        """Verify that when Sink 1 fails, Sink 2 is never called and error propagates."""
        sink1 = MagicMock(spec=OutputSink)
        sink1.write_event.side_effect = JsonlWriterError("Disk write failed")

        sink2 = MagicMock(spec=OutputSink)

        composite = CompositeOutputSink(sinks=[sink1, sink2], fail_fast=True)

        with pytest.raises(JsonlWriterError, match="Disk write failed"):
            composite.write_event(sample_event)

        sink1.write_event.assert_called_once_with(sample_event)
        sink2.write_event.assert_not_called()

    def test_fail_fast_second_sink_error_leaves_first_sink_written(self, sample_event: SensorEvent) -> None:
        """Verify that when Sink 1 succeeds and Sink 2 fails, Sink 2 error propagates.

        Confirms the documented reality that there is no cross-sink distributed transaction rollback.
        """
        sink1 = MagicMock(spec=OutputSink)
        sink2 = MagicMock(spec=OutputSink)
        sink2.write_event.side_effect = KafkaPublishError("Broker unreachable")

        composite = CompositeOutputSink(sinks=[sink1, sink2], fail_fast=True)

        with pytest.raises(KafkaPublishError, match="Broker unreachable"):
            composite.write_event(sample_event)

        # Sink 1 succeeded before Sink 2 failed
        sink1.write_event.assert_called_once_with(sample_event)
        sink2.write_event.assert_called_once_with(sample_event)

    def test_flush_delegates_to_all_sinks_and_raises_first_error(self) -> None:
        """Verify flush calls flush on all sinks and propagates errors."""
        sink1 = MagicMock(spec=OutputSink)
        sink1.flush.side_effect = OSError("Flush 1 error")
        sink2 = MagicMock(spec=OutputSink)

        composite = CompositeOutputSink(sinks=[sink1, sink2])

        with pytest.raises(OSError, match="Flush 1 error"):
            composite.flush()

        sink1.flush.assert_called_once()
        # All sinks attempted flush even if an earlier one failed
        sink2.flush.assert_called_once()

    def test_close_delegates_in_reverse_order_and_raises_first_error(self) -> None:
        """Verify close calls close in reverse order and propagates errors."""
        order_log: list[str] = []

        class OrderTrackingSink(OutputSink):
            def __init__(self, name: str, raise_err: bool = False) -> None:
                self.name = name
                self.raise_err = raise_err

            def write_event(self, event: SensorEvent) -> None:
                pass

            def close(self) -> None:
                order_log.append(self.name)
                if self.raise_err:
                    raise RuntimeError(f"Error in {self.name}")

        sink1 = OrderTrackingSink("Sink1", raise_err=False)
        sink2 = OrderTrackingSink("Sink2", raise_err=True)

        composite = CompositeOutputSink(sinks=[sink1, sink2])

        with pytest.raises(RuntimeError, match="Error in Sink2"):
            composite.close()

        # Closed in reverse order: Sink2 closed first, then Sink1
        assert order_log == ["Sink2", "Sink1"]
        assert composite.is_closed is True

    def test_context_manager_preserves_inflight_exception(self, sample_event: SensorEvent) -> None:
        """Verify context manager does not mask in-flight application exception even if close fails."""
        class FlakyCloseSink(OutputSink):
            def write_event(self, event: SensorEvent) -> None:
                pass

            def close(self) -> None:
                raise RuntimeError("Cleanup failure")

        composite = CompositeOutputSink(sinks=[FlakyCloseSink()])

        with pytest.raises(ValueError, match="In-flight application error"):
            with composite:
                raise ValueError("In-flight application error")

        assert composite.is_closed is True

    def test_context_manager_normal_cleanup_raises_if_close_fails(self) -> None:
        """Verify that if no in-flight exception exists, cleanup failure is raised."""
        class FlakyCloseSink(OutputSink):
            def write_event(self, event: SensorEvent) -> None:
                pass

            def close(self) -> None:
                raise RuntimeError("Close failed on normal exit")

        composite = CompositeOutputSink(sinks=[FlakyCloseSink()])

        with pytest.raises(RuntimeError, match="Close failed on normal exit"):
            with composite:
                pass

    def test_post_close_write_rejected(self, sample_event: SensorEvent) -> None:
        """Verify writing after composite sink is closed raises RuntimeError."""
        sink = MagicMock(spec=OutputSink)
        composite = CompositeOutputSink(sinks=[sink])
        composite.close()

        with pytest.raises(RuntimeError, match="CompositeOutputSink is closed"):
            composite.write_event(sample_event)

        with pytest.raises(RuntimeError, match="CompositeOutputSink is closed"):
            composite.write_events([sample_event])
