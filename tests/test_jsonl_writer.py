"""Unit tests for JsonlWriter and JSONL output sink.

Verifies:
- Single-event and batch writing.
- One JSON object per line ending with newline (no array brackets).
- Append behavior across multiple invocations.
- Strict UTF-8 file encoding.
- In-memory pre-write validation ensuring atomic batch safety (no partial writes on invalid events).
- Automatic directory creation when parent directories do not exist.
- Safe handling of empty batches.
- Context manager support (__enter__ / __exit__).
- Error handling for I/O issues wrapped in JsonlWriterError.
"""

from pathlib import Path
from typing import List
import json
import uuid
import pytest

from simulation.events.event_schema import SensorEvent
from simulation.events.event_serializer import EventSerializationError
from simulation.output.jsonl_writer import JsonlWriter, JsonlWriterError
from simulation.validation.event_validator import EventValidator


@pytest.fixture
def sample_events() -> List[SensorEvent]:
    """Fixture providing a list of 3 valid SensorEvents across different sensor types."""
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
            container_id="CONTAINER_001",
            sensor_type="humidity",
            observed_at="2026-11-04T12:00:00.000Z",
            value=65.2,
            unit="percent",
        ),
        SensorEvent(
            schema_version=1,
            event_id=str(uuid.uuid4()),
            container_id="CONTAINER_001",
            sensor_type="vibration",
            observed_at="2026-11-04T12:00:00.000Z",
            value=0.045,
            unit="m_s2",
        ),
    ]


class TestJsonlWriterSingleAndBatch:
    """Tests for single and batch event writing."""

    def test_write_single_event(self, tmp_path: Path, sample_events: List[SensorEvent]) -> None:
        """Writing a single event produces a 1-line JSONL file."""
        output_file = tmp_path / "telemetry.jsonl"
        writer = JsonlWriter(output_path=output_file)

        written_line = writer.write_event(sample_events[0])
        assert isinstance(written_line, str)
        assert output_file.exists()

        content = output_file.read_text(encoding="utf-8")
        lines = content.splitlines()
        assert len(lines) == 1

        parsed = json.loads(lines[0])
        assert parsed["event_id"] == sample_events[0].event_id
        EventValidator.validate(parsed)

    def test_write_batch_events(self, tmp_path: Path, sample_events: List[SensorEvent]) -> None:
        """Writing a batch of N events produces exactly N newline-delimited lines."""
        output_file = tmp_path / "batch.jsonl"
        writer = JsonlWriter(output_path=output_file)

        count = writer.write_events(sample_events)
        assert count == 3

        content = output_file.read_text(encoding="utf-8")
        lines = content.splitlines()
        assert len(lines) == 3

        for i, line in enumerate(lines):
            parsed = json.loads(line)
            assert parsed["event_id"] == sample_events[i].event_id
            EventValidator.validate(parsed)

    def test_append_behavior(self, tmp_path: Path, sample_events: List[SensorEvent]) -> None:
        """Successive write operations append lines without overwriting prior records."""
        output_file = tmp_path / "append.jsonl"
        writer = JsonlWriter(output_path=output_file)

        # First write: 1 event
        writer.write_event(sample_events[0])
        # Second write: 2 events
        writer.write_events(sample_events[1:])

        content = output_file.read_text(encoding="utf-8")
        lines = content.splitlines()
        assert len(lines) == 3

        ids = [json.loads(line)["event_id"] for line in lines]
        assert ids == [e.event_id for e in sample_events]

    def test_write_empty_batch(self, tmp_path: Path) -> None:
        """Writing an empty batch returns 0 and creates no file."""
        output_file = tmp_path / "empty.jsonl"
        writer = JsonlWriter(output_path=output_file)

        count = writer.write_events([])
        assert count == 0
        assert not output_file.exists()


class TestJsonlWriterValidationAndAtomicity:
    """Tests for pre-write validation and atomic batch failure."""

    def test_write_event_rejects_invalid_event(self, tmp_path: Path) -> None:
        """Writing an invalid event raises EventSerializationError and creates no file."""
        output_file = tmp_path / "invalid.jsonl"
        writer = JsonlWriter(output_path=output_file)

        bad_event = {
            "schema_version": 999,  # invalid version
            "event_id": str(uuid.uuid4()),
            "container_id": "CONTAINER_001",
            "sensor_type": "temperature",
            "observed_at": "2026-11-04T12:00:00.000Z",
            "value": 4.25,
            "unit": "celsius",
        }

        with pytest.raises(EventSerializationError):
            writer.write_event(bad_event)

        assert not output_file.exists()

    def test_batch_atomic_validation_zero_partial_writes(
        self, tmp_path: Path, sample_events: List[SensorEvent]
    ) -> None:
        """If any event in a batch is invalid, none of the events are written."""
        output_file = tmp_path / "atomic.jsonl"
        writer = JsonlWriter(output_path=output_file)

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

        with pytest.raises(EventSerializationError):
            writer.write_events(batch)

        # File should not exist or be empty because batch was rejected in memory
        if output_file.exists():
            assert output_file.read_text(encoding="utf-8") == ""
        else:
            assert not output_file.exists()


class TestJsonlWriterDirectoryHandling:
    """Tests for directory creation and permissions."""

    def test_auto_create_nested_parent_directories(
        self, tmp_path: Path, sample_events: List[SensorEvent]
    ) -> None:
        """Nested parent directories are created automatically when auto_create_dir is True."""
        nested_output = tmp_path / "deeply" / "nested" / "dir" / "telemetry.jsonl"
        writer = JsonlWriter(output_path=nested_output, auto_create_dir=True)

        writer.write_event(sample_events[0])
        assert nested_output.exists()
        assert nested_output.parent.is_dir()

    def test_auto_create_dir_disabled_raises_when_dir_missing(
        self, tmp_path: Path, sample_events: List[SensorEvent]
    ) -> None:
        """When auto_create_dir is False and parent dir does not exist, JsonlWriterError is raised."""
        missing_dir_file = tmp_path / "non_existent_folder" / "telemetry.jsonl"
        writer = JsonlWriter(output_path=missing_dir_file, auto_create_dir=False)

        with pytest.raises(JsonlWriterError):
            writer.write_event(sample_events[0])


class TestJsonlWriterContextManager:
    """Tests for JsonlWriter context manager protocol."""

    def test_context_manager_usage(
        self, tmp_path: Path, sample_events: List[SensorEvent]
    ) -> None:
        """Using JsonlWriter as a context manager flushes and closes cleanly."""
        output_file = tmp_path / "context.jsonl"

        with JsonlWriter(output_path=output_file) as writer:
            assert writer._file_handle is not None
            assert not writer._file_handle.closed
            writer.write_event(sample_events[0])
            writer.write_events(sample_events[1:])

        # File handle should be closed and reset to None
        assert writer._file_handle is None

        content = output_file.read_text(encoding="utf-8")
        assert len(content.splitlines()) == 3

    def test_context_manager_exception_closes_handle_and_preserves_error(
        self, tmp_path: Path, sample_events: List[SensorEvent]
    ) -> None:
        """Context manager safely closes handle on exception and allows original exception to propagate."""
        output_file = tmp_path / "context_err.jsonl"
        writer_instance = None

        with pytest.raises(RuntimeError, match="Simulated crash inside with-block"):
            with JsonlWriter(output_path=output_file) as writer:
                writer_instance = writer
                writer.write_event(sample_events[0])
                raise RuntimeError("Simulated crash inside with-block")

        assert writer_instance is not None
        assert writer_instance._file_handle is None

    def test_utf8_encoding_preservation(self, tmp_path: Path) -> None:
        """UTF-8 multi-byte characters are encoded and preserved without corruption."""
        output_file = tmp_path / "utf8.jsonl"
        writer = JsonlWriter(output_path=output_file)

        event = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "container_id": "CONT_MÜNCHEN_001",
            "sensor_type": "temperature",
            "observed_at": "2026-11-04T12:00:00.000Z",
            "value": 2.5,
            "unit": "celsius",
        }
        writer.write_event(event)

        raw_bytes = output_file.read_bytes()
        # Verify valid UTF-8
        decoded_text = raw_bytes.decode("utf-8")
        assert "CONT_MÜNCHEN_001" in decoded_text

    def test_io_write_failure_raises_jsonl_writer_error(
        self, tmp_path: Path, sample_events: List[SensorEvent], monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Low-level OS/disk write errors are wrapped in JsonlWriterError."""
        output_file = tmp_path / "io_err.jsonl"
        writer = JsonlWriter(output_path=output_file)

        def mock_open(*args, **kwargs):
            raise OSError("Disk full or permission denied")

        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(JsonlWriterError, match="Failed to write event"):
            writer.write_event(sample_events[0])

    def test_properties(self, tmp_path: Path) -> None:
        """Test output_path and validate_before_write property getters."""
        p = tmp_path / "prop.jsonl"
        writer = JsonlWriter(output_path=p, validate_before_write=False)
        assert writer.output_path == p
        assert writer.validate_before_write is False
