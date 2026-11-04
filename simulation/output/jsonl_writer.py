"""JSON Lines (JSONL) Writer for AtmoSync sensor telemetry events.

Writes canonical Version-1 sensor events to an append-only JSONL file with UTF-8 encoding,
automatic directory creation, and strict pre-write validation.
"""

from io import TextIOWrapper
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

from simulation.events.event_schema import SensorEvent
from simulation.events.event_serializer import EventSerializationError, EventSerializer
from simulation.validation.event_validator import EventValidationError, EventValidator


class JsonlWriterError(IOError):
    """Raised when an unrecoverable file-system or writing error occurs."""
    pass


class JsonlWriter:
    """Appends validated SensorEvent instances to a JSON Lines file.

    Guarantees that:
    1. Each event occupies exactly one newline-delimited line.
    2. Records are written in valid UTF-8 encoding.
    3. Batches are pre-validated and serialized in memory before touching disk,
       guaranteeing zero records are written if any event fails schema validation.
       (Note: Does not claim OS-level transactional atomicity against hardware/storage
       I/O failures midway through physical disk writes).
    4. Destination directories are created automatically if requested.

    Attributes:
        output_path: Path to the destination .jsonl file.
        validate_before_write: Whether to validate events against the strict schema.
        auto_create_dir: Whether to automatically create parent directories.
    """

    def __init__(
        self,
        output_path: Union[str, Path] = "data/telemetry.jsonl",
        validate_before_write: bool = True,
        auto_create_dir: bool = True,
    ) -> None:
        """Initialize the JSONL writer.

        Args:
            output_path: Target path for JSONL output. Defaults to 'data/telemetry.jsonl'.
            validate_before_write: If True, validates each event against EventValidator before writing.
            auto_create_dir: If True, creates parent directories if they do not exist.
        """
        self._output_path = Path(output_path)
        self._validate_before_write = validate_before_write
        self._auto_create_dir = auto_create_dir
        self._file_handle: Optional[TextIOWrapper] = None

    @property
    def output_path(self) -> Path:
        """Return the destination file path."""
        return self._output_path

    @property
    def validate_before_write(self) -> bool:
        """Return True if pre-write validation is enabled."""
        return self._validate_before_write

    def _ensure_directory_exists(self) -> None:
        """Create parent directory if auto_create_dir is enabled."""
        if self._auto_create_dir:
            parent = self._output_path.parent
            if not parent.exists():
                try:
                    parent.mkdir(parents=True, exist_ok=True)
                except OSError as exc:
                    raise JsonlWriterError(
                        f"Failed to create directory '{parent}': {exc}"
                    ) from exc

    def _serialize_and_validate(self, event: Union[SensorEvent, Dict[str, Any]]) -> str:
        """Pre-validate and serialize an event to a single JSON line string."""
        if self._validate_before_write:
            try:
                EventValidator.validate(event)
            except EventValidationError as exc:
                raise EventSerializationError(f"Pre-write validation failed: {exc}") from exc

        return EventSerializer.serialize(event, validate=False)

    def write_event(self, event: Union[SensorEvent, Dict[str, Any]]) -> str:
        """Write a single event to the JSONL file.

        Args:
            event: SensorEvent or event dictionary to write.

        Returns:
            The serialized JSON string written to the file.

        Raises:
            EventSerializationError: If the event fails validation or serialization.
            JsonlWriterError: If a file-system or write error occurs.
        """
        # In-memory validation and serialization before touching disk
        line = self._serialize_and_validate(event)

        self._ensure_directory_exists()

        try:
            if self._file_handle is not None and not self._file_handle.closed:
                self._file_handle.write(f"{line}\n")
                self._file_handle.flush()
            else:
                with open(self._output_path, "a", encoding="utf-8") as fh:
                    fh.write(f"{line}\n")
                    fh.flush()
        except OSError as exc:
            raise JsonlWriterError(f"Failed to write event to '{self._output_path}': {exc}") from exc

        return line

    def write_events(self, events: Sequence[Union[SensorEvent, Dict[str, Any]]]) -> int:
        """Write a batch of events to the JSONL file.

        Pre-validates and serializes all events in the batch in memory first.
        If any event in the batch is invalid, an exception is raised immediately
        and zero records from this batch are written to disk.

        Args:
            events: Sequence of SensorEvent or event dictionaries.

        Returns:
            Number of events successfully written.

        Raises:
            EventSerializationError: If any event in the batch fails validation.
            JsonlWriterError: If a file-system or write error occurs.
        """
        if not events:
            return 0

        # Phase 1: In-memory pre-validation and serialization
        serialized_lines: List[str] = [self._serialize_and_validate(e) for e in events]

        # Phase 2: Atomic batch write
        self._ensure_directory_exists()

        payload = "".join(f"{line}\n" for line in serialized_lines)

        try:
            if self._file_handle is not None and not self._file_handle.closed:
                self._file_handle.write(payload)
                self._file_handle.flush()
            else:
                with open(self._output_path, "a", encoding="utf-8") as fh:
                    fh.write(payload)
                    fh.flush()
        except OSError as exc:
            raise JsonlWriterError(f"Failed to write batch to '{self._output_path}': {exc}") from exc

        return len(serialized_lines)

    def __enter__(self) -> "JsonlWriter":
        """Enter the context manager, keeping an open append file handle."""
        self._ensure_directory_exists()
        try:
            self._file_handle = open(self._output_path, "a", encoding="utf-8")
        except OSError as exc:
            raise JsonlWriterError(f"Failed to open '{self._output_path}': {exc}") from exc
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager, flushing and closing the open file handle.

        Ensures resources are safely closed on both normal and exceptional exits.
        If an exception is already in-flight from within the context block, it is preserved.
        """
        if self._file_handle is not None:
            handle = self._file_handle
            self._file_handle = None
            try:
                if not handle.closed:
                    try:
                        handle.flush()
                    finally:
                        handle.close()
            except OSError as exc:
                if exc_type is None:
                    raise JsonlWriterError(f"Failed to flush/close '{self._output_path}': {exc}") from exc
