"""Validation engine for canonical Version-1 AtmoSync sensor events.

Enforces strict field structure, type constraints, UUID compliance, UTC timestamp
timezone-awareness, and finite numeric values.
"""

from datetime import datetime, timedelta
import math
from typing import Any, Dict, FrozenSet, Union
import uuid

from simulation.events.event_schema import SCHEMA_VERSION, SUPPORTED_SENSOR_TYPES, SensorEvent


class EventValidationError(ValueError):
    """Raised when an event payload fails validation against the canonical contract."""
    pass


class EventValidator:
    """Validates individual event dictionaries or SensorEvent instances against the version-1 schema."""

    EXACT_REQUIRED_FIELDS: FrozenSet[str] = frozenset({
        "schema_version",
        "event_id",
        "container_id",
        "sensor_type",
        "observed_at",
        "value",
        "unit",
    })

    @classmethod
    def validate(cls, event_data: Union[Dict[str, Any], SensorEvent]) -> None:
        """Validate an event against the strict version-1 contract.

        Args:
            event_data: A dictionary representing the event payload or a SensorEvent instance.

        Raises:
            EventValidationError: If any contract rule is violated.
        """
        if isinstance(event_data, SensorEvent):
            data = event_data.to_dict()
        elif isinstance(event_data, dict):
            data = event_data
        else:
            raise EventValidationError(
                f"Event must be a dict or SensorEvent instance, got {type(event_data).__name__}"
            )

        # 1. Enforce strict field presence and reject unexpected fields
        actual_keys = set(data.keys())
        missing_fields = cls.EXACT_REQUIRED_FIELDS - actual_keys
        if missing_fields:
            missing_sorted = sorted(list(missing_fields))
            raise EventValidationError(f"Missing required field(s): {', '.join(missing_sorted)}")

        unexpected_fields = actual_keys - cls.EXACT_REQUIRED_FIELDS
        if unexpected_fields:
            unexpected_sorted = sorted(list(unexpected_fields))
            raise EventValidationError(f"Unexpected field(s) in strict v1 contract: {', '.join(unexpected_sorted)}")

        # 2. Validate schema_version
        version = data["schema_version"]
        if isinstance(version, bool) or not isinstance(version, int):
            raise EventValidationError(
                f"'schema_version' must be an integer, got {type(version).__name__}"
            )
        if version != SCHEMA_VERSION:
            raise EventValidationError(
                f"Unsupported 'schema_version': expected {SCHEMA_VERSION}, got {version}"
            )

        # 3. Validate event_id (must be a valid UUIDv4)
        event_id = data["event_id"]
        if not isinstance(event_id, str) or not event_id.strip():
            raise EventValidationError(
                f"'event_id' must be a non-empty string, got {repr(event_id)}"
            )
        try:
            parsed_uuid = uuid.UUID(event_id.strip())
            # Ensure the string representation matches standard UUID format
            if str(parsed_uuid) != event_id.strip().lower():
                raise EventValidationError(
                    f"'event_id' must be a canonically formatted UUID string, got {repr(event_id)}"
                )
            if parsed_uuid.version != 4:
                raise EventValidationError(
                    f"'event_id' must be a UUID version 4, got version {parsed_uuid.version}"
                )
        except (ValueError, AttributeError) as exc:
            raise EventValidationError(
                f"'event_id' is not a valid UUID: {exc}"
            ) from exc

        # 4. Validate container_id
        container_id = data["container_id"]
        if not isinstance(container_id, str) or not container_id.strip():
            raise EventValidationError(
                f"'container_id' must be a non-empty string, got {repr(container_id)}"
            )

        # 5. Validate sensor_type
        sensor_type = data["sensor_type"]
        if not isinstance(sensor_type, str) or sensor_type not in SUPPORTED_SENSOR_TYPES:
            raise EventValidationError(
                f"'sensor_type' must be one of {sorted(list(SUPPORTED_SENSOR_TYPES))}, got {repr(sensor_type)}"
            )

        # 6. Validate observed_at (must be valid, timezone-aware UTC ISO-8601)
        observed_at = data["observed_at"]
        if not isinstance(observed_at, str) or not observed_at.strip():
            raise EventValidationError(
                f"'observed_at' must be a non-empty ISO-8601 string, got {repr(observed_at)}"
            )
        ts_str = observed_at.strip()
        # Normalise 'Z' suffix to '+00:00' for python fromisoformat
        normalized_ts = ts_str.replace("Z", "+00:00") if ts_str.endswith("Z") else ts_str
        try:
            parsed_dt = datetime.fromisoformat(normalized_ts)
        except ValueError as exc:
            raise EventValidationError(
                f"'observed_at' is not a valid ISO-8601 timestamp: {exc}"
            ) from exc

        if parsed_dt.tzinfo is None:
            raise EventValidationError(
                f"'observed_at' must be timezone-aware UTC, got naive timestamp: {repr(observed_at)}"
            )

        utcoffset = parsed_dt.utcoffset()
        if utcoffset is None or utcoffset != timedelta(0):
            raise EventValidationError(
                f"'observed_at' must have a UTC (+00:00 or 'Z') offset, got offset {utcoffset}"
            )

        # 7. Validate value (must be finite numeric, rejecting bools, NaN, and Infs)
        value = data["value"]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise EventValidationError(
                f"'value' must be a numeric float/int (excluding bool), got {type(value).__name__}"
            )
        if not math.isfinite(value):
            raise EventValidationError(
                f"'value' must be a finite number, got {repr(value)}"
            )

        # 8. Validate unit (non-empty string)
        unit = data["unit"]
        if not isinstance(unit, str) or not unit.strip():
            raise EventValidationError(
                f"'unit' must be a non-empty string, got {repr(unit)}"
            )

    @classmethod
    def is_valid(cls, event_data: Union[Dict[str, Any], SensorEvent]) -> bool:
        """Check whether an event is valid without raising an exception.

        Args:
            event_data: Event dictionary or SensorEvent instance.

        Returns:
            True if valid, False otherwise.
        """
        try:
            cls.validate(event_data)
            return True
        except EventValidationError:
            return False
