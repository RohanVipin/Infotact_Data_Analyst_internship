"""JSON Event Serializer for AtmoSync sensor telemetry events.

Provides canonical, deterministic serialization and deserialization of SensorEvent instances.
Enforces the 7-field Version-1 event contract and strictly rejects non-finite values (NaN, Infinity).
"""

import json
import math
from typing import Any, Dict, Sequence, Union

from simulation.events.event_schema import SCHEMA_VERSION, SUPPORTED_SENSOR_TYPES, SensorEvent
from simulation.validation.event_validator import EventValidationError, EventValidator

CANONICAL_FIELD_ORDER: Sequence[str] = (
    "schema_version",
    "event_id",
    "container_id",
    "sensor_type",
    "observed_at",
    "value",
    "unit",
)


class EventSerializationError(ValueError):
    """Raised when an event cannot be serialized or deserialized."""
    pass


class EventSerializer:
    """Serializes SensorEvent instances to JSON and deserializes JSON strings back to SensorEvent."""

    @classmethod
    def serialize(
        cls,
        event: Union[SensorEvent, Dict[str, Any]],
        validate: bool = True,
    ) -> str:
        """Serialize a SensorEvent or dictionary to a deterministic JSON string.

        Args:
            event: SensorEvent instance or event dictionary.
            validate: If True, validates event against strict v1 schema before serialization.

        Returns:
            Deterministic JSON string representing the canonical event.

        Raises:
            EventSerializationError: If the event contains non-finite numbers or violates schema.
        """
        if isinstance(event, SensorEvent):
            raw_dict = event.to_dict()
        elif isinstance(event, dict):
            raw_dict = event
        else:
            raise EventSerializationError(
                f"Expected SensorEvent or dict, got {type(event).__name__}"
            )

        if validate:
            try:
                EventValidator.validate(raw_dict)
            except EventValidationError as exc:
                raise EventSerializationError(f"Cannot serialize invalid event: {exc}") from exc

        # Construct dictionary adhering strictly to canonical field ordering
        ordered_data = {key: raw_dict[key] for key in CANONICAL_FIELD_ORDER if key in raw_dict}
        # Retain any extra keys at the end when validate=False so data is not silently discarded
        for key, val in raw_dict.items():
            if key not in ordered_data:
                ordered_data[key] = val

        try:
            # allow_nan=False strictly raises ValueError on NaN, +inf, -inf
            return json.dumps(
                ordered_data,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ": "),
            )
        except (ValueError, TypeError) as exc:
            raise EventSerializationError(f"JSON serialization failed: {exc}") from exc

    @classmethod
    def deserialize(
        cls,
        json_str: str,
        validate: bool = True,
    ) -> SensorEvent:
        """Deserialize a JSON string into a validated SensorEvent instance.

        Args:
            json_str: JSON formatted string.
            validate: If True, validates against strict v1 schema.

        Returns:
            Validated SensorEvent instance.

        Raises:
            EventSerializationError: If parsing fails, fields are missing, or schema is violated.
        """
        if not isinstance(json_str, str) or not json_str.strip():
            raise EventSerializationError("Cannot deserialize empty or non-string input")

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as exc:
            raise EventSerializationError(f"Malformed JSON: {exc}") from exc

        if not isinstance(data, dict):
            raise EventSerializationError(f"Expected JSON object, got {type(data).__name__}")

        if validate:
            try:
                EventValidator.validate(data)
            except EventValidationError as exc:
                raise EventSerializationError(f"Deserialized event failed validation: {exc}") from exc

        try:
            val = data["value"]
            if isinstance(val, bool) or not isinstance(val, (int, float)):
                raise EventSerializationError(f"Field 'value' must be numeric float/int, got {type(val).__name__}")
            if not math.isfinite(val):
                raise EventSerializationError(f"Field 'value' must be finite number, got {val}")

            return SensorEvent(
                schema_version=int(data["schema_version"]),
                event_id=str(data["event_id"]),
                container_id=str(data["container_id"]),
                sensor_type=str(data["sensor_type"]),
                observed_at=str(data["observed_at"]),
                value=float(val),
                unit=str(data["unit"]),
            )
        except KeyError as exc:
            raise EventSerializationError(f"Missing required field in JSON event: {exc}") from exc
        except (ValueError, TypeError) as exc:
            raise EventSerializationError(f"Failed to construct SensorEvent from JSON: {exc}") from exc


def serialize_event(event: Union[SensorEvent, Dict[str, Any]], validate: bool = True) -> str:
    """Convenience functional interface to serialize an event."""
    return EventSerializer.serialize(event, validate=validate)


def deserialize_event(json_str: str, validate: bool = True) -> SensorEvent:
    """Convenience functional interface to deserialize a JSON event string."""
    return EventSerializer.deserialize(json_str, validate=validate)
