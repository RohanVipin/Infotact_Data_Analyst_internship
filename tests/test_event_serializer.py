"""Unit tests for EventSerializer and JSON serialization of AtmoSync events.

Verifies:
- Canonical 7-field serialization adhering to Version-1 schema.
- Deterministic field order.
- Numeric type preservation (no strings for numeric types).
- Strict rejection of NaN, +inf, -inf.
- Robust deserialization into SensorEvent.
- Error handling for malformed JSON, empty inputs, non-object JSON, and schema violations.
"""

import json
import math
import uuid
from datetime import datetime, timezone
import pytest

from simulation.events.event_schema import SensorEvent
from simulation.events.event_serializer import (
    CANONICAL_FIELD_ORDER,
    EventSerializationError,
    EventSerializer,
    deserialize_event,
    serialize_event,
)


@pytest.fixture
def sample_valid_event() -> SensorEvent:
    """Fixture providing a known-valid SensorEvent."""
    return SensorEvent(
        schema_version=1,
        event_id=str(uuid.uuid4()),
        container_id="CONTAINER_001",
        sensor_type="temperature",
        observed_at="2026-11-04T12:00:00.000Z",
        value=4.25,
        unit="celsius",
    )


@pytest.fixture
def sample_valid_dict(sample_valid_event: SensorEvent) -> dict:
    """Fixture providing a known-valid event dictionary."""
    return sample_valid_event.to_dict()


class TestEventSerializer:
    """Tests for EventSerializer.serialize."""

    def test_serialize_valid_sensor_event(self, sample_valid_event: SensorEvent) -> None:
        """Serializing a SensorEvent produces valid JSON with all 7 fields."""
        serialized = EventSerializer.serialize(sample_valid_event)
        assert isinstance(serialized, str)

        parsed = json.loads(serialized)
        assert len(parsed) == 7
        assert parsed["schema_version"] == 1
        assert parsed["event_id"] == sample_valid_event.event_id
        assert parsed["container_id"] == "CONTAINER_001"
        assert parsed["sensor_type"] == "temperature"
        assert parsed["observed_at"] == "2026-11-04T12:00:00.000Z"
        assert parsed["value"] == 4.25
        assert parsed["unit"] == "celsius"

    def test_serialize_valid_dict(self, sample_valid_dict: dict) -> None:
        """Serializing a valid dictionary produces valid canonical JSON."""
        serialized = EventSerializer.serialize(sample_valid_dict)
        parsed = json.loads(serialized)
        assert parsed == sample_valid_dict

    def test_canonical_field_ordering(self, sample_valid_event: SensorEvent) -> None:
        """Serialized JSON string must preserve the canonical 7-field order exactly."""
        serialized = EventSerializer.serialize(sample_valid_event)
        parsed_keys = list(json.loads(serialized).keys())
        assert parsed_keys == list(CANONICAL_FIELD_ORDER)

        # Also check substring ordering in raw JSON string
        positions = [serialized.index(f'"{field}"') for field in CANONICAL_FIELD_ORDER]
        assert positions == sorted(positions), "Fields do not appear in canonical order in raw JSON"

    def test_numeric_type_preservation(self, sample_valid_event: SensorEvent) -> None:
        """Numeric fields (schema_version, value) must not be converted to strings."""
        serialized = EventSerializer.serialize(sample_valid_event)
        parsed = json.loads(serialized)

        assert isinstance(parsed["schema_version"], int)
        assert not isinstance(parsed["schema_version"], bool)
        assert isinstance(parsed["value"], (int, float))
        assert not isinstance(parsed["value"], str)

        # In raw JSON, value should not be wrapped in quotes
        assert '"value": 4.25' in serialized or '"value":4.25' in serialized

    @pytest.mark.parametrize(
        "bad_value",
        [float("nan"), float("inf"), float("-inf")],
    )
    def test_serialize_rejects_non_finite_floats(
        self, sample_valid_dict: dict, bad_value: float
    ) -> None:
        """Serializing non-finite floats (NaN, +/-Inf) must raise EventSerializationError."""
        invalid_dict = sample_valid_dict.copy()
        invalid_dict["value"] = bad_value

        with pytest.raises(EventSerializationError):
            EventSerializer.serialize(invalid_dict, validate=True)

        # Even with validate=False, allow_nan=False in json.dumps must reject it
        with pytest.raises(EventSerializationError, match="JSON serialization failed"):
            EventSerializer.serialize(invalid_dict, validate=False)

    def test_serialize_rejects_invalid_type(self) -> None:
        """Passing non-dict, non-SensorEvent raises EventSerializationError."""
        with pytest.raises(EventSerializationError, match="Expected SensorEvent or dict"):
            EventSerializer.serialize(["not", "an", "event"])  # type: ignore[arg-type]

        with pytest.raises(EventSerializationError, match="Expected SensorEvent or dict"):
            EventSerializer.serialize(123)  # type: ignore[arg-type]

    def test_serialize_schema_validation_failure(self, sample_valid_dict: dict) -> None:
        """Schema violations trigger EventSerializationError when validate=True."""
        bad_dict = sample_valid_dict.copy()
        bad_dict["event_id"] = "not-a-uuid"

        with pytest.raises(EventSerializationError, match="Cannot serialize invalid event"):
            EventSerializer.serialize(bad_dict, validate=True)

    def test_serialize_unexpected_field_rejected(self, sample_valid_dict: dict) -> None:
        """Unexpected fields trigger EventSerializationError when validate=True."""
        bad_dict = sample_valid_dict.copy()
        bad_dict["unexpected_telemetry_key"] = "forbidden"

        with pytest.raises(EventSerializationError, match="Cannot serialize invalid event"):
            EventSerializer.serialize(bad_dict, validate=True)

    def test_serialize_without_validation_if_requested(self, sample_valid_dict: dict) -> None:
        """validate=False skips schema validation and preserves extra fields."""
        dict_with_extra = sample_valid_dict.copy()
        dict_with_extra["extra_custom_tag"] = "audit_stage_4"
        serialized = EventSerializer.serialize(dict_with_extra, validate=False)
        assert isinstance(serialized, str)
        assert '"extra_custom_tag": "audit_stage_4"' in serialized


class TestEventDeserializer:
    """Tests for EventSerializer.deserialize."""

    def test_deserialize_valid_json(self, sample_valid_event: SensorEvent) -> None:
        """Deserializing valid JSON produces an identical SensorEvent."""
        serialized = EventSerializer.serialize(sample_valid_event)
        deserialized = EventSerializer.deserialize(serialized)

        assert isinstance(deserialized, SensorEvent)
        assert deserialized.schema_version == sample_valid_event.schema_version
        assert deserialized.event_id == sample_valid_event.event_id
        assert deserialized.container_id == sample_valid_event.container_id
        assert deserialized.sensor_type == sample_valid_event.sensor_type
        assert deserialized.observed_at == sample_valid_event.observed_at
        assert deserialized.value == sample_valid_event.value
        assert deserialized.unit == sample_valid_event.unit

    @pytest.mark.parametrize(
        "empty_input",
        ["", "   ", "\n", "\t"],
    )
    def test_deserialize_empty_or_whitespace_raises(self, empty_input: str) -> None:
        """Empty or whitespace-only strings raise EventSerializationError."""
        with pytest.raises(EventSerializationError, match="Cannot deserialize empty"):
            EventSerializer.deserialize(empty_input)

    def test_deserialize_non_string_raises(self) -> None:
        """Passing non-string raises EventSerializationError."""
        with pytest.raises(EventSerializationError, match="Cannot deserialize empty or non-string"):
            EventSerializer.deserialize(None)  # type: ignore[arg-type]

    def test_deserialize_malformed_json_raises(self) -> None:
        """Malformed JSON raises EventSerializationError."""
        with pytest.raises(EventSerializationError, match="Malformed JSON"):
            EventSerializer.deserialize("{invalid: json}")

    @pytest.mark.parametrize(
        "non_object_json",
        ["[1, 2, 3]", '"just a string"', "12345", "true"],
    )
    def test_deserialize_non_dict_json_raises(self, non_object_json: str) -> None:
        """JSON that parses to a non-dict raises EventSerializationError."""
        with pytest.raises(EventSerializationError, match="Expected JSON object"):
            EventSerializer.deserialize(non_object_json)

    def test_deserialize_schema_violation_raises(self, sample_valid_dict: dict) -> None:
        """Valid JSON with invalid event schema raises EventSerializationError."""
        invalid_dict = sample_valid_dict.copy()
        invalid_dict["sensor_type"] = "unsupported_radiation_sensor"

        json_str = json.dumps(invalid_dict)
        with pytest.raises(EventSerializationError, match="Deserialized event failed validation"):
            EventSerializer.deserialize(json_str, validate=True)

    def test_deserialize_unexpected_field_raises(self, sample_valid_dict: dict) -> None:
        """Valid JSON with unexpected fields raises EventSerializationError."""
        invalid_dict = sample_valid_dict.copy()
        invalid_dict["rogue_field"] = "unexpected"

        json_str = json.dumps(invalid_dict)
        with pytest.raises(EventSerializationError, match="Deserialized event failed validation"):
            EventSerializer.deserialize(json_str, validate=True)

    def test_deserialize_missing_field_raises_even_without_validation(
        self, sample_valid_dict: dict
    ) -> None:
        """Missing required fields raise EventSerializationError even if validate=False."""
        incomplete_dict = sample_valid_dict.copy()
        del incomplete_dict["container_id"]

        json_str = json.dumps(incomplete_dict)
        with pytest.raises(EventSerializationError, match="Missing required field in JSON event"):
            EventSerializer.deserialize(json_str, validate=False)

    def test_deserialize_boolean_value_rejected(self, sample_valid_dict: dict) -> None:
        """Boolean value is rejected as non-numeric in deserialization."""
        bad_dict = sample_valid_dict.copy()
        bad_dict["value"] = True

        json_str = json.dumps(bad_dict)
        with pytest.raises(EventSerializationError):
            EventSerializer.deserialize(json_str, validate=True)

        with pytest.raises(EventSerializationError, match="Field 'value' must be numeric"):
            EventSerializer.deserialize(json_str, validate=False)

    def test_deserialize_non_numeric_value_rejected(self, sample_valid_dict: dict) -> None:
        """Non-numeric string value is rejected in deserialization."""
        bad_dict = sample_valid_dict.copy()
        bad_dict["value"] = "twenty_five"

        json_str = json.dumps(bad_dict)
        with pytest.raises(EventSerializationError):
            EventSerializer.deserialize(json_str, validate=False)


class TestFunctionalHelpers:
    """Tests for serialize_event and deserialize_event helper functions."""

    def test_roundtrip_helpers(self, sample_valid_event: SensorEvent) -> None:
        """Roundtrip using serialize_event and deserialize_event."""
        json_str = serialize_event(sample_valid_event)
        restored = deserialize_event(json_str)

        assert restored == sample_valid_event
