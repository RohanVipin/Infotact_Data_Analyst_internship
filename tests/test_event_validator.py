"""Automated unit tests for EventValidator."""

from datetime import datetime, timezone
import pytest

from simulation.events.event_schema import SensorEvent
from simulation.validation.event_validator import EventValidationError, EventValidator


@pytest.fixture
def valid_event_dict():
    """Return a canonical valid event dictionary."""
    return {
        "schema_version": 1,
        "event_id": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        "container_id": "CONTAINER_001",
        "sensor_type": "temperature",
        "observed_at": "2026-11-04T11:25:30.123Z",
        "value": 4.25,
        "unit": "celsius",
    }


def test_validator_accepts_valid_dict(valid_event_dict):
    """Verify that a canonical event dictionary passes validation."""
    EventValidator.validate(valid_event_dict)
    assert EventValidator.is_valid(valid_event_dict) is True


def test_validator_accepts_valid_sensor_event():
    """Verify that a SensorEvent instance passes validation."""
    event = SensorEvent(
        schema_version=1,
        event_id="a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        container_id="CONTAINER_002",
        sensor_type="humidity",
        observed_at="2026-11-04T11:25:30.000Z",
        value=85.0,
        unit="percent",
    )
    EventValidator.validate(event)
    assert EventValidator.is_valid(event) is True


@pytest.mark.parametrize(
    "missing_field",
    [
        "schema_version",
        "event_id",
        "container_id",
        "sensor_type",
        "observed_at",
        "value",
        "unit",
    ],
)
def test_validator_rejects_missing_fields(valid_event_dict, missing_field):
    """Verify rejection when any required field is missing."""
    del valid_event_dict[missing_field]
    with pytest.raises(EventValidationError, match=f"Missing required field.*{missing_field}"):
        EventValidator.validate(valid_event_dict)
    assert EventValidator.is_valid(valid_event_dict) is False


def test_validator_rejects_unexpected_fields(valid_event_dict):
    """Verify rejection when unapproved extra fields are present."""
    valid_event_dict["cargo_type"] = "Avocados"
    with pytest.raises(EventValidationError, match="Unexpected field.*cargo_type"):
        EventValidator.validate(valid_event_dict)


@pytest.mark.parametrize("invalid_version", [2, 0, -1, True, False, "1", 1.0])
def test_validator_rejects_invalid_schema_version(valid_event_dict, invalid_version):
    """Verify that schema_version must be integer 1 (excluding boolean True)."""
    valid_event_dict["schema_version"] = invalid_version
    with pytest.raises(EventValidationError, match="schema_version"):
        EventValidator.validate(valid_event_dict)


@pytest.mark.parametrize("invalid_uuid", ["not-a-uuid", "12345", "", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380zzz"])
def test_validator_rejects_invalid_uuid(valid_event_dict, invalid_uuid):
    """Verify rejection of malformed or invalid UUID strings."""
    valid_event_dict["event_id"] = invalid_uuid
    with pytest.raises(EventValidationError, match="'event_id' is not a valid UUID|must be a non-empty string"):
        EventValidator.validate(valid_event_dict)


def test_validator_rejects_uuid_version_other_than_v4(valid_event_dict):
    """Verify that UUID versions other than 4 (e.g. UUIDv1) are rejected."""
    import uuid
    v1_uuid = str(uuid.uuid1())
    valid_event_dict["event_id"] = v1_uuid
    with pytest.raises(EventValidationError, match="'event_id' must be a UUID version 4, got version 1"):
        EventValidator.validate(valid_event_dict)


def test_validator_accepts_valid_uuidv4(valid_event_dict):
    """Verify that genuine UUIDv4 identifiers pass validation."""
    import uuid
    valid_event_dict["event_id"] = str(uuid.uuid4())
    EventValidator.validate(valid_event_dict)
    assert EventValidator.is_valid(valid_event_dict) is True


@pytest.mark.parametrize("invalid_sensor_type", ["pressure", "light", "TEMP", ""])
def test_validator_rejects_invalid_sensor_type(valid_event_dict, invalid_sensor_type):
    """Verify rejection of unsupported sensor types."""
    valid_event_dict["sensor_type"] = invalid_sensor_type
    with pytest.raises(EventValidationError, match="'sensor_type' must be one of"):
        EventValidator.validate(valid_event_dict)


@pytest.mark.parametrize("invalid_container_id", ["", "   ", None, 123])
def test_validator_rejects_invalid_container_id(valid_event_dict, invalid_container_id):
    """Verify rejection of non-string or blank container IDs."""
    valid_event_dict["container_id"] = invalid_container_id
    with pytest.raises(EventValidationError, match="'container_id' must be a non-empty string"):
        EventValidator.validate(valid_event_dict)


@pytest.mark.parametrize("invalid_value", [True, False, float("nan"), float("inf"), float("-inf"), "4.25", None])
def test_validator_rejects_invalid_values(valid_event_dict, invalid_value):
    """Verify rejection of booleans, NaN, Infinity, strings, and None for reading values."""
    valid_event_dict["value"] = invalid_value
    with pytest.raises(EventValidationError, match="'value' must be"):
        EventValidator.validate(valid_event_dict)


@pytest.mark.parametrize(
    "invalid_timestamp",
    [
        "not-a-timestamp",
        "2026-11-04 11:25:30",  # naive without timezone
        "2026-11-04T11:25:30",  # naive ISO without timezone
        "2026-11-04T11:25:30+05:30",  # non-UTC offset
        "",
        None,
    ],
)
def test_validator_rejects_invalid_timestamps(valid_event_dict, invalid_timestamp):
    """Verify rejection of naive, non-UTC, or malformed timestamps."""
    valid_event_dict["observed_at"] = invalid_timestamp
    with pytest.raises(EventValidationError, match="'observed_at'"):
        EventValidator.validate(valid_event_dict)


@pytest.mark.parametrize("invalid_unit", ["", "   ", None, 123])
def test_validator_rejects_invalid_unit(valid_event_dict, invalid_unit):
    """Verify rejection of non-string or blank units."""
    valid_event_dict["unit"] = invalid_unit
    with pytest.raises(EventValidationError, match="'unit' must be a non-empty string"):
        EventValidator.validate(valid_event_dict)
