"""Automated unit tests for EventBuilder and event construction."""

from datetime import datetime, timezone
import pytest
import uuid

from simulation.events.event_builder import EventBuilder, format_utc_iso8601
from simulation.events.event_schema import SCHEMA_VERSION, SensorEvent


def test_format_utc_iso8601_aware_utc():
    """Verify UTC ISO-8601 formatting with millisecond precision and Z suffix."""
    aware_dt = datetime(2026, 11, 4, 11, 25, 30, 123456, tzinfo=timezone.utc)
    formatted = format_utc_iso8601(aware_dt)
    assert formatted == "2026-11-04T11:25:30.123Z"


def test_format_utc_iso8601_non_utc_offset_normalized():
    """Verify that non-UTC timezone-aware datetimes are correctly converted to UTC Z."""
    from datetime import timedelta
    ist_tz = timezone(timedelta(hours=5, minutes=30))
    # 16:55:30 IST = 11:25:30 UTC
    ist_dt = datetime(2026, 11, 4, 16, 55, 30, 500000, tzinfo=ist_tz)
    formatted = format_utc_iso8601(ist_dt)
    assert formatted == "2026-11-04T11:25:30.500Z"


def test_format_utc_iso8601_naive_raises_value_error():
    """Verify that naive datetimes are rejected with ValueError."""
    naive_dt = datetime(2026, 11, 4, 11, 25, 30, 456000)
    with pytest.raises(ValueError, match="must be timezone-aware, got naive datetime"):
        format_utc_iso8601(naive_dt)


def test_event_builder_rejects_naive_datetime():
    """Verify EventBuilder rejects naive datetime passed to build_event."""
    builder = EventBuilder()
    naive_dt = datetime(2026, 11, 4, 11, 25, 30)
    with pytest.raises(ValueError, match="must be timezone-aware, got naive datetime"):
        builder.build_event("CONT_001", "temperature", 4.0, "celsius", observed_at=naive_dt)


def test_event_builder_default_creation():
    """Verify default creation of SensorEvent with auto-generated UUID and clock."""
    builder = EventBuilder()
    event = builder.build_event(
        container_id="CONT_001",
        sensor_type="temperature",
        value=4.25,
        unit="celsius",
    )
    assert isinstance(event, SensorEvent)
    assert event.schema_version == SCHEMA_VERSION
    assert event.container_id == "CONT_001"
    assert event.sensor_type == "temperature"
    assert event.value == 4.25
    assert event.unit == "celsius"
    assert event.observed_at.endswith("Z")

    # Verify UUID validity
    parsed_uuid = uuid.UUID(event.event_id)
    assert str(parsed_uuid) == event.event_id


def test_event_builder_injectable_dependencies():
    """Verify deterministic event building with injected UUID generator and clock."""
    fixed_uuid = "12345678-1234-5678-1234-567812345678"
    fixed_time = datetime(2026, 11, 5, 8, 30, 0, 0, tzinfo=timezone.utc)

    builder = EventBuilder(
        uuid_generator=lambda: fixed_uuid,
        clock=lambda: fixed_time,
    )

    event = builder.build_event(
        container_id="CONT_TEST",
        sensor_type="humidity",
        value=85.5,
        unit="percent",
    )

    assert event.event_id == fixed_uuid
    assert event.observed_at == "2026-11-05T08:30:00.000Z"
    assert event.to_dict() == {
        "schema_version": 1,
        "event_id": fixed_uuid,
        "container_id": "CONT_TEST",
        "sensor_type": "humidity",
        "observed_at": "2026-11-05T08:30:00.000Z",
        "value": 85.5,
        "unit": "percent",
    }


def test_event_builder_rejects_empty_container_id():
    """Verify rejection of empty or blank container ID."""
    builder = EventBuilder()
    with pytest.raises(ValueError, match="container_id must be a non-empty string"):
        builder.build_event(container_id="  ", sensor_type="temperature", value=4.0, unit="celsius")


def test_event_builder_rejects_unsupported_sensor_type():
    """Verify rejection of unsupported sensor type."""
    builder = EventBuilder()
    with pytest.raises(ValueError, match="sensor_type must be one of"):
        builder.build_event(container_id="CONT_001", sensor_type="pressure", value=1013.25, unit="hPa")


@pytest.mark.parametrize("invalid_value", [True, False, float("nan"), float("inf"), float("-inf"), "4.5"])
def test_event_builder_rejects_invalid_values(invalid_value):
    """Verify rejection of booleans, NaN, Infinity, and strings for reading value."""
    builder = EventBuilder()
    with pytest.raises(ValueError, match="value must be a finite numeric value"):
        builder.build_event(container_id="CONT_001", sensor_type="temperature", value=invalid_value, unit="celsius")


def test_event_builder_rejects_empty_unit():
    """Verify rejection of empty or whitespace-only unit."""
    builder = EventBuilder()
    with pytest.raises(ValueError, match="unit must be a non-empty string"):
        builder.build_event(container_id="CONT_001", sensor_type="vibration", value=0.15, unit=" ")
