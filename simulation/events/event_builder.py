"""Event builder for constructing canonical SensorEvent instances.

Provides factory methods with injectable UUID and clock dependencies for deterministic testing.
"""

from datetime import datetime, timezone
import math
from typing import Callable, Optional, Union
import uuid

from simulation.events.event_schema import SCHEMA_VERSION, SUPPORTED_SENSOR_TYPES, SensorEvent


def format_utc_iso8601(dt: datetime) -> str:
    """Format a timezone-aware datetime into a canonical UTC ISO-8601 string ending in 'Z'.

    Args:
        dt: Timezone-aware datetime object.

    Returns:
        ISO-8601 formatted timestamp string with millisecond precision and 'Z' suffix.

    Raises:
        ValueError: If dt is naive (lacks timezone information).
    """
    if dt.tzinfo is None:
        raise ValueError(f"datetime must be timezone-aware, got naive datetime: {repr(dt)}")

    dt_utc = dt.astimezone(timezone.utc)

    # Format with millisecond precision: YYYY-MM-DDTHH:MM:SS.mmmZ
    millis = dt_utc.microsecond // 1000
    base_str = dt_utc.strftime("%Y-%m-%dT%H:%M:%S")
    return f"{base_str}.{millis:03d}Z"


class EventBuilder:
    """Constructs canonical SensorEvent instances.

    Allows injection of custom clock and UUID generator functions for deterministic
    test verification.
    """

    def __init__(
        self,
        uuid_generator: Optional[Callable[[], str]] = None,
        clock: Optional[Callable[[], datetime]] = None,
    ) -> None:
        """Initialize the event builder.

        Args:
            uuid_generator: Callable returning unique UUID strings. Defaults to uuid.uuid4.
            clock: Callable returning timezone-aware datetime objects. Defaults to datetime.now(timezone.utc).
        """
        self._uuid_generator = uuid_generator or (lambda: str(uuid.uuid4()))
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def build_event(
        self,
        container_id: str,
        sensor_type: str,
        value: float,
        unit: str,
        observed_at: Optional[Union[str, datetime]] = None,
        event_id: Optional[str] = None,
    ) -> SensorEvent:
        """Build an individual SensorEvent.

        Args:
            container_id: Non-empty container identifier.
            sensor_type: One of 'temperature', 'humidity', or 'vibration'.
            value: Finite numeric sensor reading.
            unit: Non-empty measurement unit string.
            observed_at: Optional explicit timestamp (string or datetime).
                If None, sampled from the configured clock.
            event_id: Optional explicit UUID string. If None, sampled from the UUID generator.

        Returns:
            A new SensorEvent instance.

        Raises:
            ValueError: If any parameter violates basic domain constraints.
        """
        if not isinstance(container_id, str) or not container_id.strip():
            raise ValueError(f"container_id must be a non-empty string, got {repr(container_id)}")

        if sensor_type not in SUPPORTED_SENSOR_TYPES:
            raise ValueError(
                f"sensor_type must be one of {sorted(list(SUPPORTED_SENSOR_TYPES))}, got {repr(sensor_type)}"
            )

        # In Python bool is a subclass of int, so reject booleans explicitly
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"value must be a finite numeric value, got {repr(value)}")

        if not isinstance(unit, str) or not unit.strip():
            raise ValueError(f"unit must be a non-empty string, got {repr(unit)}")

        # Resolve event_id
        resolved_event_id = event_id if event_id is not None else self._uuid_generator()
        if not isinstance(resolved_event_id, str) or not resolved_event_id.strip():
            raise ValueError(f"event_id must be a non-empty string, got {repr(resolved_event_id)}")

        # Resolve timestamp
        if observed_at is None:
            resolved_observed_at = format_utc_iso8601(self._clock())
        elif isinstance(observed_at, datetime):
            resolved_observed_at = format_utc_iso8601(observed_at)
        elif isinstance(observed_at, str):
            if not observed_at.strip():
                raise ValueError("observed_at string cannot be blank")
            resolved_observed_at = observed_at.strip()
        else:
            raise ValueError(f"observed_at must be a str or datetime, got {type(observed_at).__name__}")

        return SensorEvent(
            schema_version=SCHEMA_VERSION,
            event_id=resolved_event_id,
            container_id=container_id.strip(),
            sensor_type=sensor_type,
            observed_at=resolved_observed_at,
            value=float(value),
            unit=unit.strip(),
        )
