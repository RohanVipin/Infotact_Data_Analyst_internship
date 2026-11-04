"""Event schema, generation, and serialization modules for AtmoSync sensor simulator."""

from simulation.events.event_builder import EventBuilder, format_utc_iso8601
from simulation.events.event_schema import (
    SCHEMA_VERSION,
    SUPPORTED_SENSOR_TYPES,
    SensorEvent,
)
from simulation.events.event_serializer import (
    EventSerializationError,
    EventSerializer,
    deserialize_event,
    serialize_event,
)

__all__ = [
    "EventBuilder",
    "EventSerializationError",
    "EventSerializer",
    "SCHEMA_VERSION",
    "SUPPORTED_SENSOR_TYPES",
    "SensorEvent",
    "deserialize_event",
    "format_utc_iso8601",
    "serialize_event",
]
