"""Output sinks for console, JSONL persistence, and Kafka telemetry streaming."""

from simulation.output.jsonl_writer import JsonlWriter, JsonlWriterError
from simulation.output.sink import (
    CompositeOutputSink,
    JsonlSink,
    KafkaSink,
    OutputSink,
)

__all__ = [
    "CompositeOutputSink",
    "JsonlSink",
    "JsonlWriter",
    "JsonlWriterError",
    "KafkaSink",
    "OutputSink",
]
