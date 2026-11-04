"""Output sinks for console and JSONL persistence."""

from simulation.output.jsonl_writer import JsonlWriter, JsonlWriterError

__all__ = [
    "JsonlWriter",
    "JsonlWriterError",
]
