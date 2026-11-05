"""End-to-end integration tests for SimulationRunner.

Validates the complete execution pipeline coordinating:
1. Real SimulationEngine (multi-container, multi-sensor).
2. Real JsonlSink (disk file persistence).
3. CompositeOutputSink (multi-destination routing).
4. Both fake and live KafkaSink destinations.
"""

import json
import os
from pathlib import Path
import socket
import time
from typing import List, Optional
import uuid
import pytest

from simulation.config.config_loader import AppConfig, SensorConfig, SimulationSettings
from simulation.events.event_schema import SensorEvent
from simulation.events.event_serializer import EventSerializer
from simulation.kafka.delivery_report import PublishingStats
from simulation.kafka.kafka_config import KafkaConfig
from simulation.kafka.kafka_producer import SensorEventKafkaProducer
from simulation.output.sink import CompositeOutputSink, JsonlSink, KafkaSink
from simulation.runner import SimulationRunner
from simulation.simulator.simulation_engine import SimulationEngine
from simulation.validation.event_validator import EventValidator


def is_broker_reachable(host: str = "localhost", port: int = 9092, timeout: float = 1.0) -> bool:
    """Check if TCP connection can be established to local Kafka broker."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


@pytest.fixture
def live_kafka_config() -> KafkaConfig:
    """Fixture providing KafkaConfig for live broker tests, skipping if broker is unreachable."""
    bootstrap_servers = os.getenv("ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    host, port_str = bootstrap_servers.split(",")[0].strip().rsplit(":", 1)
    port = int(port_str)

    if not is_broker_reachable(host, port):
        pytest.skip(f"Live Kafka broker is not reachable at {host}:{port}")

    return KafkaConfig.from_env(
        bootstrap_servers=bootstrap_servers,
        topic=os.getenv("ATMOSYNC_KAFKA_TOPIC", "atmosync.telemetry.integration"),
        client_id="atmosync-runner-integration",
        delivery_timeout_ms=10000,
    )


class MockKafkaProducer:
    """Test fake for Kafka producer accumulating published events and reporting stats."""

    def __init__(self) -> None:
        self.stats = PublishingStats()
        self.published_events: List[SensorEvent] = []
        self.is_closed = False

    def publish_event(self, event: SensorEvent, on_delivery=None) -> None:
        self.stats.record_attempt()
        self.published_events.append(event)

    def flush(self, timeout=None, raise_on_timeout=True) -> int:
        self.stats.record_flush()
        # Mark all enqueued events delivered on flush
        pending = self.stats.pending
        for _ in range(pending):
            self.stats.record_delivery(success=True)
        return 0

    def close(self, timeout=None, raise_on_timeout=False) -> int:
        self.is_closed = True
        return 0


def create_integration_engine(containers: Optional[List[str]] = None) -> SimulationEngine:
    """Create a real SimulationEngine configured for integration tests."""
    c_list = tuple(containers) if containers else ("CONT_INT_001", "CONT_INT_002", "CONT_INT_003")
    config = AppConfig(
        simulation=SimulationSettings(interval_seconds=0.01, random_seed=12345),
        containers=c_list,
        sensors={
            "temperature": SensorConfig(enabled=True, unit="celsius"),
            "humidity": SensorConfig(enabled=True, unit="percent"),
            "vibration": SensorConfig(enabled=True, unit="m/s^2"),
        },
    )
    return SimulationEngine(config=config, sleep_fn=lambda _: None)


class TestSimulationRunnerLocalIntegration:
    """End-to-end integration test of the runner with real engine, real JSONL sink, and composite sink."""

    def test_end_to_end_local_pipeline(self, tmp_path: Path) -> None:
        """Run multi-cycle pipeline writing to real JSONL and mock Kafka sink simultaneously."""
        jsonl_file = tmp_path / "integration_telemetry.jsonl"
        engine = create_integration_engine()

        jsonl_sink = JsonlSink(output_path=jsonl_file)
        mock_producer = MockKafkaProducer()
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore

        composite_sink = CompositeOutputSink([jsonl_sink, kafka_sink])
        runner = SimulationRunner(engine=engine, output_sink=composite_sink, owns_sink=True)

        # Run 2 complete cycles: 3 containers * 3 sensors * 2 cycles = 18 events
        metrics = runner.run(max_cycles=2)

        # 1. Verify runner execution metrics
        assert metrics.status == "COMPLETED"
        assert metrics.simulation_cycles_completed == 2
        assert metrics.simulation_cycles_failed == 0
        assert metrics.events_generated == 18
        assert metrics.sink_dispatch_attempted == 18
        assert metrics.sink_dispatch_succeeded == 18
        assert metrics.sink_dispatch_failed == 0
        assert metrics.kafka_delivery_acknowledged == 18
        assert metrics.kafka_pending == 0

        # 2. Verify JSONL file on disk
        assert jsonl_file.exists()
        lines = jsonl_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 18

        jsonl_events: List[SensorEvent] = []
        for line_idx, line in enumerate(lines):
            raw = json.loads(line)
            # Schema structure validation
            assert raw["schema_version"] == 1
            assert "event_id" in raw
            assert raw["container_id"] in ["CONT_INT_001", "CONT_INT_002", "CONT_INT_003"]
            assert raw["sensor_type"] in ["temperature", "humidity", "vibration"]
            if raw["sensor_type"] == "temperature":
                assert raw["unit"] == "celsius"
            elif raw["sensor_type"] == "humidity":
                assert raw["unit"] == "percent"
            elif raw["sensor_type"] == "vibration":
                assert raw["unit"] == "m/s^2"

            event = EventSerializer.deserialize(line)
            EventValidator.validate(event)
            jsonl_events.append(event)

        # 3. Verify Kafka mock received exact same events in identical order
        assert len(mock_producer.published_events) == 18
        for j_ev, k_ev in zip(jsonl_events, mock_producer.published_events):
            assert j_ev.event_id == k_ev.event_id
            assert j_ev.container_id == k_ev.container_id
            assert j_ev.sensor_type == k_ev.sensor_type
            assert j_ev.value == k_ev.value
            assert j_ev.unit == k_ev.unit
            assert j_ev.observed_at == k_ev.observed_at


@pytest.mark.integration
class TestSimulationRunnerLiveBrokerIntegration:
    """Live broker integration test running SimulationRunner with real Kafka broker and real JSONL file."""

    def test_live_runner_dual_sink_verification(
        self,
        tmp_path: Path,
        live_kafka_config: KafkaConfig,
    ) -> None:
        """Run SimulationRunner connected to live Kafka broker and real JSONL sink.

        Verifies:
        1. Exact event count equality across engine, JSONL file, and live Kafka topic.
        2. Exact event_id match between consumed Kafka records and written JSONL lines.
        3. All Kafka messages acknowledged by broker.
        4. Zero pending messages after runner completion.
        """
        from confluent_kafka import Consumer, KafkaError

        jsonl_file = tmp_path / "live_telemetry.jsonl"
        containers = ["LIVE_CONT_001", "LIVE_CONT_002"]
        engine = create_integration_engine(containers=containers)

        jsonl_sink = JsonlSink(output_path=jsonl_file)
        kafka_producer = SensorEventKafkaProducer(config=live_kafka_config)
        kafka_sink = KafkaSink(producer=kafka_producer)

        composite_sink = CompositeOutputSink([jsonl_sink, kafka_sink])
        runner = SimulationRunner(engine=engine, output_sink=composite_sink, owns_sink=True)

        # 2 containers * 3 sensors * 2 cycles = 12 events
        expected_events = 12
        metrics = runner.run(max_cycles=2)

        # 1. Verify runner execution summary
        assert metrics.status == "COMPLETED"
        assert metrics.simulation_cycles_completed == 2
        assert metrics.events_generated == expected_events
        assert metrics.sink_dispatch_succeeded == expected_events
        assert metrics.kafka_delivery_acknowledged == expected_events
        assert metrics.kafka_pending == 0
        assert metrics.kafka_delivery_failed == 0

        # 2. Read JSONL file lines
        assert jsonl_file.exists()
        jsonl_lines = jsonl_file.read_text(encoding="utf-8").strip().splitlines()
        assert len(jsonl_lines) == expected_events

        jsonl_events_by_id = {}
        for line in jsonl_lines:
            ev = EventSerializer.deserialize(line)
            EventValidator.validate(ev)
            jsonl_events_by_id[ev.event_id] = ev

        assert len(jsonl_events_by_id) == expected_events

        # 3. Consume from live Kafka topic
        consumer_group = f"atmosync-runner-verify-{uuid.uuid4()}"
        consumer = Consumer({
            "bootstrap.servers": live_kafka_config.bootstrap_servers,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe([live_kafka_config.topic])

        consumed_by_id = {}
        start_wait = time.time()
        timeout_seconds = 20.0

        try:
            while len(consumed_by_id) < expected_events and (time.time() - start_wait) < timeout_seconds:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    pytest.fail(f"Kafka consumer error: {msg.error()}")

                raw_val = msg.value().decode("utf-8")
                ev = EventSerializer.deserialize(raw_val)

                # Collect only events from this runner execution
                if ev.event_id in jsonl_events_by_id:
                    consumed_by_id[ev.event_id] = ev
        finally:
            consumer.close()

        # 4. Strict cross-sink verification
        assert len(consumed_by_id) == expected_events, (
            f"Expected {expected_events} consumed events matching JSONL IDs, got {len(consumed_by_id)}"
        )

        for event_id, jsonl_ev in jsonl_events_by_id.items():
            kafka_ev = consumed_by_id[event_id]
            # Verify exact field equivalence
            assert kafka_ev.schema_version == jsonl_ev.schema_version == 1
            assert kafka_ev.event_id == jsonl_ev.event_id == event_id
            assert kafka_ev.container_id == jsonl_ev.container_id
            assert kafka_ev.sensor_type == jsonl_ev.sensor_type
            assert kafka_ev.value == jsonl_ev.value
            assert kafka_ev.unit == jsonl_ev.unit
            assert kafka_ev.observed_at == jsonl_ev.observed_at
