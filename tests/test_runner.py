"""Unit tests for the SimulationRunner and RunnerMetrics.

Tests normal execution, cycle coordination, event identity preservation,
resource ownership, and Kafka asynchronous delivery tracking.
"""

from typing import List, Optional
import pytest

from simulation.config.config_loader import AppConfig, SensorConfig, SimulationSettings
from simulation.events.event_schema import SensorEvent
from simulation.kafka.delivery_report import PublishingStats
from simulation.kafka.exceptions import KafkaPublishError
from simulation.output.sink import CompositeOutputSink, KafkaSink, OutputSink
from simulation.runner import RunnerMetrics, SimulationResult, SimulationRunner
from simulation.simulator.simulation_engine import SimulationEngine


class MockOutputSink(OutputSink):
    """Test fake for OutputSink capturing dispatched events and lifecycle calls."""

    def __init__(self) -> None:
        self.events: List[SensorEvent] = []
        self.flush_called: int = 0
        self.close_called: int = 0
        self.is_closed: bool = False

    def write_event(self, event: SensorEvent) -> None:
        if self.is_closed:
            raise RuntimeError("Sink is closed")
        self.events.append(event)

    def flush(self) -> None:
        self.flush_called += 1

    def close(self) -> None:
        self.close_called += 1
        self.is_closed = True


class MockKafkaProducer:
    """Test fake for Kafka producer exposing PublishingStats and flush behavior."""

    def __init__(self) -> None:
        self.stats = PublishingStats()
        self.is_closed = False
        self.flush_called = 0
        self.flush_error: Optional[Exception] = None

    def publish_event(self, event, on_delivery=None) -> None:
        if self.is_closed:
            raise KafkaPublishError("Producer closed")
        self.stats.record_attempt()
        # Enqueue does NOT invoke delivery callback immediately (asynchronous Kafka semantics)

    def flush(self, timeout=None, raise_on_timeout=True) -> int:
        self.flush_called += 1
        self.stats.record_flush()
        if self.flush_error:
            raise self.flush_error
        # On normal flush, drain all pending messages as delivered
        pending = self.stats.pending
        for _ in range(pending):
            self.stats.record_delivery(success=True)
        return 0

    def close(self, timeout=None, raise_on_timeout=False) -> int:
        self.is_closed = True
        return 0


def create_test_engine(interval_seconds: float = 0.01) -> SimulationEngine:
    """Helper creating a real SimulationEngine with 2 containers and instantaneous sleep."""
    config = AppConfig(
        simulation=SimulationSettings(interval_seconds=interval_seconds, random_seed=42),
        containers=("CONT_001", "CONT_002"),
        sensors={
            "temperature": SensorConfig(enabled=True, unit="celsius"),
            "humidity": SensorConfig(enabled=True, unit="percent"),
            "vibration": SensorConfig(enabled=True, unit="m/s^2"),
        },
    )
    return SimulationEngine(config=config, sleep_fn=lambda _: None)


class TestRunnerMetrics:
    """Tests for the RunnerMetrics immutable dataclass."""

    def test_default_values(self) -> None:
        metrics = RunnerMetrics()
        assert metrics.events_generated == 0
        assert metrics.sink_dispatch_attempted == 0
        assert metrics.sink_dispatch_succeeded == 0
        assert metrics.sink_dispatch_failed == 0
        assert metrics.simulation_cycles_completed == 0
        assert metrics.simulation_cycles_failed == 0
        assert metrics.kafka_delivery_acknowledged is None
        assert metrics.kafka_delivery_failed is None
        assert metrics.kafka_pending is None
        assert metrics.status == "INITIALIZED"
        assert metrics.start_time is None
        assert metrics.end_time is None
        assert metrics.duration_seconds == 0.0

    def test_simulation_result_alias(self) -> None:
        assert SimulationResult is RunnerMetrics

    def test_metrics_immutability(self) -> None:
        metrics = RunnerMetrics(events_generated=10)
        with pytest.raises(Exception):
            metrics.events_generated = 20  # type: ignore

    def test_to_dict_serialization(self) -> None:
        metrics = RunnerMetrics(
            events_generated=5,
            sink_dispatch_attempted=5,
            sink_dispatch_succeeded=5,
            simulation_cycles_completed=1,
            status="COMPLETED",
            duration_seconds=1.23456,
        )
        d = metrics.to_dict()
        assert d["events_generated"] == 5
        assert d["simulation_cycles_completed"] == 1
        assert d["status"] == "COMPLETED"
        assert d["duration_seconds"] == 1.2346


class TestSimulationRunnerNormalExecution:
    """Tests normal finite execution, cycle coordination, and event identity."""

    def test_initialization_validations(self) -> None:
        engine = create_test_engine()
        sink = MockOutputSink()

        with pytest.raises(TypeError, match="engine must be an instance of SimulationEngine"):
            SimulationRunner(engine="not-an-engine", output_sink=sink)  # type: ignore

        with pytest.raises(TypeError, match="output_sink must be an instance of OutputSink"):
            SimulationRunner(engine=engine, output_sink="not-a-sink")  # type: ignore

        runner = SimulationRunner(engine=engine, output_sink=sink)
        assert runner.engine is engine
        assert runner.output_sink is sink
        assert runner.fail_fast is True
        assert runner.owns_sink is True
        assert runner.status == "INITIALIZED"
        assert not runner.is_stopped
        assert not runner.is_closed

    def test_invalid_max_cycles_rejected(self) -> None:
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        with pytest.raises(ValueError, match="max_cycles must be a positive integer"):
            runner.run(max_cycles=0)

        with pytest.raises(ValueError, match="max_cycles must be a positive integer"):
            runner.run(max_cycles=-5)

        with pytest.raises(ValueError, match="max_cycles must be a positive integer"):
            runner.run(max_cycles=True)  # type: ignore

        with pytest.raises(ValueError, match="max_cycles must be a positive integer"):
            runner.run(max_cycles=2.5)  # type: ignore

    def test_finite_run_single_cycle(self) -> None:
        """Requirement 1, 3, 4: Finite run with one cycle, correct event and cycle counts."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        metrics = runner.run(max_cycles=1)

        # 2 containers * 3 enabled sensors = 6 events per cycle
        assert metrics.simulation_cycles_completed == 1
        assert metrics.simulation_cycles_failed == 0
        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_attempted == 6
        assert metrics.sink_dispatch_succeeded == 6
        assert metrics.sink_dispatch_failed == 0
        assert metrics.status == "COMPLETED"
        assert len(sink.events) == 6
        assert sink.flush_called == 1
        assert sink.close_called == 1
        assert runner.is_closed is True

    def test_finite_run_multiple_cycles(self) -> None:
        """Requirement 2, 3, 4: Finite run with multiple cycles."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        metrics = runner.run(max_cycles=3)

        assert metrics.simulation_cycles_completed == 3
        assert metrics.simulation_cycles_failed == 0
        assert metrics.events_generated == 18
        assert metrics.sink_dispatch_attempted == 18
        assert metrics.sink_dispatch_succeeded == 18
        assert metrics.status == "COMPLETED"
        assert len(sink.events) == 18

    def test_same_event_object_reaches_sink_without_mutation(self) -> None:
        """Requirement 5: Exact same SensorEvent object reaches sink without mutation or recreation."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        # Intercept run_cycle to capture engine's generated event instances
        original_run_cycle = engine.run_cycle
        generated_events: List[SensorEvent] = []

        def intercepted_run_cycle(*args, **kwargs):
            evs = original_run_cycle(*args, **kwargs)
            generated_events.extend(evs)
            return evs

        engine.run_cycle = intercepted_run_cycle  # type: ignore

        runner.run(max_cycles=1)

        assert len(sink.events) == len(generated_events) == 6
        for generated_ev, sink_ev in zip(generated_events, sink.events):
            # Identical object reference identity
            assert generated_ev is sink_ev
            assert generated_ev.event_id == sink_ev.event_id
            assert generated_ev.observed_at == sink_ev.observed_at
            assert generated_ev.value == sink_ev.value

    def test_events_dispatched_in_generated_order(self) -> None:
        """Requirement 6: Multiple events are dispatched in their generated order."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        runner.run(max_cycles=2)

        # Expected order per cycle: CONT_001 (temp, hum, vib), CONT_002 (temp, hum, vib)
        for cycle_idx in range(2):
            base = cycle_idx * 6
            assert sink.events[base + 0].container_id == "CONT_001"
            assert sink.events[base + 0].sensor_type == "temperature"
            assert sink.events[base + 1].container_id == "CONT_001"
            assert sink.events[base + 1].sensor_type == "humidity"
            assert sink.events[base + 2].container_id == "CONT_001"
            assert sink.events[base + 2].sensor_type == "vibration"

            assert sink.events[base + 3].container_id == "CONT_002"
            assert sink.events[base + 3].sensor_type == "temperature"
            assert sink.events[base + 4].container_id == "CONT_002"
            assert sink.events[base + 4].sensor_type == "humidity"
            assert sink.events[base + 5].container_id == "CONT_002"
            assert sink.events[base + 5].sensor_type == "vibration"

    def test_resource_ownership_owns_sink_true(self) -> None:
        """Requirement 7: owns_sink=True flushes and closes output sink on completion."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink, owns_sink=True)

        runner.run(max_cycles=1)

        assert sink.flush_called == 1
        assert sink.close_called == 1
        assert sink.is_closed is True
        assert runner.is_closed is True

    def test_resource_ownership_owns_sink_false(self) -> None:
        """Requirement 7: owns_sink=False flushes but does NOT close output sink on completion."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink, owns_sink=False)

        runner.run(max_cycles=1)

        assert sink.flush_called == 1
        assert sink.close_called == 0
        assert sink.is_closed is False
        assert runner.is_closed is True

    def test_metrics_accurately_reflect_execution(self) -> None:
        """Requirement 8: Metrics accurately reflect execution counters and timing."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        metrics = runner.run(max_cycles=2)

        assert metrics.events_generated == 12
        assert metrics.sink_dispatch_attempted == 12
        assert metrics.sink_dispatch_succeeded == 12
        assert metrics.sink_dispatch_failed == 0
        assert metrics.simulation_cycles_completed == 2
        assert metrics.simulation_cycles_failed == 0
        assert metrics.status == "COMPLETED"
        assert metrics.start_time is not None
        assert metrics.end_time is not None
        assert metrics.duration_seconds >= 0.0

    def test_standalone_run_cycle(self) -> None:
        """Verify calling run_cycle() directly dispatches events and updates metrics."""
        engine = create_test_engine()
        sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=sink)

        events = runner.run_cycle()
        assert len(events) == 6
        assert len(sink.events) == 6

        metrics = runner.get_metrics()
        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_succeeded == 6
        assert metrics.simulation_cycles_completed == 1

    def test_stop_gracefully_halts_execution(self) -> None:
        """Verify stop() halts continuous execution and marks status as STOPPED."""
        engine = create_test_engine()
        sink = MockOutputSink()

        # Stop runner after first cycle via custom sleep_fn
        def stopping_sleep(_):
            runner.stop()

        runner = SimulationRunner(engine=engine, output_sink=sink, sleep_fn=stopping_sleep)
        metrics = runner.run(max_cycles=10)

        assert metrics.status == "STOPPED"
        assert metrics.simulation_cycles_completed == 1
        assert metrics.events_generated == 6
        assert sink.flush_called == 1

    def test_context_manager_normal_cleanup(self) -> None:
        """Verify context manager usage flushes and closes owned sink."""
        engine = create_test_engine()
        sink = MockOutputSink()

        with SimulationRunner(engine=engine, output_sink=sink) as runner:
            runner.run(max_cycles=1)

        assert sink.flush_called >= 1
        assert sink.close_called == 1
        assert runner.is_closed is True


class TestRunnerKafkaAsynchronousDelivery:
    """Tests Kafka asynchronous delivery semantics, statistics, and flush tracking."""

    def test_enqueue_does_not_increment_broker_acknowledgement(self) -> None:
        """Requirement 17: Successful enqueue does NOT increment broker acknowledgement count."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer()
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink)

        # Run 1 cycle manually without calling runner.run() shutdown
        events = runner.run_cycle()
        assert len(events) == 6

        # Check metrics before flush: messages enqueued, but not acknowledged by broker
        metrics = runner.get_metrics()
        assert metrics.sink_dispatch_succeeded == 6
        assert metrics.kafka_delivery_acknowledged == 0
        assert metrics.kafka_pending == 6
        assert metrics.kafka_delivery_failed == 0

    def test_delivery_callback_increments_acknowledged_count(self) -> None:
        """Requirement 18: Successful delivery callback increments acknowledged count."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer()
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink)

        runner.run_cycle()

        # Simulate 4 broker acknowledgements
        for _ in range(4):
            mock_producer.stats.record_delivery(success=True)

        metrics = runner.get_metrics()
        assert metrics.kafka_delivery_acknowledged == 4
        assert metrics.kafka_pending == 2
        assert metrics.kafka_delivery_failed == 0

    def test_delivery_callback_increments_failed_count(self) -> None:
        """Requirement 19: Failed delivery callback increments failed count."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer()
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink)

        runner.run_cycle()

        # Simulate 2 successes and 2 failures
        mock_producer.stats.record_delivery(success=True)
        mock_producer.stats.record_delivery(success=True)
        mock_producer.stats.record_delivery(success=False)
        mock_producer.stats.record_delivery(success=False)

        metrics = runner.get_metrics()
        assert metrics.kafka_delivery_acknowledged == 2
        assert metrics.kafka_delivery_failed == 2
        assert metrics.kafka_pending == 2

    def test_pending_messages_reported_accurately_after_flush(self) -> None:
        """Requirement 20: Pending messages are reported accurately after flush."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer()
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink)

        # Full run includes flush at the end
        metrics = runner.run(max_cycles=1)

        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_succeeded == 6
        # MockKafkaProducer flush() acknowledges all remaining pending messages
        assert metrics.kafka_pending == 0
        assert metrics.kafka_delivery_acknowledged == 6
        assert metrics.kafka_delivery_failed == 0

    def test_composite_sink_kafka_metrics_extraction(self) -> None:
        """Verify runner extracts Kafka metrics when wrapped inside CompositeOutputSink."""
        engine = create_test_engine()
        jsonl_sink = MockOutputSink()
        mock_producer = MockKafkaProducer()
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        composite = CompositeOutputSink([jsonl_sink, kafka_sink])

        runner = SimulationRunner(engine=engine, output_sink=composite)
        metrics = runner.run(max_cycles=1)

        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_succeeded == 6
        assert metrics.kafka_pending == 0
        assert metrics.kafka_delivery_acknowledged == 6
        assert len(jsonl_sink.events) == 6

    def test_no_kafka_sink_returns_none_for_kafka_metrics(self) -> None:
        """Verify runner returns None for Kafka metrics when only non-Kafka sinks are used."""
        engine = create_test_engine()
        mock_sink = MockOutputSink()
        runner = SimulationRunner(engine=engine, output_sink=mock_sink)

        metrics = runner.run(max_cycles=1)
        assert metrics.kafka_delivery_acknowledged is None
        assert metrics.kafka_delivery_failed is None
        assert metrics.kafka_pending is None
