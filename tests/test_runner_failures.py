"""Failure mode tests for SimulationRunner.

Tests simulation engine failures, sink dispatch failures, composite partial failures,
flush/close cleanup errors, and in-flight exception preservation.
"""

from typing import List, Optional
import pytest

from simulation.config.config_loader import AppConfig, SensorConfig, SimulationSettings
from simulation.events.event_schema import SensorEvent
from simulation.kafka.delivery_report import PublishingStats
from simulation.kafka.exceptions import KafkaBackpressureError, KafkaPublishError
from simulation.output.jsonl_writer import JsonlWriterError
from simulation.output.sink import CompositeOutputSink, KafkaSink, OutputSink
from simulation.runner import SimulationRunner
from simulation.simulator.simulation_engine import SimulationEngine


class MockOutputSink(OutputSink):
    """Test sink fake with configurable failure triggers."""

    def __init__(
        self,
        fail_on_event_idx: Optional[int] = None,
        exception_to_raise: Optional[Exception] = None,
        fail_on_flush: Optional[Exception] = None,
        fail_on_close: Optional[Exception] = None,
    ) -> None:
        self.events: List[SensorEvent] = []
        self.fail_on_event_idx = fail_on_event_idx
        self.exception_to_raise = exception_to_raise or RuntimeError("Sink write error")
        self.fail_on_flush = fail_on_flush
        self.fail_on_close = fail_on_close
        self.attempt_count: int = 0
        self.flush_called: int = 0
        self.close_called: int = 0
        self.is_closed: bool = False

    def write_event(self, event: SensorEvent) -> None:
        if self.is_closed:
            raise RuntimeError("Sink is closed")
        current_attempt = self.attempt_count
        self.attempt_count += 1
        if self.fail_on_event_idx is not None and current_attempt == self.fail_on_event_idx:
            raise self.exception_to_raise
        self.events.append(event)

    def flush(self) -> None:
        self.flush_called += 1
        if self.fail_on_flush:
            raise self.fail_on_flush

    def close(self) -> None:
        self.close_called += 1
        self.is_closed = True
        if self.fail_on_close:
            raise self.fail_on_close


class MockKafkaProducer:
    """Mock Kafka producer with configurable failure modes."""

    def __init__(
        self,
        fail_on_produce: Optional[Exception] = None,
        simulate_delivery_failure: bool = False,
        simulate_pending_count: int = 0,
    ) -> None:
        self.stats = PublishingStats()
        self.fail_on_produce = fail_on_produce
        self.simulate_delivery_failure = simulate_delivery_failure
        self.simulate_pending_count = simulate_pending_count
        self.flush_called = 0
        self.flush_error: Optional[Exception] = None

    def publish_event(self, event, on_delivery=None) -> None:
        if self.fail_on_produce:
            raise self.fail_on_produce
        self.stats.record_attempt()

    def flush(self, timeout=None, raise_on_timeout=True) -> int:
        self.flush_called += 1
        if self.flush_error:
            raise self.flush_error
        # Drain attempts into delivery receipts
        pending = self.stats.pending - self.simulate_pending_count
        for _ in range(max(0, pending)):
            self.stats.record_delivery(success=not self.simulate_delivery_failure)
        return self.simulate_pending_count

    def close(self, timeout=None, raise_on_timeout=False) -> int:
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


class TestSimulationRunnerFailures:
    """Tests failure propagation, cleanup, and metrics under error conditions."""

    def test_simulation_engine_failure(self) -> None:
        """Requirement 9: Simulation engine failure halts run and preserves exception."""
        engine = create_test_engine()
        sink = MockOutputSink()

        # Simulate engine failure before producing events
        def failing_run_cycle(*args, **kwargs):
            raise RuntimeError("Engine sensor bus hardware fault")

        engine.run_cycle = failing_run_cycle  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=sink)

        with pytest.raises(RuntimeError, match="Engine sensor bus hardware fault"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        assert metrics.simulation_cycles_completed == 0
        assert metrics.simulation_cycles_failed == 1
        assert metrics.events_generated == 0
        assert metrics.sink_dispatch_attempted == 0
        # Cleanup was executed
        assert sink.flush_called == 1
        assert sink.close_called == 1
        assert runner.is_closed is True

    def test_jsonl_output_failure_propagation(self) -> None:
        """Requirement 10: JSONL write failure propagates under fail_fast=True."""
        engine = create_test_engine()
        sink = MockOutputSink(
            fail_on_event_idx=0,
            exception_to_raise=JsonlWriterError("Disk write failed: permission denied"),
        )
        runner = SimulationRunner(engine=engine, output_sink=sink, fail_fast=True)

        with pytest.raises(JsonlWriterError, match="Disk write failed: permission denied"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        assert metrics.events_generated == 1
        assert metrics.sink_dispatch_attempted == 1
        assert metrics.sink_dispatch_succeeded == 0
        assert metrics.sink_dispatch_failed == 1
        assert metrics.simulation_cycles_completed == 0
        assert metrics.simulation_cycles_failed == 1

    def test_kafka_enqueue_failure_propagation(self) -> None:
        """Requirement 11: Kafka enqueue failure propagates under fail_fast=True."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer(
            fail_on_produce=KafkaBackpressureError("Producer queue is full after retries")
        )
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink, fail_fast=True)

        with pytest.raises(KafkaBackpressureError, match="Producer queue is full"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        assert metrics.sink_dispatch_failed == 1
        assert metrics.simulation_cycles_failed == 1

    def test_composite_sink_failure_partial_write(self) -> None:
        """Requirement 12: In composite sink, JSONL succeeds and Kafka fails; exception propagates."""
        engine = create_test_engine()
        jsonl_sink = MockOutputSink()
        mock_producer = MockKafkaProducer(
            fail_on_produce=KafkaPublishError("Topic partition unavailable")
        )
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore

        # Composite sink with JSONL first, Kafka second
        composite = CompositeOutputSink([jsonl_sink, kafka_sink], fail_fast=True)
        runner = SimulationRunner(engine=engine, output_sink=composite, fail_fast=True)

        with pytest.raises(KafkaPublishError, match="Topic partition unavailable"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        assert metrics.sink_dispatch_failed == 1

        # Real distributed reality: JSONL received the first event before Kafka failed
        assert len(jsonl_sink.events) == 1
        assert jsonl_sink.events[0].container_id == "CONT_001"

    def test_flush_failure_normal_run(self) -> None:
        """Requirement 13 & 21: Flush failure propagates when no prior application exception exists."""
        engine = create_test_engine()
        sink = MockOutputSink(
            fail_on_flush=KafkaPublishError("Flush timeout: 2 unsent messages remained")
        )
        runner = SimulationRunner(engine=engine, output_sink=sink)

        with pytest.raises(KafkaPublishError, match="Flush timeout: 2 unsent messages"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        # Events were written, but overall run failed due to flush error
        assert metrics.status == "FAILED"
        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_succeeded == 6
        # Sinks were still closed
        assert sink.close_called == 1
        assert runner.is_closed is True

    def test_close_failure_normal_run(self) -> None:
        """Requirement 14: Close failure propagates when no prior application exception exists."""
        engine = create_test_engine()
        sink = MockOutputSink(fail_on_close=OSError("Unable to sync file descriptor on close"))
        runner = SimulationRunner(engine=engine, output_sink=sink, owns_sink=True)

        with pytest.raises(OSError, match="Unable to sync file descriptor on close"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_succeeded == 6
        assert runner.is_closed is True

    def test_cleanup_preserves_inflight_exception(self) -> None:
        """Requirement 15: Cleanup failure does NOT mask original in-flight application exception."""
        engine = create_test_engine()
        # Sink fails on first write AND also fails on flush and close
        sink = MockOutputSink(
            fail_on_event_idx=0,
            exception_to_raise=ValueError("Application validation failure"),
            fail_on_flush=RuntimeError("Secondary flush cleanup error"),
            fail_on_close=RuntimeError("Secondary close cleanup error"),
        )
        runner = SimulationRunner(engine=engine, output_sink=sink, owns_sink=True)

        # The original ValueError MUST propagate, NOT the secondary cleanup errors
        with pytest.raises(ValueError, match="Application validation failure"):
            runner.run(max_cycles=1)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        assert sink.flush_called == 1
        assert sink.close_called == 1
        assert runner.is_closed is True

    def test_correct_metrics_after_partial_cycle_failure(self) -> None:
        """Requirement 16: Correct metrics after mid-run failure across multiple cycles."""
        engine = create_test_engine()
        # Fail on the 8th event (2nd event of Cycle 2: 6 events in Cycle 1, 2nd event in Cycle 2 is idx 7)
        sink = MockOutputSink(
            fail_on_event_idx=7,
            exception_to_raise=RuntimeError("Simulated mid-stream network partition"),
        )
        runner = SimulationRunner(engine=engine, output_sink=sink, fail_fast=True)

        with pytest.raises(RuntimeError, match="Simulated mid-stream network partition"):
            runner.run(max_cycles=3)

        metrics = runner.get_metrics()
        assert metrics.status == "FAILED"
        # Cycle 1 completed fully: 6 events
        assert metrics.simulation_cycles_completed == 1
        # Cycle 2 failed mid-way:
        assert metrics.simulation_cycles_failed == 1
        # 6 in cycle 1 + 2 in cycle 2 = 8 events generated and attempted
        assert metrics.events_generated == 8
        assert metrics.sink_dispatch_attempted == 8
        assert metrics.sink_dispatch_succeeded == 7
        assert metrics.sink_dispatch_failed == 1

    def test_non_fail_fast_policy_continues_and_records_failures(self) -> None:
        """Verify fail_fast=False allows remaining events in cycle to be attempted."""
        engine = create_test_engine()
        # Fail only on the 2nd event (idx 1)
        sink = MockOutputSink(
            fail_on_event_idx=1,
            exception_to_raise=RuntimeError("Temporary write error on event 2"),
        )
        runner = SimulationRunner(engine=engine, output_sink=sink, fail_fast=False)

        metrics = runner.run(max_cycles=1)

        # Under fail_fast=False, all 6 events were attempted
        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_attempted == 6
        assert metrics.sink_dispatch_succeeded == 5
        assert metrics.sink_dispatch_failed == 1
        # Since cycle had a failure:
        assert metrics.simulation_cycles_failed == 1
        assert metrics.simulation_cycles_completed == 0
        assert metrics.status == "FAILED"

    def test_kafka_delivery_failure_in_kafka_only_mode_sets_failed_status(self) -> None:
        """Requirement: When all Kafka message deliveries time out/fail in Kafka mode, status is FAILED."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer(simulate_delivery_failure=True)
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink)

        metrics = runner.run(max_cycles=1)

        assert metrics.simulation_cycles_completed == 1
        assert metrics.simulation_cycles_failed == 0
        assert metrics.events_generated == 6
        assert metrics.sink_dispatch_succeeded == 6
        assert metrics.sink_dispatch_failed == 0
        assert metrics.kafka_delivery_acknowledged == 0
        assert metrics.kafka_delivery_failed == 6
        assert metrics.kafka_pending == 0
        assert metrics.status == "FAILED"

    def test_kafka_delivery_failure_in_composite_both_mode_sets_partial_failure_status(self) -> None:
        """Requirement: When JSONL succeeds but Kafka delivery fails in both mode, status is PARTIAL_FAILURE."""
        engine = create_test_engine()
        jsonl_sink = MockOutputSink()
        mock_producer = MockKafkaProducer(simulate_delivery_failure=True)
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore

        composite = CompositeOutputSink([jsonl_sink, kafka_sink])
        runner = SimulationRunner(engine=engine, output_sink=composite)

        metrics = runner.run(max_cycles=1)

        assert metrics.simulation_cycles_completed == 1
        assert metrics.simulation_cycles_failed == 0
        assert metrics.events_generated == 6
        assert len(jsonl_sink.events) == 6
        assert metrics.kafka_delivery_acknowledged == 0
        assert metrics.kafka_delivery_failed == 6
        assert metrics.kafka_pending == 0
        assert metrics.status == "PARTIAL_FAILURE"

    def test_kafka_pending_messages_after_flush_sets_partial_failure_status(self) -> None:
        """Requirement: When messages remain unacknowledged after flush, status is PARTIAL_FAILURE."""
        engine = create_test_engine()
        mock_producer = MockKafkaProducer(simulate_pending_count=2)
        kafka_sink = KafkaSink(producer=mock_producer)  # type: ignore
        runner = SimulationRunner(engine=engine, output_sink=kafka_sink)

        metrics = runner.run(max_cycles=1)

        assert metrics.simulation_cycles_completed == 1
        assert metrics.events_generated == 6
        assert metrics.kafka_delivery_acknowledged == 4
        assert metrics.kafka_pending == 2
        assert metrics.status == "PARTIAL_FAILURE"
