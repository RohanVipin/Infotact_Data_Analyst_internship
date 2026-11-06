"""Lifecycle, execution, and graceful shutdown tests for the AtmoSync CLI."""

from pathlib import Path
import signal
from typing import Any, List
from unittest.mock import MagicMock, patch
import pytest

from simulation.cli import (
    EXIT_FAILURE,
    EXIT_INTERRUPTED,
    EXIT_SUCCESS,
    EXIT_TERMINATED,
    EXIT_USAGE_ERROR,
    SignalContext,
    main,
)
from simulation.config.config_loader import AppConfig, SensorConfig, SimulationSettings
from simulation.events.event_schema import SensorEvent
from simulation.output.sink import OutputSink
from simulation.runner import SimulationRunner
from simulation.simulator.simulation_engine import SimulationEngine


def create_mock_app_config() -> AppConfig:
    return AppConfig(
        simulation=SimulationSettings(interval_seconds=0.01, random_seed=42),
        containers=("CLI_CONT_001", "CLI_CONT_002"),
        sensors={
            "temperature": SensorConfig(enabled=True, unit="celsius"),
            "humidity": SensorConfig(enabled=True, unit="percent"),
            "vibration": SensorConfig(enabled=True, unit="m/s^2"),
        },
    )


class MockSink(OutputSink):
    def __init__(self) -> None:
        self.events: List[SensorEvent] = []
        self.flush_called = 0
        self.close_called = 0
        self.is_closed = False

    def write_event(self, event: SensorEvent) -> None:
        self.events.append(event)

    def flush(self) -> None:
        self.flush_called += 1

    def close(self) -> None:
        self.close_called += 1
        self.is_closed = True


class TestCliLifecycleExecution:
    """Tests application lifecycle execution via main()."""

    def test_missing_config_file_returns_exit_failure(self, tmp_path: Path) -> None:
        non_existent = str(tmp_path / "does_not_exist.yaml")
        code = main(["-c", non_existent])
        assert code == EXIT_FAILURE

    def test_invalid_argument_returns_usage_error(self) -> None:
        code = main(["--cycles", "-5"])
        assert code == EXIT_USAGE_ERROR

    def test_finite_run_success_jsonl_mode(self, tmp_path: Path) -> None:
        """Requirement 10: Finite simulation invocation via CLI returns EXIT_SUCCESS."""
        jsonl_path = tmp_path / "cli_test.jsonl"
        cfg = create_mock_app_config()

        with patch("simulation.cli.ConfigLoader.load", return_value=cfg):
            code = main([
                "-c", "dummy_config.yaml",
                "-n", "2",
                "-m", "jsonl",
                "--jsonl-path", str(jsonl_path),
            ])

        assert code == EXIT_SUCCESS
        assert jsonl_path.exists()
        lines = jsonl_path.read_text(encoding="utf-8").strip().splitlines()
        # 2 containers * 3 sensors * 2 cycles = 12 events
        assert len(lines) == 12

    def test_finite_run_success_dual_sink_mocked_kafka(self, tmp_path: Path) -> None:
        jsonl_path = tmp_path / "cli_dual.jsonl"
        cfg = create_mock_app_config()

        mock_producer = MagicMock()
        mock_producer.is_closed = False
        mock_producer.flush.return_value = 0
        mock_producer.close.return_value = 0

        with patch("simulation.cli.ConfigLoader.load", return_value=cfg):
            with patch("simulation.output.sink.SensorEventKafkaProducer", return_value=mock_producer):
                code = main([
                    "-c", "dummy_config.yaml",
                    "-n", "1",
                    "-m", "both",
                    "--jsonl-path", str(jsonl_path),
                ])

        assert code == EXIT_SUCCESS
        assert jsonl_path.exists()
        assert len(jsonl_path.read_text(encoding="utf-8").strip().splitlines()) == 6
        assert mock_producer.flush.called
        assert mock_producer.close.called

    def test_simulation_failure_returns_exit_failure(self, tmp_path: Path) -> None:
        """Requirement 17: Exception propagation during execution returns EXIT_FAILURE."""
        cfg = create_mock_app_config()
        jsonl_path = tmp_path / "failing.jsonl"

        with patch("simulation.cli.ConfigLoader.load", return_value=cfg):
            with patch.object(SimulationEngine, "run_cycle", side_effect=RuntimeError("Engine hardware fault")):
                code = main([
                    "-c", "dummy_config.yaml",
                    "-n", "1",
                    "-m", "jsonl",
                    "--jsonl-path", str(jsonl_path),
                ])

        assert code == EXIT_FAILURE

    def test_delivery_failure_in_both_mode_returns_exit_failure(self, tmp_path: Path) -> None:
        """Requirement: When Kafka delivery fails in both mode, CLI returns EXIT_FAILURE due to PARTIAL_FAILURE."""
        jsonl_path = tmp_path / "cli_partial.jsonl"
        cfg = create_mock_app_config()

        mock_producer = MagicMock()
        mock_producer.is_closed = False
        mock_producer.flush.return_value = 0
        mock_producer.close.return_value = 0
        mock_stats = MagicMock()
        mock_stats.delivered = 0
        mock_stats.failed = 6
        mock_stats.pending = 0
        mock_producer.stats = mock_stats

        with patch("simulation.cli.ConfigLoader.load", return_value=cfg):
            with patch("simulation.output.sink.SensorEventKafkaProducer", return_value=mock_producer):
                code = main([
                    "-c", "dummy_config.yaml",
                    "-n", "1",
                    "-m", "both",
                    "--jsonl-path", str(jsonl_path),
                ])

        assert code == EXIT_FAILURE
        assert jsonl_path.exists()


class TestCliSignalHandlingAndGracefulShutdown:
    """Tests graceful OS signal handling, cleanup, and handler restoration."""

    def test_signal_context_restores_previous_handlers(self) -> None:
        """Requirement 14: Signal handler restoration upon context exit."""
        dummy_runner = MagicMock()
        orig_sigint = signal.getsignal(signal.SIGINT)

        with SignalContext(dummy_runner):
            # Inside context, handler was swapped
            current_sigint = signal.getsignal(signal.SIGINT)
            assert current_sigint != orig_sigint

        # Outside context, handler is restored
        assert signal.getsignal(signal.SIGINT) == orig_sigint

    def test_sigint_triggers_runner_stop_non_blockingly(self) -> None:
        """Requirement 12: SIGINT requests runner.stop() and records interruption."""
        mock_runner = MagicMock()
        with SignalContext(mock_runner) as sig_ctx:
            # Simulate receiving SIGINT
            sig_ctx._signal_handler(signal.SIGINT, None)
            assert sig_ctx.interrupted is True
            assert sig_ctx.interrupted_signal == signal.SIGINT
            assert mock_runner.stop.called

    def test_sigterm_triggers_runner_stop_and_exit_terminated(self) -> None:
        """Requirement 13: SIGTERM requests runner.stop() and records termination signal."""
        mock_runner = MagicMock()
        with SignalContext(mock_runner) as sig_ctx:
            if hasattr(signal, "SIGTERM"):
                sig_ctx._signal_handler(signal.SIGTERM, None)
                assert sig_ctx.interrupted is True
                assert sig_ctx.interrupted_signal == signal.SIGTERM
                assert mock_runner.stop.called

    def test_interrupted_run_returns_exit_interrupted(self, tmp_path: Path) -> None:
        """Requirement 11 & 16: Interrupted continuous execution returns EXIT_INTERRUPTED."""
        cfg = create_mock_app_config()
        jsonl_path = tmp_path / "interrupted.jsonl"

        # Intercept SimulationEngine to inject a stopping sleep_fn
        original_engine_init = SimulationEngine.__init__

        def intercepted_engine_init(self_engine, *args, **kwargs):
            # Trigger SIGINT during interval sleep after cycle 1
            def stopping_sleep(_):
                # Send SIGINT signal to current process / signal context
                for sig in (signal.SIGINT,):
                    handler = signal.getsignal(sig)
                    if callable(handler):
                        handler(sig, None)

            kwargs["sleep_fn"] = stopping_sleep
            original_engine_init(self_engine, *args, **kwargs)

        with patch("simulation.cli.ConfigLoader.load", return_value=cfg):
            with patch.object(SimulationEngine, "__init__", side_effect=intercepted_engine_init, autospec=True):
                code = main([
                    "-c", "dummy_config.yaml",
                    "-m", "jsonl",
                    "--jsonl-path", str(jsonl_path),
                ])

        assert code == EXIT_INTERRUPTED

    def test_cli_flush_and_close_executed_on_exit(self, tmp_path: Path) -> None:
        """Requirement 15: Resources are flushed and closed upon CLI completion."""
        mock_sink = MockSink()
        cfg = create_mock_app_config()

        with patch("simulation.cli.ConfigLoader.load", return_value=cfg):
            with patch("simulation.cli.create_output_sink", return_value=mock_sink):
                code = main([
                    "-c", "dummy_config.yaml",
                    "-n", "1",
                    "-m", "jsonl",
                ])

        assert code == EXIT_SUCCESS
        assert mock_sink.flush_called == 1
        assert mock_sink.close_called == 1
        assert mock_sink.is_closed is True
