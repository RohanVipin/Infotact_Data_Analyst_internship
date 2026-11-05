"""Unit tests for AtmoSync command-line interface arguments, options, and sink factories."""

from pathlib import Path
import pytest
from unittest.mock import MagicMock, patch

from simulation.cli import (
    DEFAULT_CONFIG_PATH,
    DEFAULT_JSONL_PATH,
    EXIT_FAILURE,
    EXIT_SUCCESS,
    EXIT_USAGE_ERROR,
    VALID_OUTPUT_MODES,
    build_parser,
    create_output_sink,
    format_summary,
    main,
    parse_args,
)
from simulation.kafka.exceptions import KafkaConfigurationError
from simulation.kafka.kafka_config import KafkaConfig
from simulation.output.sink import CompositeOutputSink, JsonlSink, KafkaSink
from simulation.runner import RunnerMetrics


class TestCliArgumentParser:
    """Tests for CLI argument parsing and validation."""

    def test_default_arguments(self) -> None:
        args = parse_args([])
        assert args.config == DEFAULT_CONFIG_PATH
        assert args.cycles is None
        assert args.output_mode == "both"
        assert args.jsonl_path == DEFAULT_JSONL_PATH
        assert args.bootstrap_servers is None
        assert args.topic is None
        assert args.log_level == "INFO"

    def test_valid_custom_arguments(self, tmp_path: Path) -> None:
        custom_cfg = str(tmp_path / "custom.yaml")
        custom_jsonl = str(tmp_path / "out.jsonl")
        argv = [
            "-c", custom_cfg,
            "-n", "5",
            "-m", "jsonl",
            "--jsonl-path", custom_jsonl,
            "--bootstrap-servers", "broker1:9092,broker2:9092",
            "--topic", "custom.telemetry",
            "-l", "DEBUG",
        ]
        args = parse_args(argv)
        assert args.config == custom_cfg
        assert args.cycles == 5
        assert args.output_mode == "jsonl"
        assert args.jsonl_path == custom_jsonl
        assert args.bootstrap_servers == "broker1:9092,broker2:9092"
        assert args.topic == "custom.telemetry"
        assert args.log_level == "DEBUG"

    def test_invalid_cycle_counts_rejected(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--cycles", "0"])
        assert exc_info.value.code == EXIT_USAGE_ERROR

        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--cycles", "-10"])
        assert exc_info.value.code == EXIT_USAGE_ERROR

        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--cycles", "abc"])
        assert exc_info.value.code == EXIT_USAGE_ERROR

    def test_invalid_output_mode_rejected(self) -> None:
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--output-mode", "snowflake"])
        assert exc_info.value.code == EXIT_USAGE_ERROR

    def test_help_flag_displays_usage_and_exits(self, capsys: pytest.CaptureFixture[str]) -> None:
        code = main(["--help"])
        assert code == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "AtmoSync IoT Sensor Simulation Pipeline Runner" in captured.out
        assert "--config" in captured.out
        assert "--cycles" in captured.out
        assert "--output-mode" in captured.out

    def test_version_flag_displays_version_and_exits(self, capsys: pytest.CaptureFixture[str]) -> None:
        code = main(["--version"])
        assert code == EXIT_SUCCESS
        captured = capsys.readouterr()
        assert "AtmoSync Simulator v0.1.0" in captured.out


class TestOutputSinkFactory:
    """Tests for the create_output_sink factory."""

    def test_create_jsonl_only_sink(self, tmp_path: Path) -> None:
        path = tmp_path / "test.jsonl"
        sink = create_output_sink(output_mode="jsonl", jsonl_path=path)
        assert isinstance(sink, JsonlSink)
        assert sink.output_path == path

    def test_create_kafka_only_sink(self) -> None:
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        with patch("simulation.output.sink.SensorEventKafkaProducer", return_value=mock_producer):
            sink = create_output_sink(output_mode="kafka")
            assert isinstance(sink, KafkaSink)

    def test_create_dual_composite_sink(self, tmp_path: Path) -> None:
        path = tmp_path / "dual.jsonl"
        mock_producer = MagicMock()
        mock_producer.is_closed = False
        with patch("simulation.output.sink.SensorEventKafkaProducer", return_value=mock_producer):
            sink = create_output_sink(output_mode="both", jsonl_path=path)
            assert isinstance(sink, CompositeOutputSink)
            assert len(sink.sinks) == 2
            assert isinstance(sink.sinks[0], JsonlSink)
            assert isinstance(sink.sinks[1], KafkaSink)

    def test_invalid_output_mode_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Invalid output mode 'database'"):
            create_output_sink(output_mode="database")

    def test_partial_construction_failure_cleans_up_jsonl(self, tmp_path: Path) -> None:
        path = tmp_path / "partial.jsonl"
        # Simulate Kafka failure during 'both' construction
        with patch("simulation.cli.KafkaSink", side_effect=KafkaConfigurationError("Broker misconfigured")):
            with pytest.raises(KafkaConfigurationError, match="Broker misconfigured"):
                create_output_sink(output_mode="both", jsonl_path=path)


class TestCliSummaryAndSecurity:
    """Tests for CLI summary formatting and security protections."""

    def test_format_summary_with_kafka(self) -> None:
        metrics = RunnerMetrics(
            events_generated=10,
            sink_dispatch_attempted=10,
            sink_dispatch_succeeded=10,
            sink_dispatch_failed=0,
            simulation_cycles_completed=2,
            simulation_cycles_failed=0,
            kafka_delivery_acknowledged=10,
            kafka_delivery_failed=0,
            kafka_pending=0,
            status="COMPLETED",
            duration_seconds=1.234,
        )
        summary = format_summary(metrics)
        assert "Status:                      COMPLETED" in summary
        assert "Events Generated:            10" in summary
        assert "Kafka Confirmed Receipts:    10" in summary
        assert "Kafka In-Flight Pending:     0" in summary

    def test_format_summary_without_kafka(self) -> None:
        metrics = RunnerMetrics(
            events_generated=5,
            sink_dispatch_attempted=5,
            sink_dispatch_succeeded=5,
            simulation_cycles_completed=1,
            status="COMPLETED",
            duration_seconds=0.5,
        )
        summary = format_summary(metrics)
        assert "Status:                      COMPLETED" in summary
        assert "Kafka Confirmed Receipts" not in summary

    def test_no_kafka_password_logged(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        """Verify that CLI logs do not expose sensitive credentials in plaintext."""
        config_file = Path("simulation/config/simulation_config.yaml")
        if not config_file.exists():
            pytest.skip("Default config file not found")

        with patch.dict("os.environ", {"ATMOSYNC_KAFKA_SASL_PASSWORD": "SuperSecretPassword123!"}):
            # Mock producer so live connection is not attempted
            mock_producer = MagicMock()
            mock_producer.is_closed = False
            mock_producer.flush.return_value = 0
            with patch("simulation.output.sink.SensorEventKafkaProducer", return_value=mock_producer):
                main([
                    "-c", str(config_file),
                    "-n", "1",
                    "-m", "kafka",
                    "--jsonl-path", str(tmp_path / "t.jsonl"),
                ])

        log_text = caplog.text
        assert "SuperSecretPassword123!" not in log_text
