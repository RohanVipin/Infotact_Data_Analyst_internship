"""Command-line interface and application entrypoint for the AtmoSync Simulator.

Provides terminal commands to execute the unified SimulationRunner with selectable
output sinks (JSONL, Kafka, or both), cycle limits, logging thresholds, and graceful
OS signal handling (SIGINT / SIGTERM).

Architectural Principles:
1. Pure Adapter / No Duplicate Loops: Reuses existing ConfigLoader, SimulationEngine,
   OutputSink adapters, and SimulationRunner without reimplementing pipeline logic.
2. Robust Resource Ownership: Sinks created by the CLI are owned and closed by the runner.
   Partial sink construction failures ensure previously created resources are cleaned up.
3. Safe Signal Coordination: Signal handlers request runner.stop() non-blockingly, allowing
   in-flight cycles to complete and drain before exit in the main thread.
4. Clean Exit Codes: Predictable status codes for success (0), error (1), invalid args (2),
   SIGINT interrupt (130), and SIGTERM termination (143).
"""

import argparse
from datetime import datetime
import logging
from pathlib import Path
import signal
import sys
import threading
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

from simulation.config.config_loader import AppConfig, ConfigLoader, ConfigurationError
from simulation.kafka.exceptions import KafkaBrokerError, KafkaConfigurationError
from simulation.kafka.kafka_config import KafkaConfig
from simulation.output.jsonl_writer import JsonlWriterError
from simulation.output.sink import CompositeOutputSink, JsonlSink, KafkaSink, OutputSink
from simulation.runner import RunnerMetrics, SimulationRunner
from simulation.simulator.simulation_engine import SimulationEngine

logger = logging.getLogger("simulation.cli")

# Standard Process Exit Codes
EXIT_SUCCESS: int = 0
EXIT_FAILURE: int = 1
EXIT_USAGE_ERROR: int = 2
EXIT_INTERRUPTED: int = 130  # 128 + SIGINT (2)
EXIT_TERMINATED: int = 143   # 128 + SIGTERM (15)

DEFAULT_CONFIG_PATH: str = "simulation/config/simulation_config.yaml"
DEFAULT_JSONL_PATH: str = "data/telemetry.jsonl"
VALID_OUTPUT_MODES: Sequence[str] = ("both", "jsonl", "kafka")


def create_output_sink(
    output_mode: str,
    jsonl_path: Union[str, Path] = DEFAULT_JSONL_PATH,
    kafka_config: Optional[KafkaConfig] = None,
) -> OutputSink:
    """Factory creating the appropriate OutputSink destination.

    Guarantees that partial sink construction failures safely close resources
    that were already initialized.

    Args:
        output_mode: Destination mode ('both', 'jsonl', 'kafka').
        jsonl_path: Output file path for JSONL persistence.
        kafka_config: Optional Kafka configuration settings.

    Returns:
        Configured OutputSink instance.

    Raises:
        ValueError: If output_mode is invalid.
        Exception: If sink construction fails (all created resources are cleaned up).
    """
    mode = str(output_mode).strip().lower()
    if mode not in VALID_OUTPUT_MODES:
        raise ValueError(
            f"Invalid output mode '{output_mode}'. Allowed modes: {list(VALID_OUTPUT_MODES)}"
        )

    if mode == "jsonl":
        return JsonlSink(output_path=jsonl_path)

    if mode == "kafka":
        return KafkaSink(config=kafka_config)

    # mode == "both"
    jsonl_sink: Optional[JsonlSink] = None
    kafka_sink: Optional[KafkaSink] = None
    try:
        jsonl_sink = JsonlSink(output_path=jsonl_path)
        kafka_sink = KafkaSink(config=kafka_config)
        return CompositeOutputSink([jsonl_sink, kafka_sink], fail_fast=True)
    except Exception as exc:
        # Partial construction rollback: close already opened resources
        if jsonl_sink is not None:
            try:
                jsonl_sink.close()
            except Exception as cleanup_err:
                logger.error("Error closing JsonlSink during partial construction cleanup: %s", cleanup_err)
        if kafka_sink is not None:
            try:
                kafka_sink.close()
            except Exception as cleanup_err:
                logger.error("Error closing KafkaSink during partial construction cleanup: %s", cleanup_err)
        raise exc


class SignalContext:
    """Context manager for registering OS signal handlers during CLI execution.

    Coordinates graceful shutdown by invoking runner.stop() non-blockingly upon
    SIGINT or SIGTERM and restoring previous signal handlers upon exit.
    """

    def __init__(self, runner: SimulationRunner) -> None:
        self.runner = runner
        self.interrupted: bool = False
        self.interrupted_signal: Optional[int] = None
        self._prev_handlers: Dict[int, Any] = {}

    def _signal_handler(self, signum: int, frame: Any) -> None:
        sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
        logger.warning(
            "Received signal %d (%s); requesting simulation runner to stop gracefully...",
            signum,
            sig_name,
        )
        self.interrupted = True
        self.interrupted_signal = signum
        # Non-blocking signal handler: only request stop, do not block or flush inside handler
        self.runner.stop()

    def __enter__(self) -> "SignalContext":
        if threading.current_thread() is threading.main_thread():
            targets = [signal.SIGINT]
            if hasattr(signal, "SIGTERM"):
                targets.append(signal.SIGTERM)
            if hasattr(signal, "SIGBREAK"):
                targets.append(signal.SIGBREAK)

            for sig in targets:
                try:
                    self._prev_handlers[sig] = signal.signal(sig, self._signal_handler)
                except (ValueError, OSError) as exc:
                    logger.debug("Could not register signal %s: %s", sig, exc)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if threading.current_thread() is threading.main_thread():
            for sig, prev_handler in self._prev_handlers.items():
                try:
                    signal.signal(sig, prev_handler)
                except (ValueError, OSError) as exc:
                    logger.debug("Could not restore signal %s: %s", sig, exc)


def format_summary(metrics: RunnerMetrics) -> str:
    """Format an execution metrics snapshot into a human-readable CLI summary."""
    lines = [
        "=" * 60,
        "          AtmoSync Simulation Pipeline Summary",
        "=" * 60,
        f"Status:                      {metrics.status}",
        f"Wall-Clock Duration:         {metrics.duration_seconds:.2f}s",
        f"Simulation Cycles Completed: {metrics.simulation_cycles_completed}",
        f"Simulation Cycles Failed:    {metrics.simulation_cycles_failed}",
        f"Events Generated:            {metrics.events_generated}",
        f"Sink Dispatches Attempted:   {metrics.sink_dispatch_attempted}",
        f"Sink Dispatches Succeeded:   {metrics.sink_dispatch_succeeded}",
        f"Sink Dispatches Failed:      {metrics.sink_dispatch_failed}",
    ]
    if metrics.kafka_delivery_acknowledged is not None:
        lines.extend([
            f"Kafka Confirmed Receipts:    {metrics.kafka_delivery_acknowledged}",
            f"Kafka Failed Deliveries:     {metrics.kafka_delivery_failed or 0}",
            f"Kafka In-Flight Pending:     {metrics.kafka_pending or 0}",
        ])
    lines.append("=" * 60)
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    """Construct the command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="python -m simulation",
        description="AtmoSync IoT Sensor Simulation Pipeline Runner",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config",
        dest="config",
        type=str,
        default=DEFAULT_CONFIG_PATH,
        help="Path to YAML configuration file",
    )
    parser.add_argument(
        "-n", "--cycles",
        dest="cycles",
        type=int,
        default=None,
        help="Maximum simulation cycles to execute (positive integer; default: continuous)",
    )
    parser.add_argument(
        "-m", "--output-mode",
        dest="output_mode",
        type=str,
        choices=list(VALID_OUTPUT_MODES),
        default="both",
        help="Telemetry output destination mode",
    )
    parser.add_argument(
        "--jsonl-path",
        dest="jsonl_path",
        type=str,
        default=DEFAULT_JSONL_PATH,
        help="Output file path for JSONL records",
    )
    parser.add_argument(
        "--bootstrap-servers",
        dest="bootstrap_servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (overrides ATMOSYNC_KAFKA_BOOTSTRAP_SERVERS)",
    )
    parser.add_argument(
        "--topic",
        dest="topic",
        type=str,
        default=None,
        help="Kafka destination topic name (overrides ATMOSYNC_KAFKA_TOPIC)",
    )
    parser.add_argument(
        "-l", "--log-level",
        dest="log_level",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Logging level threshold",
    )
    parser.add_argument(
        "-v", "--version",
        action="version",
        version="AtmoSync Simulator v0.1.0",
    )
    return parser


def parse_args(args: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse and validate command-line arguments.

    Args:
        args: Optional list of argument strings.

    Returns:
        Validated argparse.Namespace instance.

    Raises:
        SystemExit: If argument parsing or semantic validation fails.
    """
    parser = build_parser()
    parsed = parser.parse_args(args)

    # Semantic validation for cycles count
    if parsed.cycles is not None and parsed.cycles <= 0:
        parser.error(f"Maximum simulation cycles must be a positive integer, got {parsed.cycles}")

    return parsed


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Primary application entrypoint executing the AtmoSync simulation pipeline.

    Args:
        argv: Optional command-line argument list (defaults to sys.argv[1:]).

    Returns:
        Integer process exit code.
    """
    try:
        args = parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else EXIT_USAGE_ERROR

    # Configure logging
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format=log_format,
        force=True,
    )

    logger.info("Starting AtmoSync simulation pipeline (output_mode=%s)", args.output_mode)

    # 1. Load application configuration
    try:
        config = ConfigLoader.load(args.config)
        logger.info(
            "Loaded configuration from '%s': %d containers, interval=%.2fs",
            args.config,
            len(config.containers),
            config.simulation.interval_seconds,
        )
    except ConfigurationError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_FAILURE
    except Exception as exc:
        logger.error("Unexpected error loading configuration: %s", exc)
        return EXIT_FAILURE

    # 2. Build Kafka configuration if Kafka output is active
    kafka_config: Optional[KafkaConfig] = None
    if args.output_mode in ("both", "kafka"):
        overrides: Dict[str, Any] = {}
        if args.bootstrap_servers:
            overrides["bootstrap_servers"] = args.bootstrap_servers
        if args.topic:
            overrides["topic"] = args.topic

        try:
            kafka_config = KafkaConfig.from_env(**overrides)
            # Log safe masked Kafka config without secrets
            logger.info(
                "Configured Kafka producer: bootstrap_servers=%s, topic=%s, client_id=%s, acks=%s",
                kafka_config.bootstrap_servers,
                kafka_config.topic,
                kafka_config.client_id,
                kafka_config.acks,
            )
        except KafkaConfigurationError as exc:
            logger.error("Kafka configuration validation failed: %s", exc)
            return EXIT_FAILURE
        except Exception as exc:
            logger.error("Failed to initialize Kafka configuration: %s", exc)
            return EXIT_FAILURE

    # 3. Create output sink with partial-failure cleanup
    try:
        output_sink = create_output_sink(
            output_mode=args.output_mode,
            jsonl_path=args.jsonl_path,
            kafka_config=kafka_config,
        )
    except (KafkaBrokerError, JsonlWriterError) as exc:
        logger.error("Failed to initialize output destination: %s", exc)
        return EXIT_FAILURE
    except Exception as exc:
        logger.error("Unexpected error constructing output sink: %s", exc)
        return EXIT_FAILURE

    # 4. Initialize engine and runner
    try:
        engine = SimulationEngine(config=config)
        runner = SimulationRunner(
            engine=engine,
            output_sink=output_sink,
            fail_fast=True,
            owns_sink=True,
        )
    except Exception as exc:
        logger.error("Failed to initialize simulation engine or runner: %s", exc)
        try:
            output_sink.close()
        except Exception:
            pass
        return EXIT_FAILURE

    # 5. Execute runner under OS signal management
    interrupted = False
    interrupted_sig = None

    with SignalContext(runner) as sig_ctx:
        try:
            metrics = runner.run(max_cycles=args.cycles)
            interrupted = sig_ctx.interrupted
            interrupted_sig = sig_ctx.interrupted_signal
        except Exception as exc:
            logger.error("Simulation run aborted due to error: %s", exc)
            print(format_summary(runner.get_metrics()))
            return EXIT_FAILURE

    # 6. Display execution summary and return status code
    print(format_summary(metrics))

    if interrupted:
        logger.warning("Simulation was interrupted by signal (%s)", interrupted_sig)
        if interrupted_sig == getattr(signal, "SIGTERM", None):
            return EXIT_TERMINATED
        return EXIT_INTERRUPTED

    if metrics.status == "COMPLETED":
        logger.info("Simulation completed successfully.")
        return EXIT_SUCCESS
    elif metrics.status == "STOPPED":
        return EXIT_INTERRUPTED
    else:
        logger.error("Simulation finished with failure status: %s", metrics.status)
        return EXIT_FAILURE
