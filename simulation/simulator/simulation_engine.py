"""Simulation engine orchestrating multi-container simulation cycles.

Coordinates ContainerSimulator instances, timing intervals, and event emission.
Supports both finite cycles (for testing and verification) and continuous streaming.
"""

from datetime import datetime
import logging
import time
from typing import Callable, Generator, List, Mapping, Optional, Union

from simulation.config.config_loader import AppConfig
from simulation.events.event_builder import EventBuilder
from simulation.events.event_schema import SensorEvent
from simulation.simulator.container_simulator import ContainerSimulator

logger = logging.getLogger(__name__)


class SimulationEngine:
    """Multi-container simulation coordinator.

    Attributes:
        config: The AppConfig driving simulation parameters.
        containers: Mapping of container IDs to their ContainerSimulator instances.
    """

    def __init__(
        self,
        config: AppConfig,
        event_builder: Optional[EventBuilder] = None,
        sleep_fn: Optional[Callable[[float], None]] = None,
        containers: Optional[Mapping[str, ContainerSimulator]] = None,
        fail_fast: bool = True,
    ) -> None:
        """Initialize the simulation engine.

        Args:
            config: Validated AppConfig instance.
            event_builder: Optional shared EventBuilder instance.
            sleep_fn: Callable used for interval delays. Defaults to time.sleep.
                Can be mocked or injected as a no-op for instantaneous testing.
            containers: Optional pre-configured container mapping (for testing).
            fail_fast: If True (default), exceptions during container simulation
                immediately propagate. If False, failures are logged and recorded
                in last_cycle_failed_containers, allowing other containers to proceed.
        """
        self._config = config
        self._event_builder = event_builder or EventBuilder()
        self._sleep_fn = sleep_fn if sleep_fn is not None else time.sleep
        self._fail_fast = fail_fast
        self._stopped = False
        self._last_cycle_failed_containers: List[str] = []

        if containers is not None:
            self._containers = dict(containers)
        else:
            self._containers = self._initialize_containers()

    @property
    def fail_fast(self) -> bool:
        """Return True if the engine fails fast on container exceptions."""
        return self._fail_fast

    @property
    def last_cycle_failed_containers(self) -> List[str]:
        """Return list of container IDs that failed during the most recent cycle."""
        return list(self._last_cycle_failed_containers)

    @property
    def config(self) -> AppConfig:
        """Return the simulation configuration."""
        return self._config

    @property
    def containers(self) -> Mapping[str, ContainerSimulator]:
        """Return the dictionary of active container simulators."""
        return self._containers

    @property
    def is_stopped(self) -> bool:
        """Return True if the engine has been requested to stop."""
        return self._stopped

    def stop(self) -> None:
        """Request the engine to gracefully halt continuous execution."""
        self._stopped = True

    def _initialize_containers(self) -> Mapping[str, ContainerSimulator]:
        """Create a ContainerSimulator for each container ID in configuration."""
        container_map: dict[str, ContainerSimulator] = {}
        master_seed = self._config.simulation.random_seed

        for container_id in self._config.containers:
            container_map[container_id] = ContainerSimulator(
                container_id=container_id,
                sensor_configs=self._config.sensors,
                master_seed=master_seed,
                event_builder=self._event_builder,
            )

        return container_map

    def run_cycle(
        self,
        observed_at: Optional[Union[str, datetime]] = None,
    ) -> List[SensorEvent]:
        """Execute one complete cycle across all configured containers.

        Iterates through all containers in configured order and collects all
        generated sensor events.

        Args:
            observed_at: Optional fixed observation timestamp for all readings.

        Returns:
            List of SensorEvent objects generated across all containers.
        """
        cycle_events: List[SensorEvent] = []
        self._last_cycle_failed_containers = []

        for container_id, simulator in self._containers.items():
            try:
                events = simulator.simulate_cycle(observed_at=observed_at)
                cycle_events.extend(events)
            except Exception as exc:
                self._last_cycle_failed_containers.append(container_id)
                logger.error(
                    "Simulation failure for container '%s': %s",
                    container_id,
                    exc,
                    exc_info=True,
                )
                if self._fail_fast:
                    raise

        return cycle_events

    def run(
        self,
        max_cycles: Optional[int] = None,
        callback: Optional[Callable[[SensorEvent], None]] = None,
    ) -> List[SensorEvent]:
        """Run the simulation for a finite number of cycles or continuously.

        Args:
            max_cycles: Positive integer number of cycles to execute, or None
                for continuous execution until stop() is called.
            callback: Optional callback invoked for each generated event.

        Returns:
            List of all SensorEvent instances generated across the entire run.

        Raises:
            ValueError: If max_cycles is <= 0.
        """
        if max_cycles is not None and max_cycles <= 0:
            raise ValueError(f"max_cycles must be a positive integer, got {max_cycles}")

        self._stopped = False
        all_events: List[SensorEvent] = []
        cycles_completed = 0
        interval = self._config.simulation.interval_seconds

        while not self._stopped:
            if max_cycles is not None and cycles_completed >= max_cycles:
                break

            cycle_events = self.run_cycle()
            for event in cycle_events:
                all_events.append(event)
                if callback is not None:
                    try:
                        callback(event)
                    except Exception as exc:
                        logger.error("Error in event callback: %s", exc)

            cycles_completed += 1

            # Only sleep between cycles if another cycle remains
            if not self._stopped and (max_cycles is None or cycles_completed < max_cycles):
                self._sleep_fn(interval)

        return all_events

    def stream(
        self,
        max_cycles: Optional[int] = None,
    ) -> Generator[SensorEvent, None, None]:
        """Generator interface yielding events one by one as they are produced.

        Args:
            max_cycles: Number of cycles to yield events from, or None for continuous.

        Yields:
            SensorEvent instances.
        """
        if max_cycles is not None and max_cycles <= 0:
            raise ValueError(f"max_cycles must be a positive integer, got {max_cycles}")

        self._stopped = False
        cycles_completed = 0
        interval = self._config.simulation.interval_seconds

        while not self._stopped:
            if max_cycles is not None and cycles_completed >= max_cycles:
                break

            cycle_events = self.run_cycle()
            for event in cycle_events:
                yield event

            cycles_completed += 1

            if not self._stopped and (max_cycles is None or cycles_completed < max_cycles):
                self._sleep_fn(interval)
