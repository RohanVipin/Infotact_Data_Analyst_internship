"""Container simulator representing an individual shipping container with virtual sensors.

Coordinates virtual sensors for a specific container and generates canonical SensorEvent
instances per cycle following Option A (one event per sensor reading).
"""

from datetime import datetime
import logging
from typing import Dict, List, Mapping, Optional, Union

from simulation.config.config_loader import SensorConfig
from simulation.events.event_builder import EventBuilder
from simulation.events.event_schema import SensorEvent
from simulation.sensors.base_sensor import BaseSensor
from simulation.sensors.humidity_sensor import HumiditySensor
from simulation.sensors.temperature_sensor import TemperatureSensor
from simulation.sensors.vibration_sensor import VibrationSensor

logger = logging.getLogger(__name__)


class ContainerSimulator:
    """Simulates an individual container equipped with environmental IoT sensors.

    Attributes:
        container_id: Unique string identifier for this container.
        sensors: Read-only mapping of initialized sensor instances.
    """

    def __init__(
        self,
        container_id: str,
        sensor_configs: Mapping[str, SensorConfig],
        master_seed: Optional[int] = None,
        event_builder: Optional[EventBuilder] = None,
        sensors: Optional[Mapping[str, BaseSensor]] = None,
        propagate_sampling_errors: bool = False,
    ) -> None:
        """Initialize the container simulator.

        Args:
            container_id: Unique container identifier string.
            sensor_configs: Mapping of sensor configurations from AppConfig.
            master_seed: Optional master seed. If supplied, container-specific seeds
                are derived using SHA-256 to ensure complete stream isolation between
                containers.
            event_builder: Optional custom EventBuilder instance.
            sensors: Optional injected mapping of BaseSensor instances (for testing).
            propagate_sampling_errors: If True, re-raises recoverable ValueError from
                sensor sampling rather than logging and continuing. Default False.

        Raises:
            ValueError: If container_id is empty or missing required sensor configs.
        """
        if not isinstance(container_id, str) or not container_id.strip():
            raise ValueError(f"container_id must be a non-empty string, got {repr(container_id)}")

        self._container_id = container_id.strip()
        self._event_builder = event_builder or EventBuilder()
        self._propagate_sampling_errors = propagate_sampling_errors

        if sensors is not None:
            self._sensors = dict(sensors)
        else:
            self._sensors = self._initialize_sensors(sensor_configs, master_seed)

    @property
    def container_id(self) -> str:
        """Return the container identifier."""
        return self._container_id

    @property
    def sensors(self) -> Mapping[str, BaseSensor]:
        """Return the mapping of sensors attached to this container."""
        return self._sensors

    def _initialize_sensors(
        self,
        sensor_configs: Mapping[str, SensorConfig],
        master_seed: Optional[int],
    ) -> Dict[str, BaseSensor]:
        """Instantiate sensor objects with isolated PRNG seeds."""
        sensors: Dict[str, BaseSensor] = {}

        # Derive container-specific seed to prevent PRNG collisions across containers
        container_seed: Optional[int] = None
        if master_seed is not None:
            container_seed = BaseSensor.derive_sensor_seed(master_seed, self._container_id)

        # 1. Temperature Sensor
        if "temperature" in sensor_configs:
            cfg = sensor_configs["temperature"]
            seed_temp = (
                BaseSensor.derive_sensor_seed(container_seed, "temperature")
                if container_seed is not None
                else None
            )
            sensors["temperature"] = TemperatureSensor(config=cfg, seed=seed_temp)

        # 2. Humidity Sensor
        if "humidity" in sensor_configs:
            cfg = sensor_configs["humidity"]
            seed_hum = (
                BaseSensor.derive_sensor_seed(container_seed, "humidity")
                if container_seed is not None
                else None
            )
            sensors["humidity"] = HumiditySensor(config=cfg, seed=seed_hum)

        # 3. Vibration Sensor
        if "vibration" in sensor_configs:
            cfg = sensor_configs["vibration"]
            seed_vib = (
                BaseSensor.derive_sensor_seed(container_seed, "vibration")
                if container_seed is not None
                else None
            )
            sensors["vibration"] = VibrationSensor(config=cfg, seed=seed_vib)

        return sensors

    def simulate_cycle(
        self,
        observed_at: Optional[Union[str, datetime]] = None,
    ) -> List[SensorEvent]:
        """Execute one simulation cycle for this container.

        Samples readings from all enabled sensors on this container and converts
        each reading into an independent SensorEvent.

        Args:
            observed_at: Optional fixed observation timestamp to associate with all
                readings generated in this cycle.

        Returns:
            List of generated SensorEvent instances (one per enabled sensor with a reading).
        """
        events: List[SensorEvent] = []

        # Process in deterministic order: temperature, humidity, vibration
        for sensor_type in ["temperature", "humidity", "vibration"]:
            sensor = self._sensors.get(sensor_type)
            if sensor is None or not sensor.enabled:
                continue

            try:
                reading = sensor.generate_reading()
            except ValueError as exc:
                logger.warning(
                    "Recoverable sensor sampling failure for container '%s', sensor '%s': %s",
                    self._container_id,
                    sensor_type,
                    exc,
                )
                if self._propagate_sampling_errors:
                    raise
                continue

            if reading is None:
                # Disabled or unreadable sensor generates no event
                continue

            event = self._event_builder.build_event(
                container_id=self._container_id,
                sensor_type=sensor.sensor_type,
                value=reading,
                unit=sensor.unit,
                observed_at=observed_at,
            )
            events.append(event)

        return events
