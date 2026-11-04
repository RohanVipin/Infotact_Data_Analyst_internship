"""Configuration loader and validator for the AtmoSync IoT Sensor Simulator.

This module provides data models, a custom YAML loader with duplicate-key detection,
and strict validation rules to guarantee simulation settings are verified at application startup.
"""

from dataclasses import dataclass
import math
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union
import yaml
from yaml.constructor import ConstructorError


class ConfigurationError(Exception):
    """Raised when configuration loading, parsing, or validation fails."""
    pass


class UniqueKeySafeLoader(yaml.SafeLoader):
    """YAML safe loader that detects and rejects duplicate keys in mappings."""

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> Dict[Any, Any]:
        if not isinstance(node, yaml.MappingNode):
            raise ConstructorError(
                None,
                None,
                f"expected a mapping node, but found {node.id}",
                node.start_mark,
            )

        mapping: Dict[Any, Any] = {}
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            try:
                hash(key)
            except TypeError:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found unacceptable key ({key})",
                    key_node.start_mark,
                )
            if key in mapping:
                raise ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key '{key}'",
                    key_node.start_mark,
                )
            mapping[key] = self.construct_object(value_node, deep=deep)
        return mapping


@dataclass(frozen=True)
class SensorConfig:
    """Configuration parameters for an individual virtual sensor.

    Attributes:
        enabled: Whether this sensor is active and should produce readings.
        unit: Provisional measurement unit string (e.g., 'celsius', 'percent', 'm/s^2').
        min_value: Optional provisional minimum limit. None if unconfirmed.
        max_value: Optional provisional maximum limit. None if unconfirmed.
    """
    enabled: bool
    unit: str
    min_value: Optional[float] = None
    max_value: Optional[float] = None


@dataclass(frozen=True)
class SimulationSettings:
    """General simulation timing and execution settings.

    Attributes:
        interval_seconds: Delay interval between simulation cycles in seconds (finite float > 0).
        random_seed: Optional integer seed for deterministic pseudo-random generation.
    """
    interval_seconds: float
    random_seed: Optional[int] = None


@dataclass(frozen=True)
class AppConfig:
    """Deeply immutable root configuration object for the AtmoSync Sensor Simulator.

    Attributes:
        simulation: Timing and execution settings (frozen dataclass).
        containers: Immutable tuple of unique container identifier strings.
        sensors: Read-only mapping proxy of sensor configurations.
    """
    simulation: SimulationSettings
    containers: Tuple[str, ...]
    sensors: Mapping[str, SensorConfig]


class ConfigLoader:
    """Loads, parses, and validates the YAML configuration for the simulator."""

    REQUIRED_TOP_LEVEL_KEYS: Set[str] = {"simulation", "containers", "sensors"}
    REQUIRED_SENSORS: Set[str] = {"temperature", "humidity", "vibration"}

    @classmethod
    def load(cls, config_path: Union[str, Path]) -> AppConfig:
        """Load and validate configuration from a YAML file path.

        Args:
            config_path: Path to the YAML configuration file.

        Returns:
            Validated, immutable AppConfig instance.

        Raises:
            ConfigurationError: If the file is missing, contains duplicate keys,
                contains malformed YAML, or fails structural or semantic validation.
        """
        path = Path(config_path)
        if not path.is_file():
            raise ConfigurationError(f"Configuration file not found: {path.resolve()}")

        try:
            with open(path, "r", encoding="utf-8") as fh:
                raw_data = yaml.load(fh, Loader=UniqueKeySafeLoader)
        except ConstructorError as exc:
            raise ConfigurationError(
                f"YAML duplicate key or mapping error in '{path}': {exc.problem} (line {exc.problem_mark.line + 1 if exc.problem_mark else 'unknown'})"
            ) from exc
        except yaml.YAMLError as exc:
            raise ConfigurationError(f"Malformed YAML in '{path}': {exc}") from exc
        except OSError as exc:
            raise ConfigurationError(f"Error reading configuration file '{path}': {exc}") from exc

        return cls.parse_and_validate(raw_data, source_name=str(path))

    @classmethod
    def parse_and_validate(cls, raw_data: Any, source_name: str = "configuration") -> AppConfig:
        """Parse and strictly validate raw dictionary data into an immutable AppConfig.

        Args:
            raw_data: Unmarshalled YAML data (expected to be a dictionary).
            source_name: Contextual label for error messages (e.g. file path).

        Returns:
            Validated, deeply immutable AppConfig instance.

        Raises:
            ConfigurationError: If any structure, type, or constraint is violated.
        """
        if not isinstance(raw_data, dict):
            raise ConfigurationError(
                f"Configuration root in {source_name} must be a dictionary/mapping, got {type(raw_data).__name__}"
            )

        # 1. Validate top-level keys
        missing_top = cls.REQUIRED_TOP_LEVEL_KEYS - set(raw_data.keys())
        if missing_top:
            missing_sorted = sorted(list(missing_top))
            raise ConfigurationError(
                f"Missing required configuration section(s): {', '.join(missing_sorted)}"
            )

        # 2. Validate simulation section
        sim_data = raw_data.get("simulation")
        simulation_settings = cls._validate_simulation_section(sim_data)

        # 3. Validate containers section
        containers_data = raw_data.get("containers")
        containers_tuple = cls._validate_containers_section(containers_data)

        # 4. Validate sensors section
        sensors_data = raw_data.get("sensors")
        sensors_mapping = cls._validate_sensors_section(sensors_data)

        return AppConfig(
            simulation=simulation_settings,
            containers=containers_tuple,
            sensors=sensors_mapping,
        )

    @classmethod
    def _validate_simulation_section(cls, data: Any) -> SimulationSettings:
        """Validate the 'simulation' section."""
        if not isinstance(data, dict):
            raise ConfigurationError(
                f"'simulation' section must be a dictionary, got {type(data).__name__}"
            )

        if "interval_seconds" not in data:
            raise ConfigurationError("Missing required field 'interval_seconds' in 'simulation' section")

        interval = data["interval_seconds"]
        # In Python bool is a subclass of int, so explicitly reject bool
        if isinstance(interval, bool) or not isinstance(interval, (int, float)):
            raise ConfigurationError(
                f"'interval_seconds' must be a numeric value (int or float), got {type(interval).__name__}"
            )

        # Reject NaN, +inf, -inf
        if not math.isfinite(interval):
            raise ConfigurationError(
                f"'interval_seconds' must be a finite numeric value, got {interval}"
            )

        if interval <= 0:
            raise ConfigurationError(
                f"'interval_seconds' must be a positive number greater than 0, got {interval}"
            )

        seed = data.get("random_seed")
        if seed is not None:
            if isinstance(seed, bool) or not isinstance(seed, int):
                raise ConfigurationError(
                    f"'random_seed' must be an integer or null, got {type(seed).__name__}"
                )

        return SimulationSettings(
            interval_seconds=float(interval),
            random_seed=seed,
        )

    @classmethod
    def _validate_containers_section(cls, data: Any) -> Tuple[str, ...]:
        """Validate the 'containers' section and return an immutable tuple."""
        if not isinstance(data, list):
            raise ConfigurationError(
                f"'containers' section must be a list of container IDs, got {type(data).__name__}"
            )

        if not data:
            raise ConfigurationError("'containers' list cannot be empty. At least one container ID is required.")

        seen_ids: Set[str] = set()
        duplicate_ids: List[str] = []
        cleaned_containers: List[str] = []

        for idx, item in enumerate(data):
            if not isinstance(item, str) or not item.strip():
                raise ConfigurationError(
                    f"Container ID at index {idx} must be a non-empty string, got {repr(item)}"
                )
            container_id = item.strip()
            if container_id in seen_ids:
                if container_id not in duplicate_ids:
                    duplicate_ids.append(container_id)
            seen_ids.add(container_id)
            cleaned_containers.append(container_id)

        if duplicate_ids:
            raise ConfigurationError(
                f"Duplicate container IDs detected: {', '.join(sorted(duplicate_ids))}"
            )

        return tuple(cleaned_containers)

    @classmethod
    def _validate_sensors_section(cls, data: Any) -> Mapping[str, SensorConfig]:
        """Validate the 'sensors' section and return an immutable mapping."""
        if not isinstance(data, dict):
            raise ConfigurationError(
                f"'sensors' section must be a dictionary, got {type(data).__name__}"
            )

        missing_sensors = cls.REQUIRED_SENSORS - set(data.keys())
        if missing_sensors:
            missing_sorted = sorted(list(missing_sensors))
            raise ConfigurationError(
                f"Missing required sensor configuration(s): {', '.join(missing_sorted)}"
            )

        sensors: Dict[str, SensorConfig] = {}
        for sensor_name, sensor_raw in data.items():
            sensors[sensor_name] = cls._validate_single_sensor(sensor_name, sensor_raw)

        return MappingProxyType(sensors)

    @classmethod
    def _validate_single_sensor(cls, sensor_name: str, data: Any) -> SensorConfig:
        """Validate an individual sensor's configuration dictionary."""
        if not isinstance(data, dict):
            raise ConfigurationError(
                f"Configuration for sensor '{sensor_name}' must be a dictionary, got {type(data).__name__}"
            )

        if "enabled" not in data:
            raise ConfigurationError(
                f"Missing required field 'enabled' for sensor '{sensor_name}'"
            )
        enabled = data["enabled"]
        if not isinstance(enabled, bool):
            raise ConfigurationError(
                f"'enabled' for sensor '{sensor_name}' must be a boolean (true/false), got {type(enabled).__name__}"
            )

        if "unit" not in data:
            raise ConfigurationError(
                f"Missing required field 'unit' for sensor '{sensor_name}'"
            )
        unit = data["unit"]
        if not isinstance(unit, str) or not unit.strip():
            raise ConfigurationError(
                f"'unit' for sensor '{sensor_name}' must be a non-empty string, got {repr(unit)}"
            )

        min_val = data.get("min_value")
        if min_val is not None:
            if isinstance(min_val, bool) or not isinstance(min_val, (int, float)):
                raise ConfigurationError(
                    f"'min_value' for sensor '{sensor_name}' must be numeric or null, got {type(min_val).__name__}"
                )
            if not math.isfinite(min_val):
                raise ConfigurationError(
                    f"'min_value' for sensor '{sensor_name}' must be a finite numeric value, got {min_val}"
                )
            min_val = float(min_val)

        max_val = data.get("max_value")
        if max_val is not None:
            if isinstance(max_val, bool) or not isinstance(max_val, (int, float)):
                raise ConfigurationError(
                    f"'max_value' for sensor '{sensor_name}' must be numeric or null, got {type(max_val).__name__}"
                )
            if not math.isfinite(max_val):
                raise ConfigurationError(
                    f"'max_value' for sensor '{sensor_name}' must be a finite numeric value, got {max_val}"
                )
            max_val = float(max_val)

        if min_val is not None and max_val is not None:
            if min_val >= max_val:
                raise ConfigurationError(
                    f"Sensor '{sensor_name}' has invalid limits: min_value ({min_val}) "
                    f"must be strictly less than max_value ({max_val})"
                )

        return SensorConfig(
            enabled=enabled,
            unit=unit.strip(),
            min_value=min_val,
            max_value=max_val,
        )


def load_config(config_path: Union[str, Path] = "simulation/config/simulation_config.yaml") -> AppConfig:
    """Convenience function to load and validate simulation configuration.

    Args:
        config_path: Path to the YAML file. Defaults to 'simulation/config/simulation_config.yaml'.

    Returns:
        Validated, immutable AppConfig instance.
    """
    return ConfigLoader.load(config_path)
