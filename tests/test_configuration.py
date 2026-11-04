"""Automated unit tests for AtmoSync configuration management and validation."""

from dataclasses import FrozenInstanceError
import math
from pathlib import Path
import pytest
import yaml

from simulation.config.config_loader import (
    AppConfig,
    ConfigLoader,
    ConfigurationError,
    SensorConfig,
    SimulationSettings,
    load_config,
)


@pytest.fixture
def valid_config_dict():
    """Returns a valid baseline configuration dictionary with unconfirmed (null) limits."""
    return {
        "simulation": {
            "interval_seconds": 1.5,
            "random_seed": 42,
        },
        "containers": ["CONT-001", "CONT-002", "CONT-003"],
        "sensors": {
            "temperature": {
                "enabled": True,
                "unit": "celsius",
                "min_value": None,
                "max_value": None,
            },
            "humidity": {
                "enabled": True,
                "unit": "percent",
                "min_value": None,
                "max_value": None,
            },
            "vibration": {
                "enabled": False,
                "unit": "m/s^2",
                "min_value": None,
                "max_value": None,
            },
        },
    }


def write_yaml_config(tmp_path: Path, data: dict, filename: str = "config.yaml") -> Path:
    """Helper to write configuration data to a temporary YAML file."""
    config_file = tmp_path / filename
    with open(config_file, "w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh)
    return config_file


# -----------------------------------------------------------------------------
# 1. Default and Valid Configuration Loading Tests
# -----------------------------------------------------------------------------

def test_load_default_project_config():
    """Verify that the official default simulation_config.yaml loads and validates cleanly."""
    default_path = Path("simulation/config/simulation_config.yaml")
    assert default_path.is_file(), "Default config file must exist"

    config = load_config(default_path)
    assert isinstance(config, AppConfig)
    assert config.simulation.interval_seconds == 1.0
    assert config.simulation.random_seed is None
    assert len(config.containers) == 3
    assert "CONTAINER_001" in config.containers
    assert "temperature" in config.sensors
    assert "humidity" in config.sensors
    assert "vibration" in config.sensors

    # Verify unconfirmed provisional limits are loaded as None
    for sensor in config.sensors.values():
        assert sensor.min_value is None
        assert sensor.max_value is None


def test_parse_and_validate_valid_dict(valid_config_dict):
    """Verify parsing and validation of a valid configuration dictionary."""
    config = ConfigLoader.parse_and_validate(valid_config_dict)
    assert isinstance(config, AppConfig)
    assert config.simulation.interval_seconds == 1.5
    assert config.simulation.random_seed == 42
    assert config.containers == ("CONT-001", "CONT-002", "CONT-003")
    assert config.sensors["temperature"].enabled is True
    assert config.sensors["temperature"].unit == "celsius"
    assert config.sensors["vibration"].enabled is False


# -----------------------------------------------------------------------------
# 2. Container Configuration Tests
# -----------------------------------------------------------------------------

def test_multiple_containers_parsing(valid_config_dict):
    """Verify handling of multiple distinct container identifiers as an immutable tuple."""
    valid_config_dict["containers"] = ["BOX_A", "BOX_B", "BOX_C", "BOX_D"]
    config = ConfigLoader.parse_and_validate(valid_config_dict)
    assert len(config.containers) == 4
    assert config.containers == ("BOX_A", "BOX_B", "BOX_C", "BOX_D")


def test_duplicate_container_ids_detected(valid_config_dict):
    """Verify that duplicate container IDs are detected and rejected."""
    valid_config_dict["containers"] = ["CONT-001", "CONT-002", "CONT-001"]
    with pytest.raises(ConfigurationError, match="Duplicate container IDs detected: CONT-001"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_empty_container_list_rejected(valid_config_dict):
    """Verify that an empty containers list is rejected."""
    valid_config_dict["containers"] = []
    with pytest.raises(ConfigurationError, match="'containers' list cannot be empty"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_invalid_container_item_type(valid_config_dict):
    """Verify that non-string container IDs are rejected."""
    valid_config_dict["containers"] = ["CONT-001", 12345]
    with pytest.raises(ConfigurationError, match="must be a non-empty string"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_blank_container_item_rejected(valid_config_dict):
    """Verify that whitespace-only container IDs are rejected."""
    valid_config_dict["containers"] = ["CONT-001", "   "]
    with pytest.raises(ConfigurationError, match="must be a non-empty string"):
        ConfigLoader.parse_and_validate(valid_config_dict)


# -----------------------------------------------------------------------------
# 3. Missing Required Fields Tests
# -----------------------------------------------------------------------------

@pytest.mark.parametrize("missing_section", ["simulation", "containers", "sensors"])
def test_missing_top_level_section(valid_config_dict, missing_section):
    """Verify that omitting any required top-level section raises a clear error."""
    del valid_config_dict[missing_section]
    with pytest.raises(ConfigurationError, match=f"Missing required configuration section.*{missing_section}"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_missing_simulation_interval(valid_config_dict):
    """Verify error when interval_seconds is missing."""
    del valid_config_dict["simulation"]["interval_seconds"]
    with pytest.raises(ConfigurationError, match="Missing required field 'interval_seconds'"):
        ConfigLoader.parse_and_validate(valid_config_dict)


@pytest.mark.parametrize("missing_sensor", ["temperature", "humidity", "vibration"])
def test_missing_required_sensor_definition(valid_config_dict, missing_sensor):
    """Verify error when a required sensor definition is omitted."""
    del valid_config_dict["sensors"][missing_sensor]
    with pytest.raises(ConfigurationError, match=f"Missing required sensor configuration.*{missing_sensor}"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_missing_sensor_enabled_field(valid_config_dict):
    """Verify error when a sensor's 'enabled' flag is missing."""
    del valid_config_dict["sensors"]["temperature"]["enabled"]
    with pytest.raises(ConfigurationError, match="Missing required field 'enabled' for sensor 'temperature'"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_missing_sensor_unit_field(valid_config_dict):
    """Verify error when a sensor's 'unit' string is missing."""
    del valid_config_dict["sensors"]["temperature"]["unit"]
    with pytest.raises(ConfigurationError, match="Missing required field 'unit' for sensor 'temperature'"):
        ConfigLoader.parse_and_validate(valid_config_dict)


# -----------------------------------------------------------------------------
# 4. Simulation Interval & Seed Validation Tests
# -----------------------------------------------------------------------------

@pytest.mark.parametrize("invalid_interval", [0, -1, -0.5])
def test_non_positive_simulation_interval(valid_config_dict, invalid_interval):
    """Verify that zero or negative simulation intervals are rejected."""
    valid_config_dict["simulation"]["interval_seconds"] = invalid_interval
    with pytest.raises(ConfigurationError, match="positive number greater than 0"):
        ConfigLoader.parse_and_validate(valid_config_dict)


@pytest.mark.parametrize("invalid_type", ["fast", True, False, [1.0]])
def test_invalid_interval_type(valid_config_dict, invalid_type):
    """Verify that non-numeric or boolean interval values are rejected."""
    valid_config_dict["simulation"]["interval_seconds"] = invalid_type
    with pytest.raises(ConfigurationError, match="must be a numeric value"):
        ConfigLoader.parse_and_validate(valid_config_dict)


@pytest.mark.parametrize("non_finite_interval", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_simulation_interval_rejected(valid_config_dict, non_finite_interval):
    """Verify that NaN, +inf, and -inf intervals are rejected."""
    valid_config_dict["simulation"]["interval_seconds"] = non_finite_interval
    with pytest.raises(ConfigurationError, match="must be a finite numeric value"):
        ConfigLoader.parse_and_validate(valid_config_dict)


@pytest.mark.parametrize("invalid_seed", ["seed123", True, 4.5, float("nan"), float("inf")])
def test_invalid_random_seed_type(valid_config_dict, invalid_seed):
    """Verify that non-integer seeds (including booleans and floats) are rejected."""
    valid_config_dict["simulation"]["random_seed"] = invalid_seed
    with pytest.raises(ConfigurationError, match="'random_seed' must be an integer or null"):
        ConfigLoader.parse_and_validate(valid_config_dict)


# -----------------------------------------------------------------------------
# 5. Sensor Limits and Configurations Tests
# -----------------------------------------------------------------------------

def test_optional_unconfirmed_sensor_limits(valid_config_dict):
    """Verify that optional sensor limits remain None when unconfirmed."""
    valid_config_dict["sensors"]["temperature"]["min_value"] = None
    valid_config_dict["sensors"]["temperature"]["max_value"] = None
    config = ConfigLoader.parse_and_validate(valid_config_dict)
    assert config.sensors["temperature"].min_value is None
    assert config.sensors["temperature"].max_value is None


def test_valid_confirmed_sensor_limits(valid_config_dict):
    """Verify that valid numerical limits are correctly parsed and stored as floats."""
    valid_config_dict["sensors"]["temperature"]["min_value"] = -10
    valid_config_dict["sensors"]["temperature"]["max_value"] = 35.5
    config = ConfigLoader.parse_and_validate(valid_config_dict)
    assert config.sensors["temperature"].min_value == -10.0
    assert config.sensors["temperature"].max_value == 35.5


def test_sensor_min_equal_to_max_rejected(valid_config_dict):
    """Verify that min_value equal to max_value is rejected."""
    valid_config_dict["sensors"]["temperature"]["min_value"] = 25.0
    valid_config_dict["sensors"]["temperature"]["max_value"] = 25.0
    with pytest.raises(ConfigurationError, match="min_value \\(25.0\\) must be strictly less than max_value \\(25.0\\)"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_sensor_min_greater_than_max_rejected(valid_config_dict):
    """Verify that min_value greater than max_value is rejected."""
    valid_config_dict["sensors"]["humidity"]["min_value"] = 90.0
    valid_config_dict["sensors"]["humidity"]["max_value"] = 30.0
    with pytest.raises(ConfigurationError, match="min_value \\(90.0\\) must be strictly less than max_value \\(30.0\\)"):
        ConfigLoader.parse_and_validate(valid_config_dict)


@pytest.mark.parametrize("invalid_limit", ["low", True, False, [10], {"val": 1}])
def test_sensor_non_numeric_limits(valid_config_dict, invalid_limit):
    """Verify that non-numeric limits (e.g. strings, booleans, containers) are rejected."""
    valid_config_dict["sensors"]["vibration"]["min_value"] = invalid_limit
    with pytest.raises(ConfigurationError, match="must be numeric or null"):
        ConfigLoader.parse_and_validate(valid_config_dict)


@pytest.mark.parametrize("non_finite_limit", [float("nan"), float("inf"), float("-inf")])
def test_sensor_non_finite_limits_rejected(valid_config_dict, non_finite_limit):
    """Verify that NaN, +inf, and -inf limits are rejected for sensors."""
    valid_config_dict["sensors"]["temperature"]["min_value"] = non_finite_limit
    with pytest.raises(ConfigurationError, match="must be a finite numeric value"):
        ConfigLoader.parse_and_validate(valid_config_dict)

    valid_config_dict["sensors"]["temperature"]["min_value"] = 0.0
    valid_config_dict["sensors"]["temperature"]["max_value"] = non_finite_limit
    with pytest.raises(ConfigurationError, match="must be a finite numeric value"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_sensor_invalid_enabled_type(valid_config_dict):
    """Verify that non-boolean enabled flags are rejected."""
    valid_config_dict["sensors"]["temperature"]["enabled"] = "yes"
    with pytest.raises(ConfigurationError, match="must be a boolean"):
        ConfigLoader.parse_and_validate(valid_config_dict)


def test_sensor_blank_unit(valid_config_dict):
    """Verify that blank or non-string unit values are rejected."""
    valid_config_dict["sensors"]["temperature"]["unit"] = "   "
    with pytest.raises(ConfigurationError, match="must be a non-empty string"):
        ConfigLoader.parse_and_validate(valid_config_dict)


# -----------------------------------------------------------------------------
# 6. File Handling & YAML Parsing Tests (Including Duplicate Keys)
# -----------------------------------------------------------------------------

def test_missing_file_raises_configuration_error(tmp_path):
    """Verify that attempting to load a non-existent file raises ConfigurationError."""
    non_existent = tmp_path / "does_not_exist.yaml"
    with pytest.raises(ConfigurationError, match="Configuration file not found"):
        ConfigLoader.load(non_existent)


def test_malformed_yaml_raises_configuration_error(tmp_path):
    """Verify that syntax errors in YAML raise ConfigurationError."""
    malformed_file = tmp_path / "broken.yaml"
    malformed_file.write_text("simulation: [broken yaml: {", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="Malformed YAML"):
        ConfigLoader.load(malformed_file)


def test_non_dictionary_root_rejected(tmp_path):
    """Verify that a YAML file with a non-dictionary root raises ConfigurationError."""
    list_file = tmp_path / "list_root.yaml"
    list_file.write_text("- item1\n- item2\n", encoding="utf-8")
    with pytest.raises(ConfigurationError, match="must be a dictionary/mapping"):
        ConfigLoader.load(list_file)


def test_duplicate_yaml_keys_rejected(tmp_path):
    """Verify that duplicate YAML keys anywhere in the configuration raise ConfigurationError."""
    yaml_content = """
simulation:
  interval_seconds: 1.0
  interval_seconds: 2.0
containers:
  - "CONTAINER_001"
sensors:
  temperature:
    enabled: true
    unit: "celsius"
  humidity:
    enabled: true
    unit: "percent"
  vibration:
    enabled: true
    unit: "m/s^2"
"""
    dup_file = tmp_path / "dup_keys.yaml"
    dup_file.write_text(yaml_content, encoding="utf-8")
    with pytest.raises(ConfigurationError, match="duplicate key 'interval_seconds'"):
        ConfigLoader.load(dup_file)


def test_duplicate_sensor_key_rejected(tmp_path):
    """Verify that duplicate sensor definitions in YAML raise ConfigurationError."""
    yaml_content = """
simulation:
  interval_seconds: 1.0
containers:
  - "CONTAINER_001"
sensors:
  temperature:
    enabled: true
    unit: "celsius"
  temperature:
    enabled: false
    unit: "celsius"
  humidity:
    enabled: true
    unit: "percent"
  vibration:
    enabled: true
    unit: "m/s^2"
"""
    dup_sensor_file = tmp_path / "dup_sensor.yaml"
    dup_sensor_file.write_text(yaml_content, encoding="utf-8")
    with pytest.raises(ConfigurationError, match="duplicate key 'temperature'"):
        ConfigLoader.load(dup_sensor_file)


# -----------------------------------------------------------------------------
# 7. Deep Immutability Tests
# -----------------------------------------------------------------------------

def test_app_config_immutability(valid_config_dict):
    """Verify that AppConfig and all nested structures cannot be modified."""
    config = ConfigLoader.parse_and_validate(valid_config_dict)

    # 1. Modifying top-level dataclass attributes is blocked
    with pytest.raises(FrozenInstanceError):
        config.simulation = SimulationSettings(interval_seconds=2.0)

    # 2. Modifying containers tuple is blocked
    with pytest.raises(AttributeError):
        config.containers.append("CONT-999")  # type: ignore

    with pytest.raises(TypeError):
        config.containers[0] = "MODIFIED"  # type: ignore

    # 3. Modifying sensors mapping proxy is blocked
    with pytest.raises(TypeError):
        config.sensors["temperature"] = SensorConfig(enabled=False, unit="K")  # type: ignore

    with pytest.raises(AttributeError):
        config.sensors.clear()  # type: ignore

    # 4. Modifying nested SensorConfig dataclass is blocked
    with pytest.raises(FrozenInstanceError):
        config.sensors["temperature"].enabled = False  # type: ignore
