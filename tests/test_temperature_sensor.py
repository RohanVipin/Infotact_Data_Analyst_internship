"""Automated unit tests for TemperatureSensor."""

import math
import subprocess
import sys
import pytest

from simulation.config.config_loader import SensorConfig
from simulation.sensors.temperature_sensor import TemperatureSensor
from simulation.sensors.base_sensor import BaseSensor


@pytest.fixture
def default_temp_config():
    """Default active temperature configuration without limits."""
    return SensorConfig(
        enabled=True,
        unit="celsius",
        min_value=None,
        max_value=None,
    )


def test_temperature_sensor_initialization(default_temp_config):
    """Verify clean initialization and properties."""
    sensor = TemperatureSensor(default_temp_config, baseline_mean=4.0, baseline_std=1.0)
    assert sensor.sensor_type == "temperature"
    assert sensor.unit == "celsius"
    assert sensor.enabled is True
    assert sensor.precision == 2
    assert sensor.baseline_mean == 4.0
    assert sensor.baseline_std == 1.0


def test_temperature_sensor_disabled():
    """Verify that disabled sensor returns None and does not advance PRNG."""
    config_disabled = SensorConfig(enabled=False, unit="celsius")
    sensor_disabled = TemperatureSensor(config_disabled, seed=42)

    # Calling generate_reading when disabled
    assert sensor_disabled.generate_reading() is None

    # Enable and check first generated value
    config_enabled = SensorConfig(enabled=True, unit="celsius")
    sensor_enabled = TemperatureSensor(config_enabled, seed=42)
    first_val = sensor_enabled.generate_reading()

    # The PRNG of sensor_disabled should not have advanced
    sensor_disabled._config = config_enabled  # manually switch config to inspect PRNG state
    assert sensor_disabled.generate_reading() == first_val


def test_temperature_sensor_reproducibility(default_temp_config):
    """Verify that identical seeds produce identical 100-reading sequences."""
    sensor_a = TemperatureSensor(default_temp_config, seed=12345)
    sensor_b = TemperatureSensor(default_temp_config, seed=12345)

    readings_a = [sensor_a.generate_reading() for _ in range(100)]
    readings_b = [sensor_b.generate_reading() for _ in range(100)]

    assert readings_a == readings_b
    assert all(math.isfinite(r) for r in readings_a)


def test_temperature_sensor_different_seeds(default_temp_config):
    """Verify that different seeds produce different sequences."""
    sensor_a = TemperatureSensor(default_temp_config, seed=111)
    sensor_b = TemperatureSensor(default_temp_config, seed=222)

    readings_a = [sensor_a.generate_reading() for _ in range(20)]
    readings_b = [sensor_b.generate_reading() for _ in range(20)]

    assert readings_a != readings_b


def test_temperature_sensor_rounding_precision(default_temp_config):
    """Verify that readings are rounded to 2 decimal places."""
    sensor = TemperatureSensor(default_temp_config, seed=99)
    for _ in range(50):
        val = sensor.generate_reading()
        assert val is not None
        # String representation after 2 decimal places has at most 2 digits after '.'
        parts = str(val).split(".")
        if len(parts) == 2:
            assert len(parts[1]) <= 2


def test_temperature_sensor_configured_limits():
    """Verify that readings strictly stay within configured bounds."""
    config_bounded = SensorConfig(
        enabled=True,
        unit="celsius",
        min_value=2.0,
        max_value=6.0,
    )
    sensor = TemperatureSensor(config_bounded, baseline_mean=4.0, baseline_std=1.0, seed=77)
    readings = [sensor.generate_reading() for _ in range(100)]

    assert all(2.0 <= r <= 6.0 for r in readings)


def test_temperature_sensor_post_rounding_validation():
    """Verify that rounded values outside bounds are rejected during resampling."""
    # Narrow limits [3.00, 5.00]
    config = SensorConfig(enabled=True, unit="celsius", min_value=3.00, max_value=5.00)
    sensor = TemperatureSensor(config, baseline_mean=4.0, baseline_std=0.5, seed=54321)
    for _ in range(50):
        val = sensor.generate_reading()
        assert val is not None
        assert 3.00 <= val <= 5.00


def test_temperature_sensor_exhaustion_raises_value_error():
    """Verify that impossible limits raise ValueError upon attempt exhaustion."""
    # Config bounds [100.0, 101.0] while mean=4.0, std=0.1 (statistically impossible within 10 attempts)
    config = SensorConfig(enabled=True, unit="celsius", min_value=100.0, max_value=101.0)
    sensor = TemperatureSensor(
        config,
        baseline_mean=4.0,
        baseline_std=0.1,
        seed=1,
        max_sampling_attempts=10,
    )
    with pytest.raises(ValueError, match="failed to generate a valid reading"):
        sensor.generate_reading()


def test_temperature_sensor_invalid_parameters(default_temp_config):
    """Verify rejection of invalid constructor parameters."""
    with pytest.raises(ValueError, match="baseline_std must be a positive finite number"):
        TemperatureSensor(default_temp_config, baseline_std=0.0)

    with pytest.raises(ValueError, match="baseline_std must be a positive finite number"):
        TemperatureSensor(default_temp_config, baseline_std=-1.0)

    with pytest.raises(ValueError, match="baseline_std must be a positive finite number"):
        TemperatureSensor(default_temp_config, baseline_std=float("nan"))

    with pytest.raises(ValueError, match="baseline_mean must be a finite number"):
        TemperatureSensor(default_temp_config, baseline_mean=float("inf"))

    with pytest.raises(ValueError, match="max_sampling_attempts must be positive"):
        TemperatureSensor(default_temp_config, max_sampling_attempts=0)


def test_temperature_sensor_conflicting_limits():
    """Verify that conflicting limits (eff_min > eff_max) raise ValueError."""
    config = SensorConfig(enabled=True, unit="celsius", min_value=10.0, max_value=5.0)
    sensor = TemperatureSensor(config)
    with pytest.raises(ValueError, match="Conflicting boundaries"):
        sensor.generate_reading()


def test_cross_process_seed_derivation():
    """Verify that SHA-256 seed derivation is identical across separate Python processes."""
    master_seed = 987654321
    sensor_name = "temperature"
    local_derived = BaseSensor.derive_sensor_seed(master_seed, sensor_name)

    code = f"""
from simulation.sensors.base_sensor import BaseSensor
print(BaseSensor.derive_sensor_seed({master_seed}, '{sensor_name}'))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=True,
    )
    remote_derived = int(result.stdout.strip())
    assert local_derived == remote_derived

