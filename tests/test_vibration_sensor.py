"""Automated unit tests for VibrationSensor."""

import math
from unittest.mock import patch
import pytest

from simulation.config.config_loader import SensorConfig
from simulation.sensors.vibration_sensor import VibrationSensor
from simulation.sensors.temperature_sensor import TemperatureSensor


@pytest.fixture
def default_vibration_config():
    """Default active vibration configuration without limits."""
    return SensorConfig(
        enabled=True,
        unit="m/s^2",
        min_value=None,
        max_value=None,
    )


def test_vibration_sensor_initialization(default_vibration_config):
    """Verify clean initialization and properties."""
    sensor = VibrationSensor(default_vibration_config, rayleigh_scale=0.15)
    assert sensor.sensor_type == "vibration"
    assert sensor.unit == "m/s^2"
    assert sensor.enabled is True
    assert sensor.precision == 3
    assert sensor.rayleigh_scale == 0.15
    assert sensor.physical_min == 0.0
    assert sensor.physical_max == math.inf


def test_vibration_sensor_disabled():
    """Verify that disabled sensor returns None and does not advance PRNG."""
    config_disabled = SensorConfig(enabled=False, unit="m/s^2")
    sensor_disabled = VibrationSensor(config_disabled, seed=321)

    assert sensor_disabled.generate_reading() is None

    config_enabled = SensorConfig(enabled=True, unit="m/s^2")
    sensor_enabled = VibrationSensor(config_enabled, seed=321)
    first_val = sensor_enabled.generate_reading()

    sensor_disabled._config = config_enabled
    assert sensor_disabled.generate_reading() == first_val


def test_vibration_sensor_reproducibility(default_vibration_config):
    """Verify that identical seeds produce identical 100-reading sequences."""
    sensor_a = VibrationSensor(default_vibration_config, seed=654)
    sensor_b = VibrationSensor(default_vibration_config, seed=654)

    readings_a = [sensor_a.generate_reading() for _ in range(100)]
    readings_b = [sensor_b.generate_reading() for _ in range(100)]

    assert readings_a == readings_b
    assert all(math.isfinite(r) for r in readings_a)


def test_vibration_sensor_non_negative_invariant(default_vibration_config):
    """Verify that 1000 generated readings are strictly finite and non-negative."""
    sensor = VibrationSensor(default_vibration_config, seed=10101)
    for _ in range(1000):
        val = sensor.generate_reading()
        assert val is not None
        assert math.isfinite(val)
        assert val >= 0.0


def test_vibration_sensor_rounding_precision(default_vibration_config):
    """Verify that readings are rounded to 3 decimal places."""
    sensor = VibrationSensor(default_vibration_config, seed=20202)
    for _ in range(50):
        val = sensor.generate_reading()
        assert val is not None
        parts = str(val).split(".")
        if len(parts) == 2:
            assert len(parts[1]) <= 3


def test_vibration_sensor_configured_limits():
    """Verify that readings strictly stay within configured bounds."""
    config_bounded = SensorConfig(
        enabled=True,
        unit="m/s^2",
        min_value=0.05,
        max_value=0.50,
    )
    sensor = VibrationSensor(config_bounded, rayleigh_scale=0.15, seed=30303)
    readings = [sensor.generate_reading() for _ in range(100)]

    assert all(0.05 <= r <= 0.50 for r in readings)


def test_vibration_sensor_post_rounding_validation():
    """Verify that post-rounding checks prevent boundary leakage."""
    config = SensorConfig(enabled=True, unit="m/s^2", min_value=0.100, max_value=0.300)
    sensor = VibrationSensor(config, rayleigh_scale=0.15, seed=40404)
    for _ in range(50):
        val = sensor.generate_reading()
        assert val is not None
        assert 0.100 <= val <= 0.300


def test_vibration_sensor_endpoint_handling(default_vibration_config):
    """Verify that when random() returns 0.0 (u = 1.0), the endpoint math.nextafter is used."""
    sensor = VibrationSensor(default_vibration_config, rayleigh_scale=0.15, seed=50505)

    # Mock rng.random to return 0.0 exactly, causing u = 1.0
    with patch.object(sensor._rng, "random", return_value=0.0):
        reading = sensor.generate_reading()
        assert reading is not None
        assert math.isfinite(reading)
        assert reading >= 0.0


def test_vibration_sensor_exhaustion_raises_value_error():
    """Verify that impossible limits raise ValueError upon attempt exhaustion."""
    # Config limits [10.0, 11.0] when scale is 0.15 with 5 attempts
    config = SensorConfig(enabled=True, unit="m/s^2", min_value=10.0, max_value=11.0)
    sensor = VibrationSensor(
        config,
        rayleigh_scale=0.15,
        seed=1,
        max_sampling_attempts=5,
    )
    with pytest.raises(ValueError, match="failed to generate a valid reading"):
        sensor.generate_reading()


def test_vibration_sensor_invalid_parameters(default_vibration_config):
    """Verify rejection of invalid constructor parameters."""
    with pytest.raises(ValueError, match="rayleigh_scale must be a positive finite number"):
        VibrationSensor(default_vibration_config, rayleigh_scale=0.0)

    with pytest.raises(ValueError, match="rayleigh_scale must be a positive finite number"):
        VibrationSensor(default_vibration_config, rayleigh_scale=-0.1)

    with pytest.raises(ValueError, match="rayleigh_scale must be a positive finite number"):
        VibrationSensor(default_vibration_config, rayleigh_scale=float("inf"))


def test_independent_prng_stream_vibration():
    """Verify that sampling from VibrationSensor does not affect TemperatureSensor."""
    master_seed = 88888
    temp_cfg = SensorConfig(enabled=True, unit="celsius")
    vib_cfg = SensorConfig(enabled=True, unit="m/s^2")

    # Run 1: Sample temperature alone
    temp_sensor_1 = TemperatureSensor(temp_cfg, seed=master_seed)
    temp_readings_1 = [temp_sensor_1.generate_reading() for _ in range(25)]

    # Run 2: Sample vibration 100 times, then sample temperature
    vib_sensor = VibrationSensor(vib_cfg, seed=master_seed)
    for _ in range(100):
        vib_sensor.generate_reading()

    temp_sensor_2 = TemperatureSensor(temp_cfg, seed=master_seed)
    temp_readings_2 = [temp_sensor_2.generate_reading() for _ in range(25)]

    assert temp_readings_1 == temp_readings_2

