"""Automated unit tests for HumiditySensor."""

import math
import pytest

from simulation.config.config_loader import SensorConfig
from simulation.sensors.humidity_sensor import HumiditySensor
from simulation.sensors.temperature_sensor import TemperatureSensor


@pytest.fixture
def default_humidity_config():
    """Default active relative humidity configuration without limits."""
    return SensorConfig(
        enabled=True,
        unit="percent",
        min_value=None,
        max_value=None,
    )


def test_humidity_sensor_initialization(default_humidity_config):
    """Verify clean initialization and properties."""
    sensor = HumiditySensor(default_humidity_config, baseline_mean=85.0, baseline_std=2.5)
    assert sensor.sensor_type == "humidity"
    assert sensor.unit == "percent"
    assert sensor.enabled is True
    assert sensor.precision == 1
    assert sensor.baseline_mean == 85.0
    assert sensor.baseline_std == 2.5
    assert sensor.physical_min == 0.0
    assert sensor.physical_max == 100.0


def test_humidity_sensor_disabled():
    """Verify that disabled sensor returns None and does not advance PRNG."""
    config_disabled = SensorConfig(enabled=False, unit="percent")
    sensor_disabled = HumiditySensor(config_disabled, seed=123)

    assert sensor_disabled.generate_reading() is None

    config_enabled = SensorConfig(enabled=True, unit="percent")
    sensor_enabled = HumiditySensor(config_enabled, seed=123)
    first_val = sensor_enabled.generate_reading()

    sensor_disabled._config = config_enabled
    assert sensor_disabled.generate_reading() == first_val


def test_humidity_sensor_reproducibility(default_humidity_config):
    """Verify that identical seeds produce identical 100-reading sequences."""
    sensor_a = HumiditySensor(default_humidity_config, seed=777)
    sensor_b = HumiditySensor(default_humidity_config, seed=777)

    readings_a = [sensor_a.generate_reading() for _ in range(100)]
    readings_b = [sensor_b.generate_reading() for _ in range(100)]

    assert readings_a == readings_b
    assert all(math.isfinite(r) for r in readings_a)


def test_humidity_sensor_physical_invariants(default_humidity_config):
    """Verify that 1000 generated readings strictly lie within [0.0, 100.0]."""
    sensor = HumiditySensor(default_humidity_config, seed=888)
    for _ in range(1000):
        val = sensor.generate_reading()
        assert val is not None
        assert 0.0 <= val <= 100.0


def test_humidity_sensor_rounding_precision(default_humidity_config):
    """Verify that readings are rounded to 1 decimal place."""
    sensor = HumiditySensor(default_humidity_config, seed=333)
    for _ in range(50):
        val = sensor.generate_reading()
        assert val is not None
        parts = str(val).split(".")
        if len(parts) == 2:
            assert len(parts[1]) <= 1


def test_humidity_sensor_configured_limits():
    """Verify that readings strictly stay within configured bounds."""
    config_bounded = SensorConfig(
        enabled=True,
        unit="percent",
        min_value=80.0,
        max_value=90.0,
    )
    sensor = HumiditySensor(config_bounded, baseline_mean=85.0, baseline_std=2.5, seed=444)
    readings = [sensor.generate_reading() for _ in range(100)]

    assert all(80.0 <= r <= 90.0 for r in readings)


def test_humidity_sensor_post_rounding_validation():
    """Verify that post-rounding checks prevent boundary leakage."""
    config = SensorConfig(enabled=True, unit="percent", min_value=70.0, max_value=95.0)
    sensor = HumiditySensor(config, baseline_mean=85.0, baseline_std=3.0, seed=555)
    for _ in range(50):
        val = sensor.generate_reading()
        assert val is not None
        assert 70.0 <= val <= 95.0


def test_humidity_sensor_exhaustion_raises_value_error():
    """Verify that impossible limits raise ValueError upon attempt exhaustion."""
    # Attempting to force humidity to [10.0, 12.0] when mean=85, std=1 with 5 attempts
    config = SensorConfig(enabled=True, unit="percent", min_value=10.0, max_value=12.0)
    sensor = HumiditySensor(
        config,
        baseline_mean=85.0,
        baseline_std=1.0,
        seed=1,
        max_sampling_attempts=5,
    )
    with pytest.raises(ValueError, match="failed to generate a valid reading"):
        sensor.generate_reading()


def test_humidity_sensor_invalid_parameters(default_humidity_config):
    """Verify rejection of invalid constructor parameters."""
    with pytest.raises(ValueError, match="baseline_mean for relative humidity must be in"):
        HumiditySensor(default_humidity_config, baseline_mean=-5.0)

    with pytest.raises(ValueError, match="baseline_mean for relative humidity must be in"):
        HumiditySensor(default_humidity_config, baseline_mean=105.0)

    with pytest.raises(ValueError, match="baseline_std must be a positive finite number"):
        HumiditySensor(default_humidity_config, baseline_std=0.0)

    with pytest.raises(ValueError, match="baseline_std must be a positive finite number"):
        HumiditySensor(default_humidity_config, baseline_std=-2.0)


def test_independent_prng_streams():
    """Verify that sampling from TemperatureSensor does not advance HumiditySensor."""
    master_seed = 99999
    temp_cfg = SensorConfig(enabled=True, unit="celsius")
    hum_cfg = SensorConfig(enabled=True, unit="percent")

    # Run 1: Sample humidity without touching temperature
    hum_sensor_1 = HumiditySensor(hum_cfg, seed=master_seed)
    hum_readings_1 = [hum_sensor_1.generate_reading() for _ in range(20)]

    # Run 2: Sample temperature 50 times, then sample humidity
    temp_sensor = TemperatureSensor(temp_cfg, seed=master_seed)
    for _ in range(50):
        temp_sensor.generate_reading()

    hum_sensor_2 = HumiditySensor(hum_cfg, seed=master_seed)
    hum_readings_2 = [hum_sensor_2.generate_reading() for _ in range(20)]

    # Sequences must be completely identical
    assert hum_readings_1 == hum_readings_2

