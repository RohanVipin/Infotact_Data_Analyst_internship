"""Automated unit tests for ContainerSimulator."""

import pytest

from simulation.config.config_loader import SensorConfig
from simulation.events.event_schema import SensorEvent
from simulation.simulator.container_simulator import ContainerSimulator
from simulation.validation.event_validator import EventValidator


@pytest.fixture
def all_sensors_enabled_config():
    """Return sensor configs with all three sensors active."""
    return {
        "temperature": SensorConfig(enabled=True, unit="celsius"),
        "humidity": SensorConfig(enabled=True, unit="percent"),
        "vibration": SensorConfig(enabled=True, unit="m/s^2"),
    }


def test_container_simulator_all_sensors_enabled(all_sensors_enabled_config):
    """Verify that a container with all 3 sensors enabled produces exactly 3 events per cycle."""
    container = ContainerSimulator(
        container_id="CONTAINER_001",
        sensor_configs=all_sensors_enabled_config,
        master_seed=42,
    )

    events = container.simulate_cycle()
    assert len(events) == 3

    # Option A: One independent event per sensor
    sensor_types = {e.sensor_type for e in events}
    assert sensor_types == {"temperature", "humidity", "vibration"}

    # Verify container ID association and schema validity
    for event in events:
        assert isinstance(event, SensorEvent)
        assert event.container_id == "CONTAINER_001"
        EventValidator.validate(event)


def test_container_simulator_disabled_sensor_generates_no_event(all_sensors_enabled_config):
    """Verify that disabled sensors produce no events."""
    all_sensors_enabled_config["vibration"] = SensorConfig(enabled=False, unit="m/s^2")

    container = ContainerSimulator(
        container_id="CONTAINER_002",
        sensor_configs=all_sensors_enabled_config,
        master_seed=42,
    )

    events = container.simulate_cycle()
    assert len(events) == 2

    sensor_types = {e.sensor_type for e in events}
    assert "vibration" not in sensor_types
    assert sensor_types == {"temperature", "humidity"}


def test_container_simulator_all_disabled(all_sensors_enabled_config):
    """Verify that when all sensors are disabled, zero events are produced."""
    for s_name in all_sensors_enabled_config:
        all_sensors_enabled_config[s_name] = SensorConfig(enabled=False, unit="unit")

    container = ContainerSimulator(
        container_id="CONTAINER_OFF",
        sensor_configs=all_sensors_enabled_config,
    )

    events = container.simulate_cycle()
    assert len(events) == 0


def test_container_simulator_stream_isolation_between_containers(all_sensors_enabled_config):
    """Verify that two distinct containers have isolated PRNG seeds even under the same master seed."""
    c1 = ContainerSimulator("CONT_A", all_sensors_enabled_config, master_seed=100)
    c2 = ContainerSimulator("CONT_B", all_sensors_enabled_config, master_seed=100)

    events_c1 = c1.simulate_cycle()
    events_c2 = c2.simulate_cycle()

    # Different container IDs
    assert {e.container_id for e in events_c1} == {"CONT_A"}
    assert {e.container_id for e in events_c2} == {"CONT_B"}

    # Container seeds derived from container_id differ, so readings differ
    val_map_c1 = {e.sensor_type: e.value for e in events_c1}
    val_map_c2 = {e.sensor_type: e.value for e in events_c2}
    assert val_map_c1 != val_map_c2


def test_container_simulator_rejects_empty_container_id(all_sensors_enabled_config):
    """Verify rejection of empty or blank container ID."""
    with pytest.raises(ValueError, match="container_id must be a non-empty string"):
        ContainerSimulator(container_id="   ", sensor_configs=all_sensors_enabled_config)


def test_container_simulator_unexpected_exception_propagates(all_sensors_enabled_config):
    """Verify that unexpected programming exceptions (e.g. TypeError, AttributeError) propagate."""
    from unittest.mock import MagicMock
    broken_sensor = MagicMock()
    broken_sensor.enabled = True
    broken_sensor.sensor_type = "temperature"
    broken_sensor.generate_reading.side_effect = TypeError("Unexpected bug in sensor logic")

    container = ContainerSimulator(
        container_id="CONT_ERR",
        sensor_configs=all_sensors_enabled_config,
        sensors={"temperature": broken_sensor},
    )

    with pytest.raises(TypeError, match="Unexpected bug in sensor logic"):
        container.simulate_cycle()


def test_container_simulator_recoverable_value_error_continues(all_sensors_enabled_config):
    """Verify that expected sampling exhaustion (ValueError) is caught and allows remaining sensors to proceed."""
    from unittest.mock import MagicMock

    # Temperature raises ValueError (recoverable sampling failure)
    temp_sensor = MagicMock()
    temp_sensor.enabled = True
    temp_sensor.sensor_type = "temperature"
    temp_sensor.generate_reading.side_effect = ValueError("Sampling attempts exhausted")

    # Humidity succeeds
    hum_sensor = MagicMock()
    hum_sensor.enabled = True
    hum_sensor.sensor_type = "humidity"
    hum_sensor.unit = "percent"
    hum_sensor.generate_reading.return_value = 85.0

    container = ContainerSimulator(
        container_id="CONT_REC",
        sensor_configs=all_sensors_enabled_config,
        sensors={"temperature": temp_sensor, "humidity": hum_sensor},
        propagate_sampling_errors=False,
    )

    events = container.simulate_cycle()
    # Humidity event generated despite temperature failure
    assert len(events) == 1
    assert events[0].sensor_type == "humidity"


def test_container_simulator_propagate_sampling_errors_flag(all_sensors_enabled_config):
    """Verify that propagate_sampling_errors=True raises ValueError."""
    from unittest.mock import MagicMock
    temp_sensor = MagicMock()
    temp_sensor.enabled = True
    temp_sensor.sensor_type = "temperature"
    temp_sensor.generate_reading.side_effect = ValueError("Sampling attempts exhausted")

    container = ContainerSimulator(
        container_id="CONT_PROP",
        sensor_configs=all_sensors_enabled_config,
        sensors={"temperature": temp_sensor},
        propagate_sampling_errors=True,
    )

    with pytest.raises(ValueError, match="Sampling attempts exhausted"):
        container.simulate_cycle()
