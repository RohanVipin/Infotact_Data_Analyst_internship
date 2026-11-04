"""Automated unit tests for SimulationEngine."""

from pathlib import Path
from typing import List
import pytest

from simulation.config.config_loader import AppConfig, SensorConfig, SimulationSettings, load_config
from simulation.events.event_schema import SensorEvent
from simulation.simulator.simulation_engine import SimulationEngine
from simulation.validation.event_validator import EventValidator


@pytest.fixture
def mock_app_config():
    """Return a 3-container AppConfig with all sensors active."""
    return AppConfig(
        simulation=SimulationSettings(interval_seconds=1.0, random_seed=42),
        containers=("CONTAINER_001", "CONTAINER_002", "CONTAINER_003"),
        sensors={
            "temperature": SensorConfig(enabled=True, unit="celsius"),
            "humidity": SensorConfig(enabled=True, unit="percent"),
            "vibration": SensorConfig(enabled=True, unit="m/s^2"),
        },
    )


def test_simulation_engine_single_cycle(mock_app_config):
    """Verify one cycle across 3 containers with 3 sensors produces exactly 9 events."""
    engine = SimulationEngine(config=mock_app_config)
    assert len(engine.containers) == 3

    events = engine.run_cycle()
    # 3 containers * 3 enabled sensors = 9 events
    assert len(events) == 9

    # Verify 3 events per container
    for cid in mock_app_config.containers:
        container_events = [e for e in events if e.container_id == cid]
        assert len(container_events) == 3
        assert {e.sensor_type for e in container_events} == {"temperature", "humidity", "vibration"}

    # Verify all generated events pass strict validation
    for event in events:
        EventValidator.validate(event)


def test_simulation_engine_finite_cycles_without_real_delay(mock_app_config):
    """Verify running finite cycles records sleep calls without actual test execution delays."""
    sleep_calls: List[float] = []

    def fake_sleep(duration: float) -> None:
        sleep_calls.append(duration)

    engine = SimulationEngine(config=mock_app_config, sleep_fn=fake_sleep)

    # Run for exactly 4 cycles
    all_events = engine.run(max_cycles=4)

    # 4 cycles * 9 events per cycle = 36 events
    assert len(all_events) == 36

    # Between 4 cycles, sleep should be called exactly 3 times with interval 1.0
    assert len(sleep_calls) == 3
    assert all(d == 1.0 for d in sleep_calls)


def test_simulation_engine_stream_generator(mock_app_config):
    """Verify stream() yields individual events incrementally."""
    sleep_calls: List[float] = []
    engine = SimulationEngine(config=mock_app_config, sleep_fn=lambda d: sleep_calls.append(d))

    streamed_events = list(engine.stream(max_cycles=2))
    # 2 cycles * 9 events = 18 events
    assert len(streamed_events) == 18
    assert all(isinstance(e, SensorEvent) for e in streamed_events)
    assert len(sleep_calls) == 1


def test_simulation_engine_event_callback(mock_app_config):
    """Verify callback is invoked for each generated event."""
    received_events: List[SensorEvent] = []
    engine = SimulationEngine(config=mock_app_config, sleep_fn=lambda _: None)

    engine.run(max_cycles=1, callback=lambda e: received_events.append(e))
    assert len(received_events) == 9


def test_simulation_engine_graceful_stop(mock_app_config):
    """Verify stop() terminates execution cleanly."""
    cycles_seen = 0

    def stopping_callback(event: SensorEvent) -> None:
        nonlocal cycles_seen
        cycles_seen += 1
        if cycles_seen >= 9:  # after 1st cycle (9 events)
            engine.stop()

    engine = SimulationEngine(config=mock_app_config, sleep_fn=lambda _: None)
    # Start continuous run with max_cycles=None
    events = engine.run(max_cycles=None, callback=stopping_callback)

    assert engine.is_stopped is True
    assert len(events) == 9


def test_simulation_engine_integration_with_default_config_yaml():
    """Verify end-to-end integration with the default simulation_config.yaml."""
    default_config_path = Path("simulation/config/simulation_config.yaml")
    config = load_config(default_config_path)

    engine = SimulationEngine(config=config, sleep_fn=lambda _: None)
    events = engine.run_cycle()

    # Default config has 3 containers and 3 active sensors
    assert len(events) == 9
    for event in events:
        EventValidator.validate(event)
        assert event.schema_version == 1
        assert event.unit in {"celsius", "percent", "m/s^2"}


def test_simulation_engine_fail_fast_default_raises(mock_app_config):
    """Verify that default fail_fast=True immediately propagates container exceptions."""
    from unittest.mock import MagicMock
    broken_sim = MagicMock()
    broken_sim.simulate_cycle.side_effect = RuntimeError("Container hardware communication loss")

    working_sim = MagicMock()
    working_sim.simulate_cycle.return_value = []

    engine = SimulationEngine(
        config=mock_app_config,
        containers={"CONTAINER_001": broken_sim, "CONTAINER_002": working_sim},
        fail_fast=True,
    )

    with pytest.raises(RuntimeError, match="Container hardware communication loss"):
        engine.run_cycle()

    assert "CONTAINER_001" in engine.last_cycle_failed_containers


def test_simulation_engine_configurable_recovery_policy(mock_app_config):
    """Verify that fail_fast=False logs and records failures without aborting other containers."""
    from unittest.mock import MagicMock
    broken_sim = MagicMock()
    broken_sim.simulate_cycle.side_effect = RuntimeError("Container hardware error")

    working_event = SensorEvent(
        schema_version=1,
        event_id="11111111-1111-4111-8111-111111111111",
        container_id="CONTAINER_002",
        sensor_type="temperature",
        observed_at="2026-11-04T12:00:00.000Z",
        value=4.0,
        unit="celsius",
    )
    working_sim = MagicMock()
    working_sim.simulate_cycle.return_value = [working_event]

    engine = SimulationEngine(
        config=mock_app_config,
        containers={"CONTAINER_001": broken_sim, "CONTAINER_002": working_sim},
        fail_fast=False,
    )

    events = engine.run_cycle()
    assert len(events) == 1
    assert events[0].container_id == "CONTAINER_002"
    assert engine.last_cycle_failed_containers == ["CONTAINER_001"]
