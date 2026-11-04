"""Humidity virtual sensor module for the AtmoSync IoT Sensor Simulator.

Implements a bounded sampling model representing relative humidity (% RH)
strictly bounded within [0.0, 100.0].
"""

import math
from typing import Optional

from simulation.config.config_loader import SensorConfig
from simulation.sensors.base_sensor import BaseSensor


class HumiditySensor(BaseSensor):
    """Virtual humidity sensor producing bounded relative humidity telemetry.

    MODEL ASSUMPTIONS:
        - Confirmed to model Relative Humidity (% RH). The physical [0.0, 100.0] bound
          applies strictly to relative humidity expressed as a percentage.
        - Measurement unit (e.g. 'percent') is provisional and unconfirmed.
        - Samples are drawn from a normal distribution and bounded via rejection sampling
          to prevent artificial boundary spike concentrations at 0% or 100%.
        - Parameters (mean=85.0, std=2.5) are provisional synthetic demonstration values
          and do NOT represent official AtmoSync storage requirements.
        - Does not model moisture condensation, psychrometric curves, or transpiration.

    Attributes:
        baseline_mean: Mean relative humidity in % RH (provisional default: 85.0).
        baseline_std: Standard deviation of humidity variance (provisional default: 2.5).
    """

    def __init__(
        self,
        config: SensorConfig,
        baseline_mean: float = 85.0,
        baseline_std: float = 2.5,
        seed: Optional[int] = None,
        max_sampling_attempts: int = 100,
    ) -> None:
        """Initialize the humidity sensor.

        Args:
            config: Validated SensorConfig instance.
            baseline_mean: Mean relative humidity (must be within [0.0, 100.0]).
            baseline_std: Standard deviation (must be positive and finite).
            seed: Optional master integer seed for deterministic cross-process runs.
            max_sampling_attempts: Rejection-sampling budget.

        Raises:
            ValueError: If baseline_mean is not in [0.0, 100.0], or baseline_std <= 0 or not finite.
        """
        if not math.isfinite(baseline_mean) or not (0.0 <= baseline_mean <= 100.0):
            raise ValueError(
                f"baseline_mean for relative humidity must be in [0.0, 100.0], got {baseline_mean}"
            )

        if not math.isfinite(baseline_std) or baseline_std <= 0:
            raise ValueError(f"baseline_std must be a positive finite number, got {baseline_std}")

        self._baseline_mean = float(baseline_mean)
        self._baseline_std = float(baseline_std)

        super().__init__(
            config=config,
            seed=seed,
            max_sampling_attempts=max_sampling_attempts,
        )

    @property
    def sensor_type(self) -> str:
        """Return 'humidity'."""
        return "humidity"

    @property
    def precision(self) -> int:
        """Return 1 decimal place for relative humidity readings."""
        return 1

    @property
    def physical_min(self) -> float:
        """Relative humidity cannot physically be below 0.0%."""
        return 0.0

    @property
    def physical_max(self) -> float:
        """Relative humidity cannot physically exceed 100.0% saturation."""
        return 100.0

    @property
    def baseline_mean(self) -> float:
        """Return the configured baseline mean."""
        return self._baseline_mean

    @property
    def baseline_std(self) -> float:
        """Return the configured baseline standard deviation."""
        return self._baseline_std

    def _sample_raw(self) -> float:
        """Draw an unrounded raw sample from N(mean, std^2)."""
        return self._rng.gauss(self._baseline_mean, self._baseline_std)

