"""Temperature virtual sensor module for the AtmoSync IoT Sensor Simulator.

Implements an independent normal-distribution sampling model around a configurable
baseline setpoint.
"""

import math
from typing import Optional

from simulation.config.config_loader import SensorConfig
from simulation.sensors.base_sensor import BaseSensor


class TemperatureSensor(BaseSensor):
    """Virtual temperature sensor producing independent Gaussian telemetry.

    MODEL ASSUMPTIONS:
        - Samples are independent, identically distributed (i.i.d.) normal random variables.
        - This model is explicitly NOT a Gaussian random walk (successive samples are uncorrelated).
        - Does not simulate thermal mass, insulation aging, ambient exchange, or reefer cycling.
        - Parameters (mean=4.0, std=1.0) are provisional demonstration defaults for software
          pipeline verification and must NOT be treated as approved preservation thresholds.

    Attributes:
        baseline_mean: Nominal mean temperature in configured units (provisional default: 4.0).
        baseline_std: Standard deviation of temperature variance (provisional default: 1.0).
    """

    def __init__(
        self,
        config: SensorConfig,
        baseline_mean: float = 4.0,
        baseline_std: float = 1.0,
        seed: Optional[int] = None,
        max_sampling_attempts: int = 100,
    ) -> None:
        """Initialize the temperature sensor.

        Args:
            config: Validated SensorConfig instance.
            baseline_mean: Mean of the Gaussian temperature distribution.
            baseline_std: Standard deviation of the Gaussian distribution (must be > 0).
            seed: Optional master integer seed for deterministic cross-process runs.
            max_sampling_attempts: Rejection-sampling budget.

        Raises:
            ValueError: If baseline_std is <= 0 or not finite, or baseline_mean is not finite.
        """
        if not math.isfinite(baseline_mean):
            raise ValueError(f"baseline_mean must be a finite number, got {baseline_mean}")

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
        """Return 'temperature'."""
        return "temperature"

    @property
    def precision(self) -> int:
        """Return 2 decimal places for temperature readings."""
        return 2

    @property
    def physical_min(self) -> float:
        """Temperature has no theoretical physical lower bound in this synthetic model."""
        return -math.inf

    @property
    def physical_max(self) -> float:
        """Temperature has no theoretical physical upper bound in this synthetic model."""
        return math.inf

    @property
    def baseline_mean(self) -> float:
        """Return the configured baseline mean."""
        return self._baseline_mean

    @property
    def baseline_std(self) -> float:
        """Return the configured baseline standard deviation."""
        return self._baseline_std

    def _sample_raw(self) -> float:
        """Draw an unrounded sample from N(mean, std^2)."""
        return self._rng.gauss(self._baseline_mean, self._baseline_std)

