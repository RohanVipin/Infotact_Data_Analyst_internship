"""Vibration virtual sensor module for the AtmoSync IoT Sensor Simulator.

Implements a Rayleigh-distribution sampling model representing synthetic scalar
vibration magnitude (e.g., RMS acceleration) strictly non-negative.
"""

import math
from typing import Optional

from simulation.config.config_loader import SensorConfig
from simulation.sensors.base_sensor import BaseSensor


class VibrationSensor(BaseSensor):
    """Virtual vibration sensor producing non-negative Rayleigh-distributed telemetry.

    MODEL ASSUMPTIONS:
        - Represents a synthetic scalar vibration magnitude (e.g., RMS acceleration in m/s^2).
        - Does NOT simulate triaxial 3D accelerometer waveforms, frequency spectra (FFT),
          structural resonance, or physical transit shock impulses.
        - Measurement unit (e.g. 'm/s^2') is provisional and unconfirmed.
        - Output values are strictly non-negative (>= 0.0) and finite.
        - The scale parameter (sigma=0.15) is a provisional demonstration value
          and does NOT represent an official operational limit or shock threshold.

    Attributes:
        rayleigh_scale: Scale parameter sigma > 0 for the Rayleigh distribution
            (provisional default: 0.15).
    """

    def __init__(
        self,
        config: SensorConfig,
        rayleigh_scale: float = 0.15,
        seed: Optional[int] = None,
        max_sampling_attempts: int = 100,
    ) -> None:
        """Initialize the vibration sensor.

        Args:
            config: Validated SensorConfig instance.
            rayleigh_scale: Scale parameter sigma for Rayleigh distribution (must be > 0 and finite).
            seed: Optional master integer seed for deterministic cross-process runs.
            max_sampling_attempts: Rejection-sampling budget.

        Raises:
            ValueError: If rayleigh_scale <= 0 or not finite.
        """
        if not math.isfinite(rayleigh_scale) or rayleigh_scale <= 0:
            raise ValueError(f"rayleigh_scale must be a positive finite number, got {rayleigh_scale}")

        self._rayleigh_scale = float(rayleigh_scale)

        super().__init__(
            config=config,
            seed=seed,
            max_sampling_attempts=max_sampling_attempts,
        )

    @property
    def sensor_type(self) -> str:
        """Return 'vibration'."""
        return "vibration"

    @property
    def precision(self) -> int:
        """Return 3 decimal places for vibration magnitude readings."""
        return 3

    @property
    def physical_min(self) -> float:
        """Scalar vibration magnitude cannot physically be negative."""
        return 0.0

    @property
    def physical_max(self) -> float:
        """Scalar vibration magnitude has no theoretical upper bound in this synthetic model."""
        return math.inf

    @property
    def rayleigh_scale(self) -> float:
        """Return the configured Rayleigh scale parameter."""
        return self._rayleigh_scale

    def _sample_raw(self) -> float:
        """Draw an unrounded sample using the Rayleigh inverse CDF formula.

        Formula:
            u in (0, 1)
            x = sigma * sqrt(-2.0 * ln(u))
        """
        # rng.random() produces in [0.0, 1.0)
        u = 1.0 - self._rng.random()

        # Explicitly enforce open interval (0, 1) by preventing u == 1.0
        if u == 1.0:
            u = math.nextafter(1.0, 0.0)

        raw_val = self._rayleigh_scale * math.sqrt(-2.0 * math.log(u))

        # Enforce non-negativity and finite constraint
        return max(0.0, raw_val)

