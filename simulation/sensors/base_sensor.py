"""Base virtual sensor abstraction for the AtmoSync IoT Sensor Simulator.

Provides common lifecycle management, isolated cryptographically derived PRNG streams,
finite numeric validation, and bounded rejection sampling.
"""

from abc import ABC, abstractmethod
import hashlib
import math
from typing import Optional
import random

from simulation.config.config_loader import SensorConfig


class BaseSensor(ABC):
    """Abstract base class for all AtmoSync virtual sensors.

    Attributes:
        config: The validated SensorConfig associated with this sensor.
        _rng: Dedicated pseudo-random number generator instance.
        _max_sampling_attempts: Maximum rejection-sampling attempts before raising ValueError.
    """

    def __init__(
        self,
        config: SensorConfig,
        seed: Optional[int] = None,
        max_sampling_attempts: int = 100,
    ) -> None:
        """Initialize the sensor with configuration and an isolated PRNG.

        Args:
            config: Validated SensorConfig instance.
            seed: Optional master integer seed. When provided, an independent,
                cross-process deterministic seed is derived using SHA-256.
            max_sampling_attempts: Maximum attempts to sample a valid reading
                within effective boundaries before raising an error.

        Raises:
            ValueError: If max_sampling_attempts is not a positive integer.
        """
        if max_sampling_attempts <= 0:
            raise ValueError(f"max_sampling_attempts must be positive, got {max_sampling_attempts}")

        self._config = config
        self._max_sampling_attempts = max_sampling_attempts

        if seed is not None:
            derived_seed = self.derive_sensor_seed(seed, self.sensor_type)
            self._rng = random.Random(derived_seed)
        else:
            self._rng = random.Random()

    @property
    @abstractmethod
    def sensor_type(self) -> str:
        """Return the unique sensor type identifier (e.g., 'temperature')."""
        pass

    @property
    @abstractmethod
    def precision(self) -> int:
        """Return the number of decimal places for output rounding."""
        pass

    @property
    @abstractmethod
    def physical_min(self) -> float:
        """Return the fundamental physical or mathematical minimum bound."""
        pass

    @property
    @abstractmethod
    def physical_max(self) -> float:
        """Return the fundamental physical or mathematical maximum bound."""
        pass

    @property
    def enabled(self) -> bool:
        """Return whether the sensor is active."""
        return self._config.enabled

    @property
    def unit(self) -> str:
        """Return the provisional measurement unit string."""
        return self._config.unit

    @property
    def config(self) -> SensorConfig:
        """Return the underlying SensorConfig object."""
        return self._config

    @staticmethod
    def derive_sensor_seed(master_seed: int, sensor_name: str) -> int:
        """Derive a stable 64-bit integer seed using SHA-256.

        Eliminates Python hash randomization (PYTHONHASHSEED) variance across
        separate processes and platforms.

        Args:
            master_seed: The global integer seed.
            sensor_name: Sensor identifier string.

        Returns:
            A deterministic 64-bit unsigned integer seed.
        """
        payload = f"{master_seed}:{sensor_name}".encode("utf-8")
        digest = hashlib.sha256(payload).digest()
        return int.from_bytes(digest[:8], byteorder="big")

    @abstractmethod
    def _sample_raw(self) -> float:
        """Draw an unrounded raw sample from the sensor's distribution model.

        Must be implemented by concrete sensor subclasses.
        """
        pass

    def generate_reading(self) -> Optional[float]:
        """Generate a validated, rounded sensor reading.

        If the sensor is disabled, returns None immediately without advancing
        the PRNG stream.

        When enabled, draws raw samples and verifies they fall within the effective
        physical and configured limits. Re-validates the final rounded value against
        the effective boundaries before returning.

        Returns:
            Rounded float reading if enabled, or None if disabled.

        Raises:
            ValueError: If conflicting boundaries are detected, or if no valid reading
                can be generated within max_sampling_attempts.
        """
        if not self.enabled:
            return None

        cfg_min = self._config.min_value if self._config.min_value is not None else -math.inf
        cfg_max = self._config.max_value if self._config.max_value is not None else math.inf

        eff_min = max(self.physical_min, cfg_min)
        eff_max = min(self.physical_max, cfg_max)

        if eff_min > eff_max:
            raise ValueError(
                f"Conflicting boundaries for sensor '{self.sensor_type}': "
                f"effective min ({eff_min}) > effective max ({eff_max})"
            )

        for _ in range(self._max_sampling_attempts):
            raw_val = self._sample_raw()

            if not math.isfinite(raw_val):
                continue

            if not (eff_min <= raw_val <= eff_max):
                continue

            rounded_val = round(raw_val, self.precision)

            # Re-validate after rounding to ensure no boundary leakage
            if not math.isfinite(rounded_val):
                continue

            if eff_min <= rounded_val <= eff_max:
                return rounded_val

        raise ValueError(
            f"Sensor '{self.sensor_type}' failed to generate a valid reading "
            f"within [{eff_min}, {eff_max}] after {self._max_sampling_attempts} attempts."
        )

