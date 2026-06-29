from __future__ import annotations

import math
from dataclasses import dataclass

# Sentinel z-score used when the running variance is exactly zero but a new
# value differs from the mean. A flat stream has no variance, so any deviation
# is effectively infinite; we cap it at a large finite number to stay JSON safe.
_MAX_Z = 1e6


@dataclass
class StreamState:
    """Running mean and variance for one metric key."""

    count: int = 0
    mean: float = 0.0
    var: float = 0.0


@dataclass
class Score:
    key: str
    value: float
    zscore: float
    is_anomaly: bool
    mean: float
    std: float
    warming_up: bool


class AnomalyDetector:
    """
    Exponentially weighted online anomaly detector.

    Keeps a running mean and variance per key with an EWMA update, so it adapts
    to slow drift without storing history. Each new value is scored against the
    distribution learned so far, then folded in. Points past the warmup window
    whose z-score exceeds the threshold are flagged.
    """

    def __init__(self, alpha: float = 0.05, threshold: float = 3.0, warmup: int = 20) -> None:
        if not 0.0 < alpha <= 1.0:
            raise ValueError("alpha must be in (0, 1]")
        if threshold <= 0:
            raise ValueError("threshold must be positive")
        if warmup < 1:
            raise ValueError("warmup must be at least 1")
        self.alpha = alpha
        self.threshold = threshold
        self.warmup = warmup
        self._state: dict[str, StreamState] = {}

    def score(self, key: str, value: float) -> Score:
        state = self._state.get(key)
        if state is None:
            state = StreamState()
            self._state[key] = state

        warming_up = state.count < self.warmup

        if state.count == 0:
            # First sample seeds the mean. No variance to compare against yet.
            std = 0.0
            zscore = 0.0
            state.mean = value
            state.var = 0.0
        else:
            std = math.sqrt(state.var)
            zscore = self._zscore(value, state.mean, std)
            diff = value - state.mean
            incr = self.alpha * diff
            state.mean += incr
            state.var = (1 - self.alpha) * (state.var + diff * incr)

        state.count += 1
        is_anomaly = (not warming_up) and abs(zscore) >= self.threshold
        return Score(
            key=key,
            value=value,
            zscore=zscore,
            is_anomaly=is_anomaly,
            mean=state.mean,
            std=std,
            warming_up=warming_up,
        )

    @staticmethod
    def _zscore(value: float, mean: float, std: float) -> float:
        if std > 0:
            return (value - mean) / std
        if value == mean:
            return 0.0
        return math.copysign(_MAX_Z, value - mean)
