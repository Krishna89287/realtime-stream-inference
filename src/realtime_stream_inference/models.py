from __future__ import annotations

from pydantic import BaseModel, Field


class Event(BaseModel):
    """A single measurement arriving on the stream."""

    key: str = Field(..., description="Metric or entity id, for example 'sensor-7'.")
    value: float = Field(..., description="Observed numeric value.")
    ts: float | None = Field(default=None, description="Optional event time, epoch seconds.")


class ScoreResult(BaseModel):
    key: str
    value: float
    zscore: float
    is_anomaly: bool
    mean: float
    std: float
    warming_up: bool
    latency_ms: float
