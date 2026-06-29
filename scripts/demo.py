"""Run a synthetic sensor stream through the pipeline and print what trips.

This is the quickest way to see the service work end to end without a broker.
It feeds 500 readings that sit around 100 with light noise, injects three
spikes, and reports which ones the detector caught plus latency percentiles.
"""
from __future__ import annotations

import asyncio
import random

from realtime_stream_inference.detector import AnomalyDetector
from realtime_stream_inference.metrics import Metrics
from realtime_stream_inference.models import Event
from realtime_stream_inference.pipeline import StreamPipeline
from realtime_stream_inference.sources import iterable_source

SPIKE_AT = {180, 300, 455}


def synthetic_events(n: int = 500) -> list[Event]:
    rng = random.Random(7)
    events: list[Event] = []
    for i in range(n):
        value = 100.0 + rng.gauss(0, 1.5)
        if i in SPIKE_AT:
            value += 40.0
        events.append(Event(key="sensor-7", value=value))
    return events


async def main() -> None:
    detector = AnomalyDetector(alpha=0.05, threshold=4.0, warmup=30)
    metrics = Metrics()
    pipe = StreamPipeline(detector, metrics, queue_size=256, workers=4)

    hits: list = []

    async def sink(result):
        if result.is_anomaly:
            hits.append(result)

    await pipe.run(iterable_source(synthetic_events()), sink)

    snap = metrics.snapshot()
    print(f"processed {snap['processed']} events, {len(hits)} anomalies flagged")
    for r in hits:
        print(f"  {r.key}: value={r.value:6.1f}  z={r.zscore:6.1f}")
    lat = snap["latency_ms"]
    print(f"latency ms  p50={lat['p50']}  p95={lat['p95']}  p99={lat['p99']}")


if __name__ == "__main__":
    asyncio.run(main())
