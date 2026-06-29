from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator, Awaitable, Callable

from .detector import AnomalyDetector
from .metrics import Metrics
from .models import Event, ScoreResult

Sink = Callable[[ScoreResult], Awaitable[None]]


class StreamPipeline:
    """Async pipeline: source -> bounded queue -> worker pool -> sink.

    The bounded queue is the backpressure mechanism. When the workers fall
    behind, the queue fills and ``put`` blocks the producer, so a fast source
    cannot overrun a slower model. Scoring itself is synchronous and runs to
    completion inside one task step, so the shared detector stays consistent
    without locks under the asyncio event loop.
    """

    def __init__(
        self,
        detector: AnomalyDetector,
        metrics: Metrics | None = None,
        queue_size: int = 1000,
        workers: int = 4,
    ) -> None:
        if workers < 1:
            raise ValueError("workers must be at least 1")
        self.detector = detector
        self.metrics = metrics or Metrics()
        self.queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=queue_size)
        self.workers = workers

    def score(self, event: Event) -> ScoreResult:
        start = time.perf_counter()
        s = self.detector.score(event.key, event.value)
        latency_ms = (time.perf_counter() - start) * 1000.0
        self.metrics.record(latency_ms, s.is_anomaly)
        return ScoreResult(
            key=s.key,
            value=s.value,
            zscore=s.zscore,
            is_anomaly=s.is_anomaly,
            mean=s.mean,
            std=s.std,
            warming_up=s.warming_up,
            latency_ms=latency_ms,
        )

    async def run(self, source: AsyncIterator[Event], sink: Sink) -> None:
        workers = [asyncio.create_task(self._worker(sink)) for _ in range(self.workers)]
        try:
            async for event in source:
                await self.queue.put(event)
            await self.queue.join()
        finally:
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    async def _worker(self, sink: Sink) -> None:
        while True:
            event = await self.queue.get()
            try:
                result = self.score(event)
                await sink(result)
            except Exception:
                self.metrics.record_error()
            finally:
                self.queue.task_done()
