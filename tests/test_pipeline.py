import asyncio

from realtime_stream_inference.detector import AnomalyDetector
from realtime_stream_inference.metrics import Metrics
from realtime_stream_inference.models import Event
from realtime_stream_inference.pipeline import StreamPipeline
from realtime_stream_inference.sources import iterable_source


def test_pipeline_processes_every_event_and_flags_spike():
    async def main():
        detector = AnomalyDetector(warmup=5, threshold=3.0)
        metrics = Metrics()
        # Queue smaller than the event count, so the producer hits backpressure.
        pipe = StreamPipeline(detector, metrics, queue_size=8, workers=3)

        events = [Event(key="s", value=100.0) for _ in range(50)]
        events.append(Event(key="s", value=100000.0))

        out = []

        async def sink(result):
            out.append(result)

        await pipe.run(iterable_source(events), sink)
        return metrics, out

    metrics, out = asyncio.run(main())
    assert len(out) == 51
    assert metrics.snapshot()["processed"] == 51
    assert any(r.is_anomaly for r in out)


def test_sink_errors_are_counted_not_raised():
    async def main():
        pipe = StreamPipeline(AnomalyDetector(warmup=1), Metrics(), queue_size=4, workers=2)

        async def bad_sink(result):
            raise RuntimeError("downstream is down")

        events = [Event(key="s", value=1.0) for _ in range(5)]
        await pipe.run(iterable_source(events), bad_sink)
        return pipe.metrics.snapshot()

    snap = asyncio.run(main())
    assert snap["errors"] == 5
