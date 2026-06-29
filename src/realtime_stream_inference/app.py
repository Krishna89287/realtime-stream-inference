from __future__ import annotations

import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import PlainTextResponse

from .detector import AnomalyDetector
from .metrics import Metrics
from .models import Event, ScoreResult
from .settings import settings

app = FastAPI(title="realtime-stream-inference", version="0.1.0")

detector = AnomalyDetector(
    alpha=settings.alpha, threshold=settings.threshold, warmup=settings.warmup
)
metrics = Metrics()


def _score(event: Event) -> ScoreResult:
    start = time.perf_counter()
    s = detector.score(event.key, event.value)
    latency_ms = (time.perf_counter() - start) * 1000.0
    metrics.record(latency_ms, s.is_anomaly)
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


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/score", response_model=ScoreResult)
def score(event: Event) -> ScoreResult:
    return _score(event)


@app.get("/metrics")
def metrics_json() -> dict:
    return metrics.snapshot()


@app.get("/metrics/prometheus", response_class=PlainTextResponse)
def metrics_prom() -> str:
    return metrics.prometheus()


@app.websocket("/stream")
async def stream(ws: WebSocket) -> None:
    """Bidirectional stream. Client sends events as JSON, server replies with a
    score per event in real time."""
    await ws.accept()
    try:
        while True:
            payload = await ws.receive_json()
            event = Event.model_validate(payload)
            result = _score(event)
            await ws.send_json(result.model_dump())
    except WebSocketDisconnect:
        return
