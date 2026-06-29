import pytest

from realtime_stream_inference.metrics import Metrics


def test_percentiles_and_counters():
    m = Metrics(window=1000)
    for i in range(1, 101):
        m.record(latency_ms=float(i), is_anomaly=(i % 10 == 0))
    snap = m.snapshot()
    assert snap["processed"] == 100
    assert snap["anomalies"] == 10
    assert snap["latency_ms"]["p50"] == pytest.approx(50.5, abs=0.5)
    assert snap["latency_ms"]["p99"] == pytest.approx(99.01, abs=0.5)


def test_window_bounds_memory():
    m = Metrics(window=10)
    for _ in range(100):
        m.record(1.0, False)
    snap = m.snapshot()
    assert snap["latency_ms"]["samples"] == 10
    assert snap["processed"] == 100


def test_prometheus_format():
    m = Metrics()
    m.record(5.0, True)
    text = m.prometheus()
    assert "rsi_processed_total 1" in text
    assert "rsi_anomalies_total 1" in text
    assert 'quantile="0.99"' in text
