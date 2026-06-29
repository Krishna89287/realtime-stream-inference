import pytest

from realtime_stream_inference.detector import AnomalyDetector


def test_constant_stream_has_no_anomalies():
    d = AnomalyDetector(warmup=10, threshold=3.0)
    results = [d.score("k", 100.0) for _ in range(50)]
    assert not any(r.is_anomaly for r in results)
    assert results[-1].std == pytest.approx(0.0, abs=1e-9)


def test_spike_is_flagged():
    d = AnomalyDetector(alpha=0.05, warmup=20, threshold=3.0)
    for i in range(60):
        d.score("k", 100.0 + (1 if i % 2 == 0 else -1))
    spike = d.score("k", 200.0)
    assert spike.is_anomaly
    assert spike.zscore > 3.0


def test_warmup_suppresses_early_anomalies():
    d = AnomalyDetector(alpha=0.5, warmup=20, threshold=2.0)
    flagged = [d.score("k", v).is_anomaly for v in [1, 1000, 2, 900, 3]]
    assert not any(flagged)


def test_separate_keys_are_independent():
    d = AnomalyDetector(warmup=5, threshold=3.0)
    for _ in range(20):
        d.score("a", 10.0)
        d.score("b", 5000.0)
    assert not d.score("a", 10.0).is_anomaly
    assert not d.score("b", 5000.0).is_anomaly
    assert d.score("a", 9000.0).is_anomaly


@pytest.mark.parametrize(
    "kwargs", [{"alpha": 0}, {"alpha": 1.5}, {"threshold": 0}, {"warmup": 0}]
)
def test_invalid_params_raise(kwargs):
    with pytest.raises(ValueError):
        AnomalyDetector(**kwargs)
