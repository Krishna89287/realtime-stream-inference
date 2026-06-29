from starlette.testclient import TestClient

from realtime_stream_inference.app import app

client = TestClient(app)


def test_healthz():
    assert client.get("/healthz").json() == {"status": "ok"}


def test_score_endpoint_returns_a_score():
    r = client.post("/score", json={"key": "x", "value": 42.0})
    assert r.status_code == 200
    body = r.json()
    assert body["key"] == "x"
    assert "zscore" in body
    assert "latency_ms" in body


def test_metrics_endpoint_counts_requests():
    client.post("/score", json={"key": "m", "value": 1.0})
    snap = client.get("/metrics").json()
    assert snap["processed"] >= 1


def test_prometheus_endpoint():
    r = client.get("/metrics/prometheus")
    assert r.status_code == 200
    assert "rsi_processed_total" in r.text


def test_websocket_stream_round_trip():
    with client.websocket_connect("/stream") as ws:
        ws.send_json({"key": "w", "value": 10.0})
        msg = ws.receive_json()
        assert msg["key"] == "w"
        assert "is_anomaly" in msg
