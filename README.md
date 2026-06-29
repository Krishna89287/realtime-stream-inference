# realtime-stream-inference

> Catch anomalies on a live event stream without falling behind it

[![Python](https://img.shields.io/badge/python-3.10+-blue?style=flat-square)](https://python.org)
[![License](https://img.shields.io/badge/license-MIT-green?style=flat-square)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/Krishna89287/realtime-stream-inference/ci.yml?style=flat-square)](https://github.com/Krishna89287/realtime-stream-inference/actions)

Most "real-time" scoring services work fine in a notebook and then fall over the
first time the stream comes in faster than the model can score it. Events pile
up in memory, latency creeps, and eventually something gets killed.

This is built around that failure. A bounded queue sits between the source and a
pool of workers, so when scoring falls behind, the producer slows down instead
of the process running out of memory. The model is an exponentially weighted
online detector: it keeps a running mean and variance per key, adapts to slow
drift, and never stores history, so memory stays flat whether it runs for a
minute or a week.

It ships with an in-memory source so you can run it with no broker. A Kafka
adapter is sketched in [`sources.py`](src/realtime_stream_inference/sources.py)
for when you want to point it at a real topic.

## What it looks like running

```
$ make demo
processed 500 events, 3 anomalies flagged
  sensor-7: value= 141.2  z=  25.0
  sensor-7: value= 141.3  z=  25.1
  sensor-7: value= 141.4  z=  27.4
latency ms  p50=0.001  p95=0.001  p99=0.001
```

Scoring a single point over the HTTP endpoint, after the stream has warmed up:

```
$ curl -s localhost:8000/score -d '{"key":"gpu-util","value":99.0}' | jq
{
  "key": "gpu-util",
  "value": 99.0,
  "zscore": 12.76,
  "is_anomaly": true,
  "mean": 70.77,
  "std": 2.33,
  "warming_up": false,
  "latency_ms": 0.0023
}
```

## Getting started

```bash
git clone https://github.com/Krishna89287/realtime-stream-inference
cd realtime-stream-inference
pip install -r requirements-dev.txt

make demo      # synthetic stream, end to end, no broker needed
make test      # full test suite
make run       # serve the API on :8000
```

## API

| Method | Path                  | What it does                                  |
| ------ | --------------------- | --------------------------------------------- |
| POST   | `/score`              | Score one event, returns z-score and verdict  |
| GET    | `/metrics`            | Counters and p50/p95/p99 latency as JSON      |
| GET    | `/metrics/prometheus` | Same numbers in Prometheus text format        |
| WS     | `/stream`             | Send events, get a score back per event       |
| GET    | `/healthz`            | Liveness check                                |

## How the detection works

For each key the detector keeps a running mean and variance updated with an EWMA
step, so recent values count for more than old ones. A new value is scored as a
z-score against the distribution learned so far, then folded in. A short warmup
window suppresses flags until there is enough history to trust. Thresholds and
the smoothing factor are configurable through `RSI_*` environment variables, see
[`.env.example`](.env.example).

This is intentionally a lightweight model. It is cheap enough to run per event at
stream speed and good at flagging sudden level shifts and spikes. For multivariate
or seasonal data you would swap in a heavier model behind the same pipeline.

**Stack:** Python · FastAPI · Pydantic · asyncio · Prometheus · Kafka (optional)

---

Built by [Krishna Gove](https://github.com/Krishna89287), working on AI and cloud infrastructure in Munich.
