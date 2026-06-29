from __future__ import annotations

import threading
from collections import deque


class Metrics:
    """Thread-safe counters and a bounded latency reservoir.

    The reservoir keeps the most recent N latency samples so percentile cost and
    memory stay flat no matter how long the service runs.
    """

    def __init__(self, window: int = 1024) -> None:
        self._lock = threading.Lock()
        self._latencies_ms: deque[float] = deque(maxlen=window)
        self.processed = 0
        self.anomalies = 0
        self.errors = 0

    def record(self, latency_ms: float, is_anomaly: bool) -> None:
        with self._lock:
            self._latencies_ms.append(latency_ms)
            self.processed += 1
            if is_anomaly:
                self.anomalies += 1

    def record_error(self) -> None:
        with self._lock:
            self.errors += 1

    @staticmethod
    def _percentile(samples: list[float], pct: float) -> float:
        if not samples:
            return 0.0
        ordered = sorted(samples)
        rank = (pct / 100.0) * (len(ordered) - 1)
        low = int(rank)
        high = min(low + 1, len(ordered) - 1)
        frac = rank - low
        return ordered[low] + (ordered[high] - ordered[low]) * frac

    def snapshot(self) -> dict:
        with self._lock:
            samples = list(self._latencies_ms)
            processed = self.processed
            anomalies = self.anomalies
            errors = self.errors
        return {
            "processed": processed,
            "anomalies": anomalies,
            "errors": errors,
            "latency_ms": {
                "p50": round(self._percentile(samples, 50), 3),
                "p95": round(self._percentile(samples, 95), 3),
                "p99": round(self._percentile(samples, 99), 3),
                "samples": len(samples),
            },
        }

    def prometheus(self) -> str:
        snap = self.snapshot()
        lat = snap["latency_ms"]
        lines = [
            "# HELP rsi_processed_total Events processed.",
            "# TYPE rsi_processed_total counter",
            f"rsi_processed_total {snap['processed']}",
            "# HELP rsi_anomalies_total Anomalies detected.",
            "# TYPE rsi_anomalies_total counter",
            f"rsi_anomalies_total {snap['anomalies']}",
            "# HELP rsi_errors_total Processing errors.",
            "# TYPE rsi_errors_total counter",
            f"rsi_errors_total {snap['errors']}",
            "# HELP rsi_latency_ms Processing latency in milliseconds.",
            "# TYPE rsi_latency_ms summary",
            f'rsi_latency_ms{{quantile="0.5"}} {lat["p50"]}',
            f'rsi_latency_ms{{quantile="0.95"}} {lat["p95"]}',
            f'rsi_latency_ms{{quantile="0.99"}} {lat["p99"]}',
        ]
        return "\n".join(lines) + "\n"
