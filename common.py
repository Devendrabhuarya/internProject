import argparse
import json
import statistics
import time
import uuid
from dataclasses import dataclass
from typing import List


DEFAULT_COUNT = 1000
DEFAULT_PAYLOAD_SIZE = 256
DEFAULT_TIMEOUT = 30.0


@dataclass
class BenchmarkConfig:
    count: int
    payload_size: int
    timeout: float


@dataclass
class BenchmarkResult:
    system: str
    sent: int
    received: int
    duration_seconds: float
    latencies_ms: List[float]

    @property
    def success_rate(self) -> float:
        if self.sent == 0:
            return 0.0
        return (self.received / self.sent) * 100.0

    @property
    def throughput(self) -> float:
        if self.duration_seconds <= 0:
            return 0.0
        return self.received / self.duration_seconds

    def summary(self) -> dict:
        latencies = self.latencies_ms
        return {
            "system": self.system,
            "sent": self.sent,
            "received": self.received,
            "lost": self.sent - self.received,
            "success_rate_percent": round(self.success_rate, 2),
            "duration_seconds": round(self.duration_seconds, 4),
            "throughput_msg_per_sec": round(self.throughput, 2),
            "latency_ms": {
                "min": round(min(latencies), 3) if latencies else None,
                "avg": round(statistics.mean(latencies), 3) if latencies else None,
                "median": round(statistics.median(latencies), 3) if latencies else None,
                "p95": round(percentile(latencies, 95), 3) if latencies else None,
                "max": round(max(latencies), 3) if latencies else None,
            },
        }


def percentile(values: List[float], percent: int) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = (len(ordered) - 1) * (percent / 100.0)
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def build_payload(sequence: int, payload_size: int) -> bytes:
    message = {
        "id": str(uuid.uuid4()),
        "sequence": sequence,
        "device_id": f"device-{sequence % 25:02d}",
        "resource": f"door-{sequence % 5}",
        "action": "access_request",
        "sent_at": time.perf_counter_ns(),
        "padding": "x" * max(payload_size, 0),
    }
    return json.dumps(message, separators=(",", ":")).encode("utf-8")


def parse_payload(body: bytes) -> dict:
    return json.loads(body.decode("utf-8"))


def latency_ms_from_payload(body: bytes) -> float:
    payload = parse_payload(body)
    return (time.perf_counter_ns() - payload["sent_at"]) / 1_000_000


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--count", type=int, default=DEFAULT_COUNT)
    parser.add_argument("--payload-size", type=int, default=DEFAULT_PAYLOAD_SIZE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)


def print_result(result: BenchmarkResult) -> None:
    print(json.dumps(result.summary(), indent=2))
