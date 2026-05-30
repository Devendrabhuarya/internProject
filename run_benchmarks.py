import argparse
import csv
import json
import subprocess
import sys
import tempfile
import time
import uuid
from collections import defaultdict
from pathlib import Path


CSV_COLUMNS = [
    "system",
    "run",
    "count",
    "payload_size",
    "qos",
    "sent",
    "received",
    "lost",
    "success_rate_percent",
    "subscriber_duration_seconds",
    "subscriber_throughput_msg_per_sec",
    "publisher_duration_seconds",
    "publisher_rate_msg_per_sec",
    "latency_min_ms",
    "latency_avg_ms",
    "latency_median_ms",
    "latency_p95_ms",
    "latency_max_ms",
    "status",
    "error",
]

AGGREGATE_COLUMNS = [
    "system",
    "count",
    "payload_size",
    "qos",
    "ok_runs",
    "avg_success_rate_percent",
    "avg_subscriber_throughput_msg_per_sec",
    "avg_publisher_rate_msg_per_sec",
    "avg_latency_avg_ms",
    "avg_latency_p95_ms",
    "avg_latency_max_ms",
]


def parse_int_list(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def run_command(command: list[str], timeout: float) -> tuple[int, str, str]:
    completed = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=False)
    return completed.returncode, completed.stdout, completed.stderr


def wait_for_ready(path: Path, process: subprocess.Popen, timeout: float) -> bool:
    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        if path.exists():
            return True
        if process.poll() is not None:
            return False
        time.sleep(0.05)
    return False


def parse_json_output(output: str) -> dict:
    return json.loads(output.strip())


def summarize_error(output: str, fallback: str) -> str:
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else fallback


def flatten_result(case: dict, subscriber: dict | None, publisher: dict | None, status: str, error: str = "") -> dict:
    latency = subscriber.get("latency_ms", {}) if subscriber else {}
    return {
        "system": case["system_label"],
        "run": case["run"],
        "count": case["count"],
        "payload_size": case["payload_size"],
        "qos": case.get("qos", ""),
        "sent": subscriber.get("sent") if subscriber else "",
        "received": subscriber.get("received") if subscriber else "",
        "lost": subscriber.get("lost") if subscriber else "",
        "success_rate_percent": subscriber.get("success_rate_percent") if subscriber else "",
        "subscriber_duration_seconds": subscriber.get("duration_seconds") if subscriber else "",
        "subscriber_throughput_msg_per_sec": subscriber.get("throughput_msg_per_sec") if subscriber else "",
        "publisher_duration_seconds": publisher.get("duration_seconds") if publisher else "",
        "publisher_rate_msg_per_sec": publisher.get("publish_rate_msg_per_sec") if publisher else "",
        "latency_min_ms": latency.get("min"),
        "latency_avg_ms": latency.get("avg"),
        "latency_median_ms": latency.get("median"),
        "latency_p95_ms": latency.get("p95"),
        "latency_max_ms": latency.get("max"),
        "status": status,
        "error": error,
    }


def terminate_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()


def run_pair(case: dict, args: argparse.Namespace) -> tuple[dict, dict]:
    ready_file = Path(tempfile.gettempdir()) / f"queue-benchmark-ready-{uuid.uuid4().hex}"
    subscriber = subprocess.Popen(
        case["subscriber_command"] + ["--ready-file", str(ready_file)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    if not wait_for_ready(ready_file, subscriber, args.ready_timeout):
        stdout, stderr = subscriber.communicate(timeout=1) if subscriber.poll() is not None else ("", "")
        terminate_process(subscriber)
        error = summarize_error(stderr or stdout, "subscriber did not become ready")
        row = flatten_result(case, None, None, "failed", error)
        return row, {"case": case, "status": "failed", "error": error}

    publisher = None
    try:
        code, publisher_stdout, publisher_stderr = run_command(case["publisher_command"], timeout=args.timeout + 30)
        if code != 0:
            terminate_process(subscriber)
            error = summarize_error(publisher_stderr or publisher_stdout, f"publisher exited with code {code}")
            row = flatten_result(case, None, None, "failed", error)
            return row, {"case": case, "status": "failed", "error": error}

        publisher = parse_json_output(publisher_stdout)
        subscriber_stdout, subscriber_stderr = subscriber.communicate(timeout=args.timeout + 10)
        if subscriber.returncode not in (0, None):
            error = summarize_error(subscriber_stderr or subscriber_stdout, f"subscriber exited with code {subscriber.returncode}")
            row = flatten_result(case, None, publisher, "failed", error)
            return row, {"case": case, "publisher": publisher, "status": "failed", "error": error}

        subscriber_result = parse_json_output(subscriber_stdout)
        row = flatten_result(case, subscriber_result, publisher, "ok")
        raw = {"case": case, "subscriber": subscriber_result, "publisher": publisher, "status": "ok"}
        return row, raw
    except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
        terminate_process(subscriber)
        row = flatten_result(case, None, publisher, "failed", str(exc))
        return row, {"case": case, "publisher": publisher, "status": "failed", "error": str(exc)}
    finally:
        ready_file.unlink(missing_ok=True)


def build_cases(args: argparse.Namespace) -> list[dict]:
    cases = []
    counts = parse_int_list(args.counts)
    payload_sizes = parse_int_list(args.payload_sizes)
    mqtt_qos_values = parse_int_list(args.mqtt_qos)
    run_id = uuid.uuid4().hex[:8]

    for run_number in range(1, args.runs + 1):
        for count in counts:
            for payload_size in payload_sizes:
                if "rabbitmq" in args.systems:
                    queue = f"iot-access-benchmark-{run_id}-{run_number}-{count}-{payload_size}"
                    base = [
                        sys.executable,
                        "benchmark_rabbitmq.py",
                        "--count",
                        str(count),
                        "--payload-size",
                        str(payload_size),
                        "--timeout",
                        str(args.timeout),
                        "--host",
                        args.rabbitmq_host,
                        "--port",
                        str(args.rabbitmq_port),
                        "--queue",
                        queue,
                    ]
                    cases.append(
                        {
                            "system_label": "RabbitMQ",
                            "run": run_number,
                            "count": count,
                            "payload_size": payload_size,
                            "subscriber_command": base + ["--role", "subscriber"],
                            "publisher_command": base + ["--role", "publisher"],
                        }
                    )

                if "mqtt" in args.systems:
                    for qos in mqtt_qos_values:
                        topic = f"iot/access/benchmark/{run_id}/{run_number}/{count}/{payload_size}/qos{qos}"
                        base = [
                            sys.executable,
                            "benchmark_mqtt.py",
                            "--count",
                            str(count),
                            "--payload-size",
                            str(payload_size),
                            "--timeout",
                            str(args.timeout),
                            "--host",
                            args.mqtt_host,
                            "--port",
                            str(args.mqtt_port),
                            "--topic",
                            topic,
                            "--qos",
                            str(qos),
                        ]
                        cases.append(
                            {
                                "system_label": f"MQTT QoS {qos}",
                                "run": run_number,
                                "count": count,
                                "payload_size": payload_size,
                                "qos": qos,
                                "subscriber_command": base + ["--role", "subscriber"],
                                "publisher_command": base + ["--role", "publisher"],
                            }
                        )
    return cases


def average(values: list[float | None]) -> float | str:
    numbers = [value for value in values if value is not None]
    if not numbers:
        return ""
    return round(sum(numbers) / len(numbers), 3)


def numeric(value) -> float | None:
    if value in ("", None):
        return None
    return float(value)


def write_aggregate_csv(rows: list[dict], path: Path) -> None:
    grouped = defaultdict(list)
    for row in rows:
        if row["status"] != "ok":
            continue
        key = (row["system"], row["count"], row["payload_size"], row["qos"])
        grouped[key].append(row)

    with path.open("w", encoding="utf-8", newline="") as aggregate_file:
        writer = csv.DictWriter(aggregate_file, fieldnames=AGGREGATE_COLUMNS)
        writer.writeheader()
        for key, group_rows in sorted(grouped.items()):
            system, count, payload_size, qos = key
            writer.writerow(
                {
                    "system": system,
                    "count": count,
                    "payload_size": payload_size,
                    "qos": qos,
                    "ok_runs": len(group_rows),
                    "avg_success_rate_percent": average([numeric(row["success_rate_percent"]) for row in group_rows]),
                    "avg_subscriber_throughput_msg_per_sec": average([numeric(row["subscriber_throughput_msg_per_sec"]) for row in group_rows]),
                    "avg_publisher_rate_msg_per_sec": average([numeric(row["publisher_rate_msg_per_sec"]) for row in group_rows]),
                    "avg_latency_avg_ms": average([numeric(row["latency_avg_ms"]) for row in group_rows]),
                    "avg_latency_p95_ms": average([numeric(row["latency_p95_ms"]) for row in group_rows]),
                    "avg_latency_max_ms": average([numeric(row["latency_max_ms"]) for row in group_rows]),
                }
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run automated RabbitMQ and MQTT benchmark comparisons.")
    parser.add_argument("--systems", nargs="+", choices=["rabbitmq", "mqtt"], default=["rabbitmq", "mqtt"])
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--counts", default="1000")
    parser.add_argument("--payload-sizes", default="256")
    parser.add_argument("--mqtt-qos", default="1")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--ready-timeout", type=float, default=10.0)
    parser.add_argument("--output-dir", default="results")
    parser.add_argument("--rabbitmq-host", default="localhost")
    parser.add_argument("--rabbitmq-port", type=int, default=5672)
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    csv_path = output_dir / f"benchmark-results-{timestamp}.csv"
    jsonl_path = output_dir / f"benchmark-results-{timestamp}.jsonl"
    aggregate_path = output_dir / f"benchmark-aggregate-{timestamp}.csv"

    cases = build_cases(args)
    rows = []
    with csv_path.open("w", encoding="utf-8", newline="") as csv_file, jsonl_path.open("w", encoding="utf-8") as jsonl_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for index, case in enumerate(cases, start=1):
            print(f"[{index}/{len(cases)}] {case['system_label']} run={case['run']} count={case['count']} payload={case['payload_size']}")
            row, raw = run_pair(case, args)
            writer.writerow(row)
            rows.append(row)
            jsonl_file.write(json.dumps(raw) + "\n")
            csv_file.flush()
            jsonl_file.flush()

    write_aggregate_csv(rows, aggregate_path)
    print(f"CSV: {csv_path}")
    print(f"JSONL: {jsonl_path}")
    print(f"Aggregate CSV: {aggregate_path}")


if __name__ == "__main__":
    main()
