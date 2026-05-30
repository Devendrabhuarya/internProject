import argparse
import json
import time

import pika

from common import BenchmarkConfig, BenchmarkResult, add_common_arguments, build_payload, latency_ms_from_payload, print_result, write_ready_file


def connect(host: str, port: int, queue: str):
    params = pika.ConnectionParameters(host=host, port=port)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=queue, durable=False, auto_delete=True)
    return connection, channel


def run_subscriber(config: BenchmarkConfig, host: str, port: int, queue: str, purge: bool, ready_file: str | None) -> BenchmarkResult:
    connection, channel = connect(host, port, queue)
    if purge:
        channel.queue_purge(queue=queue)
    write_ready_file(ready_file)

    latencies = []
    started_at = None
    deadline = time.perf_counter() + config.timeout

    while len(latencies) < config.count and time.perf_counter() < deadline:
        method, _, body = channel.basic_get(queue=queue, auto_ack=True)
        if method is None:
            time.sleep(0.001)
            continue
        if started_at is None:
            started_at = time.perf_counter()
        latencies.append(latency_ms_from_payload(body))

    duration = (time.perf_counter() - started_at) if started_at is not None else 0.0
    connection.close()

    return BenchmarkResult(
        system="RabbitMQ/AMQP subscriber",
        sent=config.count,
        received=len(latencies),
        duration_seconds=duration,
        latencies_ms=latencies,
    )


def run_publisher(config: BenchmarkConfig, host: str, port: int, queue: str) -> dict:
    connection, channel = connect(host, port, queue)
    channel.queue_purge(queue=queue)

    start = time.perf_counter()
    for sequence in range(config.count):
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=build_payload(sequence, config.payload_size),
            properties=pika.BasicProperties(delivery_mode=1),
        )

    duration = time.perf_counter() - start
    connection.close()

    return {
        "system": "RabbitMQ/AMQP publisher",
        "role": "publisher",
        "sent": config.count,
        "duration_seconds": round(duration, 4),
        "publish_rate_msg_per_sec": round(config.count / duration, 2) if duration > 0 else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark RabbitMQ with simulated IoT messages.")
    add_common_arguments(parser)
    parser.add_argument("--role", choices=["publisher", "subscriber"], required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5672)
    parser.add_argument("--queue", default="iot-access-benchmark")
    parser.add_argument("--no-purge", action="store_true", help="Do not purge the queue when starting a subscriber.")
    args = parser.parse_args()

    config = BenchmarkConfig(args.count, args.payload_size, args.timeout)
    if args.role == "subscriber":
        result = run_subscriber(config, host=args.host, port=args.port, queue=args.queue, purge=not args.no_purge, ready_file=args.ready_file)
        print_result(result)
    else:
        result = run_publisher(config, host=args.host, port=args.port, queue=args.queue)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
