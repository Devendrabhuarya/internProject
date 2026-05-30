import argparse
import json
import time

import paho.mqtt.client as mqtt

from common import BenchmarkConfig, BenchmarkResult, add_common_arguments, build_payload, latency_ms_from_payload, print_result, write_ready_file


def run_subscriber(config: BenchmarkConfig, host: str, port: int, topic: str, qos: int, ready_file: str | None) -> BenchmarkResult:
    latencies = []
    started_at = None
    subscribed = False

    def on_message(client, userdata, message):
        nonlocal started_at
        if started_at is None:
            started_at = time.perf_counter()
        latencies.append(latency_ms_from_payload(message.payload))
        if len(latencies) >= config.count:
            client.disconnect()

    def on_subscribe(client, userdata, mid, reason_codes, properties):
        nonlocal subscribed
        subscribed = True

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.on_subscribe = on_subscribe
    client.connect(host, port, keepalive=60)
    client.subscribe(topic, qos=qos)
    subscribe_deadline = time.perf_counter() + config.timeout
    while not subscribed and time.perf_counter() < subscribe_deadline:
        client.loop(timeout=0.1)
    if not subscribed:
        client.disconnect()
        return BenchmarkResult(
            system=f"MQTT QoS {qos} subscriber",
            sent=config.count,
            received=0,
            duration_seconds=0.0,
            latencies_ms=[],
        )
    write_ready_file(ready_file)

    deadline = time.perf_counter() + config.timeout
    while len(latencies) < config.count and time.perf_counter() < deadline:
        client.loop(timeout=0.1)

    duration = (time.perf_counter() - started_at) if started_at is not None else 0.0
    client.disconnect()

    return BenchmarkResult(
        system=f"MQTT QoS {qos} subscriber",
        sent=config.count,
        received=len(latencies),
        duration_seconds=duration,
        latencies_ms=latencies,
    )


def run_publisher(config: BenchmarkConfig, host: str, port: int, topic: str, qos: int) -> dict:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(host, port, keepalive=60)
    client.loop_start()

    start = time.perf_counter()
    for sequence in range(config.count):
        info = client.publish(topic, build_payload(sequence, config.payload_size), qos=qos)
        if qos > 0:
            info.wait_for_publish()

    duration = time.perf_counter() - start
    client.loop_stop()
    client.disconnect()

    return {
        "system": f"MQTT QoS {qos} publisher",
        "role": "publisher",
        "sent": config.count,
        "duration_seconds": round(duration, 4),
        "publish_rate_msg_per_sec": round(config.count / duration, 2) if duration > 0 else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark MQTT with simulated IoT messages.")
    add_common_arguments(parser)
    parser.add_argument("--role", choices=["publisher", "subscriber"], required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument("--topic", default="iot/access/benchmark")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=1)
    args = parser.parse_args()

    config = BenchmarkConfig(args.count, args.payload_size, args.timeout)
    if args.role == "subscriber":
        result = run_subscriber(config, host=args.host, port=args.port, topic=args.topic, qos=args.qos, ready_file=args.ready_file)
        print_result(result)
    else:
        result = run_publisher(config, host=args.host, port=args.port, topic=args.topic, qos=args.qos)
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
