# RabbitMQ vs MQTT Lightweight Queue Benchmark

This project is a small implementation for comparing RabbitMQ and MQTT using the same simulated IoT access-message workload.

Blockchain is intentionally excluded. The current focus is only message queuing behavior.

## What It Measures

- Messages sent
- Messages received
- Lost messages
- Success rate
- Total duration
- Throughput in messages/second
- Latency: min, average, median, p95, max

## Files

- `common.py`: shared payload generation and metric calculation
- `benchmark_rabbitmq.py`: RabbitMQ/AMQP benchmark
- `benchmark_mqtt.py`: MQTT benchmark
- `requirements.txt`: Python dependencies

## Setup

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Start Brokers

RabbitMQ:

```bash
docker run --rm -p 5672:5672 --name rabbitmq-benchmark rabbitmq:3-management
```

MQTT using Mosquitto:

```bash
docker run --rm -p 1883:1883 --name mqtt-benchmark eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf
```

Run each broker in a separate terminal.

## Run Benchmarks

Each benchmark now uses separate publisher and subscriber/consumer processes.

RabbitMQ consumer terminal:

```bash
python benchmark_rabbitmq.py --role subscriber --count 1000 --payload-size 256
```

RabbitMQ publisher terminal:

```bash
python benchmark_rabbitmq.py --role publisher --count 1000 --payload-size 256
```

MQTT subscriber terminal:

```bash
python benchmark_mqtt.py --role subscriber --count 1000 --payload-size 256 --qos 1
```

MQTT publisher terminal:

```bash
python benchmark_mqtt.py --role publisher --count 1000 --payload-size 256 --qos 1
```

Start the subscriber/consumer first, then run the publisher. Use the same `--count`, `--payload-size`, and QoS where applicable for a fair comparison. The subscriber/consumer output is the main benchmark result because it contains end-to-end receive latency.

## Example Larger Test

```bash
python benchmark_rabbitmq.py --role subscriber --count 10000 --payload-size 512
python benchmark_rabbitmq.py --role publisher --count 10000 --payload-size 512

python benchmark_mqtt.py --role subscriber --count 10000 --payload-size 512 --qos 1
python benchmark_mqtt.py --role publisher --count 10000 --payload-size 512 --qos 1
```

## Suggested Internship Comparison Table

| Metric | RabbitMQ | MQTT |
| --- | --- | --- |
| Protocol | AMQP | MQTT |
| Main use case | Reliable backend queues | Lightweight IoT publish/subscribe |
| Message model | Queue/exchange/routing key | Topic publish/subscribe |
| Delivery options | Ack, persistence, routing | QoS 0, 1, 2 |
| Throughput | Use benchmark result | Use benchmark result |
| Average latency | Use benchmark result | Use benchmark result |
| Success rate | Use benchmark result | Use benchmark result |

## Notes

- RabbitMQ is usually stronger for backend task queues, routing, acknowledgements, and durable processing.
- MQTT is usually stronger for lightweight IoT device communication with topic-based publish/subscribe.
- For the paper context, RabbitMQ was used to scale backend blockchain operations. In this lightweight version, it is compared directly against MQTT as a message queuing/communication layer.
