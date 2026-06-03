# RabbitMQ vs MQTT Benchmark Comparison

Chart: `rabbitmq_vs_mqtt_comparison_payload_256.png`

| Metric | RabbitMQ mean | MQTT mean | Better result | Difference |
| --- | ---: | ---: | --- | ---: |
| Subscriber throughput (msg/sec) | 10832.756 | 8970.596 | RabbitMQ | 20.76% |
| Publisher rate (msg/sec) | 20120.420 | 8897.584 | RabbitMQ | 126.13% |
| Average latency (ms) | 26.309 | 0.086 | MQTT | 30350.69% |
| P95 latency (ms) | 42.023 | 0.169 | MQTT | 24706.97% |
| Success rate (%) | 100.000 | 100.000 | Tie | 0.00% |
| Lost messages (messages) | 0.000 | 0.000 | Tie | 0.00% |

## Short Interpretation

- MQTT has much lower average latency in this run: 0.086 ms vs 26.309 ms for RabbitMQ.
- RabbitMQ has higher subscriber throughput in this run: 10832.756 msg/sec vs 8970.596 msg/sec for MQTT.
- Both systems delivered all messages successfully in these CSVs.

Use this result as a benchmark observation for this workload, message count, payload size, and local broker setup. It is not a universal protocol ranking.
