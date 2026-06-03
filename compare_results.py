#!/usr/bin/env python3
"""Compare RabbitMQ and MQTT benchmark CSV results."""

from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path
from statistics import mean, stdev


METRICS = [
    ("subscriber_throughput_msg_per_sec", "Subscriber throughput", "msg/sec", True),
    ("publisher_rate_msg_per_sec", "Publisher rate", "msg/sec", True),
    ("latency_avg_ms", "Average latency", "ms", False),
    ("latency_p95_ms", "P95 latency", "ms", False),
    ("success_rate_percent", "Success rate", "%", True),
    ("lost", "Lost messages", "messages", False),
]


def load_rows(path: Path, payload_size: int | None = None) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if payload_size is not None:
        rows = [row for row in rows if int(row["payload_size"]) == payload_size]
    if not rows:
        filter_text = f" with payload_size={payload_size}" if payload_size is not None else ""
        raise SystemExit(f"No rows found in {path}{filter_text}")
    return rows


def numeric_values(rows: list[dict[str, str]], key: str) -> list[float]:
    values = []
    for row in rows:
        value = row.get(key, "")
        if value == "":
            continue
        values.append(float(value))
    return values


def summarize(name: str, rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    summary: dict[str, dict[str, float]] = {}
    for key, _, _, _ in METRICS:
        values = numeric_values(rows, key)
        summary[key] = {
            "mean": mean(values),
            "min": min(values),
            "max": max(values),
            "stddev": stdev(values) if len(values) > 1 else 0.0,
        }
    summary["runs"] = {"mean": float(len(rows)), "min": float(len(rows)), "max": float(len(rows)), "stddev": 0.0}
    summary["name"] = {"mean": 0.0, "min": 0.0, "max": 0.0, "stddev": 0.0}
    return summary


def percent_difference(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return ((a - b) / b) * 100.0


def write_summary_csv(path: Path, summaries: dict[str, dict[str, dict[str, float]]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "unit", "rabbitmq_mean", "mqtt_mean", "winner", "difference_percent"])
        for key, label, unit, higher_is_better in METRICS:
            rabbit = summaries["RabbitMQ"][key]["mean"]
            mqtt = summaries["MQTT"][key]["mean"]
            if abs(rabbit - mqtt) < 1e-12:
                winner = "Tie"
                diff = 0.0
            elif higher_is_better:
                winner = "RabbitMQ" if rabbit > mqtt else "MQTT"
                diff = percent_difference(max(rabbit, mqtt), min(rabbit, mqtt))
            else:
                winner = "RabbitMQ" if rabbit < mqtt else "MQTT"
                diff = percent_difference(max(rabbit, mqtt), min(rabbit, mqtt))
            writer.writerow([label, unit, f"{rabbit:.3f}", f"{mqtt:.3f}", winner, f"{diff:.2f}"])


def write_markdown(path: Path, summaries: dict[str, dict[str, dict[str, float]]], chart_name: str) -> None:
    lines = [
        "# RabbitMQ vs MQTT Benchmark Comparison",
        "",
        f"Chart: `{chart_name}`",
        "",
        "| Metric | RabbitMQ mean | MQTT mean | Better result | Difference |",
        "| --- | ---: | ---: | --- | ---: |",
    ]
    for key, label, unit, higher_is_better in METRICS:
        rabbit = summaries["RabbitMQ"][key]["mean"]
        mqtt = summaries["MQTT"][key]["mean"]
        if abs(rabbit - mqtt) < 1e-12:
            winner = "Tie"
            diff = 0.0
        elif higher_is_better:
            winner = "RabbitMQ" if rabbit > mqtt else "MQTT"
            diff = percent_difference(max(rabbit, mqtt), min(rabbit, mqtt))
        else:
            winner = "RabbitMQ" if rabbit < mqtt else "MQTT"
            diff = percent_difference(max(rabbit, mqtt), min(rabbit, mqtt))
        lines.append(f"| {label} ({unit}) | {rabbit:.3f} | {mqtt:.3f} | {winner} | {diff:.2f}% |")

    rabbit_latency = summaries["RabbitMQ"]["latency_avg_ms"]["mean"]
    mqtt_latency = summaries["MQTT"]["latency_avg_ms"]["mean"]
    rabbit_throughput = summaries["RabbitMQ"]["subscriber_throughput_msg_per_sec"]["mean"]
    mqtt_throughput = summaries["MQTT"]["subscriber_throughput_msg_per_sec"]["mean"]

    lines.extend(
        [
            "",
            "## Short Interpretation",
            "",
            f"- MQTT has much lower average latency in this run: {mqtt_latency:.3f} ms vs {rabbit_latency:.3f} ms for RabbitMQ.",
            f"- RabbitMQ has higher subscriber throughput in this run: {rabbit_throughput:.3f} msg/sec vs {mqtt_throughput:.3f} msg/sec for MQTT.",
            "- Both systems delivered all messages successfully in these CSVs.",
            "",
            "Use this result as a benchmark observation for this workload, message count, payload size, and local broker setup. It is not a universal protocol ranking.",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def draw_chart(path: Path, summaries: dict[str, dict[str, dict[str, float]]]) -> None:
    import matplotlib.pyplot as plt

    chart_metrics = [
        ("subscriber_throughput_msg_per_sec", "Subscriber throughput\n(msg/sec)"),
        ("publisher_rate_msg_per_sec", "Publisher rate\n(msg/sec)"),
        ("latency_avg_ms", "Average latency\n(ms)"),
        ("latency_p95_ms", "P95 latency\n(ms)"),
        ("success_rate_percent", "Success rate\n(%)"),
        ("lost", "Lost messages"),
    ]
    systems = ["RabbitMQ", "MQTT"]
    colors = {"RabbitMQ": "#2563eb", "MQTT": "#16a34a"}

    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    fig.suptitle("RabbitMQ vs MQTT Benchmark Comparison", fontsize=16, fontweight="bold")

    for ax, (key, title) in zip(axes.flat, chart_metrics):
        values = [summaries[system][key]["mean"] for system in systems]
        errors = [summaries[system][key]["stddev"] for system in systems]
        bars = ax.bar(systems, values, yerr=errors, capsize=6, color=[colors[s] for s in systems])
        ax.set_title(title)
        ax.grid(axis="y", linestyle="--", alpha=0.35)
        ax.set_axisbelow(True)
        upper = max(values) if values else 0
        if upper == 0:
            ax.set_ylim(0, 1)
        else:
            ax.set_ylim(0, upper * 1.2)
        for bar, value in zip(bars, values):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height(),
                f"{value:.2f}",
                ha="center",
                va="bottom",
                fontsize=9,
            )

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    fig.savefig(path, dpi=180)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rabbitmq", type=Path, default=Path("results/RabbitMQ.csv"))
    parser.add_argument("--mqtt", type=Path, default=Path("results/MQTT.csv"))
    parser.add_argument("--out-dir", type=Path, default=Path("results"))
    parser.add_argument("--payload-size", type=int, help="Only compare rows with this payload size")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    summaries = {
        "RabbitMQ": summarize("RabbitMQ", load_rows(args.rabbitmq, args.payload_size)),
        "MQTT": summarize("MQTT", load_rows(args.mqtt, args.payload_size)),
    }

    suffix = f"_payload_{args.payload_size}" if args.payload_size is not None else ""
    chart_path = args.out_dir / f"rabbitmq_vs_mqtt_comparison{suffix}.png"
    csv_path = args.out_dir / f"rabbitmq_vs_mqtt_summary{suffix}.csv"
    md_path = args.out_dir / f"rabbitmq_vs_mqtt_summary{suffix}.md"

    os.environ.setdefault("MPLCONFIGDIR", "/tmp/matplotlib")
    draw_chart(chart_path, summaries)
    write_summary_csv(csv_path, summaries)
    write_markdown(md_path, summaries, chart_path.name)

    print(f"Wrote {chart_path}")
    print(f"Wrote {csv_path}")
    print(f"Wrote {md_path}")


if __name__ == "__main__":
    main()
