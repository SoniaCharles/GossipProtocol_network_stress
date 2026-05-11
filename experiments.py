from __future__ import annotations
#  Controlls all experiments. 
from math import log2
from pathlib import Path
from statistics import mean, variance

import matplotlib.pyplot as plt

from simulator import run_simulation
from protocols import (
    push_round,
    push_pull_round,
    adaptive_push_round,
    adaptive_push_pull_round,
    reliability_aware_push_round,
)
SCENARIO_ORDER = [
    "Baseline",
    "Packet Loss",
    "Latency Variation",
    "Combined Stress",
]
PROTOCOL_ORDER = [
    "Push",
    "Push-Pull",
    "Adaptive Push",
    "Adaptive Push-Pull",
    "Reliability-Aware Push",
]
METRICS = {
    "convergence_success_rate": "Success Rate",
    "avg_rounds": "Convergence Time",
    "avg_messages": "Message Overhead",
    "avg_redundancy_factor": "Redundancy Factor",
    "avg_dropped": "Packet Loss",
    "avg_latency": "Latency",
    "avg_bandwidth_utilization": "Bandwidth Utilization",
    "scalability_ratio": "Scalability",
}


def _safe_mean(values):
    return mean(values) if values else None

def _safe_variance(values):
    return variance(values) if len(values) >= 2 else 0.0 if len(values) == 1 else None

def run_trials(protocol_name, protocol_func, num_nodes, scenario_name, scenario, trials=10, base_seed=0, **kwargs):
    results = []

    for trial_idx in range(trials):
        result = run_simulation(
            protocol_func=protocol_func,
            num_nodes=num_nodes,
            seed=base_seed + trial_idx,
            **scenario,
            **kwargs,
        )
        results.append(result)

    converged_trials = [r for r in results if r["converged"]]

    success_rate = len(converged_trials) / trials
    rounds_values = [r["rounds"] for r in converged_trials]
    msg_values = [r["total_messages"] for r in converged_trials]
    redundant_values = [r["redundant_messages"] for r in converged_trials]
    dropped_values = [r["dropped_messages"] for r in converged_trials]
    bandwidth_values = [r["bandwidth_utilization"] for r in converged_trials]
    latency_values = [r["avg_latency"] for r in converged_trials]
    redundancy_factor_values = [r["redundancy_factor"] for r in converged_trials]

    avg_rounds = _safe_mean(rounds_values)
    avg_messages = _safe_mean(msg_values)
    avg_redundant = _safe_mean(redundant_values)
    avg_dropped = _safe_mean(dropped_values)
    avg_bandwidth_utilization = _safe_mean(bandwidth_values)
    avg_latency = _safe_mean(latency_values)
    avg_redundancy_factor = _safe_mean(redundancy_factor_values)

    expected_log_rounds = log2(num_nodes) if num_nodes > 1 else 1.0
    scalability_ratio = (avg_rounds / expected_log_rounds) if avg_rounds is not None else None

    return {
        "protocol": protocol_name,
        "scenario": scenario_name,
        "num_nodes": num_nodes,
        "packet_loss": scenario.get("packet_loss", 0.0),
        "latency_mean": scenario.get("latency_mean", 0.0),
        "latency_jitter": scenario.get("latency_jitter", 0.0),
        "bandwidth_limit": scenario.get("bandwidth_limit"),
        "trials": trials,
        "convergence_success_rate": success_rate,
        "avg_rounds": avg_rounds,
        "var_rounds": _safe_variance(rounds_values),
        "avg_messages": avg_messages,
        "avg_redundant": avg_redundant,
        "avg_dropped": avg_dropped,
        "avg_bandwidth_utilization": avg_bandwidth_utilization,
        "avg_latency": avg_latency,
        "avg_redundancy_factor": avg_redundancy_factor,
        "scalability_ratio": scalability_ratio,
    }



def run_all_experiments(trials=10):
    protocols = [
        ("Push", push_round, {"fanout": 1}),
        ("Push-Pull", push_pull_round, {"fanout": 1}),
        ("Adaptive Push", adaptive_push_round, {"base_fanout": 1}),
        ("Adaptive Push-Pull", adaptive_push_pull_round, {"base_fanout": 1}),
        ("Reliability-Aware Push", reliability_aware_push_round, {"fanout": 1}),
    ]

    scenarios = {
        "Baseline": {
            "packet_loss": 0.0,
            "latency_mean": 0.0,
            "latency_jitter": 0.0,
            "bandwidth_limit": None,
        },
        "Packet Loss": {
            "packet_loss": 0.20,
            "latency_mean": 0.0,
            "latency_jitter": 0.0,
            "bandwidth_limit": None,
        },
        "Latency Variation": {
            "packet_loss": 0.0,
            "latency_mean": 1.5,
            "latency_jitter": 1.0,
            "bandwidth_limit": None,
        },
        "Combined Stress": {
            "packet_loss": 0.20,
            "latency_mean": 1.5,
            "latency_jitter": 1.0,
            "bandwidth_limit": 600,
        },
    }

    node_counts = [50, 100, 500, 1000]
    all_results = []

    seed_cursor = 1000
    for num_nodes in node_counts:
        for scenario_name, scenario in scenarios.items():
            adjusted_scenario = dict(scenario)
            if adjusted_scenario.get("bandwidth_limit") is not None:
                adjusted_scenario["bandwidth_limit"] = max(50, min(adjusted_scenario["bandwidth_limit"], num_nodes))

            for protocol_name, protocol_func, kwargs in protocols:
                result = run_trials(
                    protocol_name=protocol_name,
                    protocol_func=protocol_func,
                    num_nodes=num_nodes,
                    scenario_name=scenario_name,
                    scenario=adjusted_scenario,
                    trials=trials,
                    base_seed=seed_cursor,
                    **kwargs,
                )
                all_results.append(result)
                seed_cursor += trials + 17

    return all_results



def print_results_table(results):
    def fmt(value, digits=2):
        return f"{value:.{digits}f}" if value is not None else "N/A"

    for scenario in SCENARIO_ORDER:
        scenario_results = [r for r in results if r["scenario"] == scenario]
        if not scenario_results:
            continue

        scenario_results.sort(key=lambda r: (r["num_nodes"], PROTOCOL_ORDER.index(r["protocol"])))

        print(f"\n{scenario}")
        header = (
            f"{'Protocol':<20}"
            f"{'Nodes':<8}"
            f"{'Success Rate':<15}"
            f"{'Convergence Time':<18}"
            f"{'Message Overhead':<18}"
            f"{'Redundancy Factor':<20}"
            f"{'Packet Loss':<14}"
            f"{'Latency':<10}"
            f"{'Bandwidth Utilization':<24}"
            f"{'Scalability':<12}"
        )
        print(header)
        print("-" * len(header))

        for r in scenario_results:
            print(
                f"{r['protocol']:<20}"
                f"{r['num_nodes']:<8}"
                f"{fmt(r['convergence_success_rate']):<15}"
                f"{fmt(r['avg_rounds']):<18}"
                f"{fmt(r['avg_messages']):<18}"
                f"{fmt(r['avg_redundancy_factor']):<20}"
                f"{fmt(r['avg_dropped']):<14}"
                f"{fmt(r['avg_latency']):<10}"
                f"{fmt(r['avg_bandwidth_utilization']):<24}"
                f"{fmt(r['scalability_ratio']):<12}"
            )



def print_summary(results):
    print("\nBest configurations by scenario and metric:")

    metric_preferences = [
        ("avg_rounds", "Convergence Time", "min"),
        ("avg_messages", "Message Overhead", "min"),
        ("avg_redundancy_factor", "Redundancy Factor", "min"),
        ("avg_bandwidth_utilization", "Bandwidth Utilization", "min"),
        ("convergence_success_rate", "Success Rate", "max"),
    ]

    for scenario in SCENARIO_ORDER:
        subset = [r for r in results if r["scenario"] == scenario]
        if not subset:
            continue

        print(f"\n{scenario}:")
        for metric_key, metric_label, direction in metric_preferences:
            valid = [r for r in subset if r[metric_key] is not None]
            if not valid:
                continue

            if direction == "min":
                best = min(valid, key=lambda r: r[metric_key])
            else:
                best = max(valid, key=lambda r: r[metric_key])

            print(
                f"- {metric_label}: {best['protocol']} @ {best['num_nodes']} nodes "
                f"({best[metric_key]:.2f})"
            )

def print_combined_stress_bandwidth_winners(results):
    print("\nCombined Stress bandwidth winners by network size:")
    subset = [r for r in results if r["scenario"] == "Combined Stress"]

    for num_nodes in sorted({r["num_nodes"] for r in subset}):
        candidates = [r for r in subset if r["num_nodes"] == num_nodes]
        best = min(candidates, key=lambda r: r["avg_bandwidth_utilization"])
        print(
            f"- {num_nodes} nodes: {best['protocol']} "
            f"({best['avg_bandwidth_utilization']:.2f})"
        )

def save_metric_plots(results, output_dir="plots"):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for metric_key, metric_label in METRICS.items():
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        axes = axes.flatten()
        fig.suptitle(f"{metric_label} vs Number of Nodes", fontsize=14)

        for ax, scenario in zip(axes, SCENARIO_ORDER):
            scenario_results = [r for r in results if r["scenario"] == scenario]
            if not scenario_results:
                ax.set_visible(False)
                continue

            for protocol in PROTOCOL_ORDER:
                protocol_results = sorted(
                    [r for r in scenario_results if r["protocol"] == protocol],
                    key=lambda r: r["num_nodes"],
                )
                if not protocol_results:
                    continue

                x_values = [r["num_nodes"] for r in protocol_results]
                y_values = [r[metric_key] for r in protocol_results]
                ax.plot(x_values, y_values, marker="o", label=protocol)

            ax.set_title(scenario)
            ax.set_xlabel("Number of Nodes")
            ax.set_ylabel(metric_label)
            ax.set_xticks(sorted({r["num_nodes"] for r in scenario_results}))
            ax.grid(True, alpha=0.3)
            ax.legend(fontsize=8)

        plt.tight_layout(rect=(0, 0, 1, 0.96))
        file_name = metric_key.replace("_", "-") + ".png"
        fig.savefig(output_path / file_name, dpi=200, bbox_inches="tight")
        plt.close(fig)

    return output_path
