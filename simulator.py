from __future__ import annotations

import random
from network import Network


DEFAULT_SCENARIO = {
    "packet_loss": 0.0,
    "latency_mean": 0.0,
    "latency_jitter": 0.0,
    "bandwidth_limit": None,
}


def run_simulation(
    protocol_func,
    num_nodes,
    packet_loss=0.0,
    latency_mean=0.0,
    latency_jitter=0.0,
    bandwidth_limit=None,
    max_rounds=1000,
    seed=None,
    **protocol_kwargs,
):
    if seed is not None:
        random.seed(seed)

    network = Network(num_nodes)

    rounds = 0
    total_messages = 0
    total_successful_deliveries = 0
    total_redundant = 0
    total_dropped = 0
    total_congestion_drops = 0
    total_bandwidth_used = 0
    total_latency = 0.0
    total_delayed_scheduled = 0
    history = []

    while not network.all_informed() and rounds < max_rounds:
        delayed_from_queue = network.start_round()

        round_metrics = protocol_func(
            network,
            packet_loss=packet_loss,
            latency_mean=latency_mean,
            latency_jitter=latency_jitter,
            bandwidth_limit=bandwidth_limit,
            delayed_from_queue=delayed_from_queue,
            **protocol_kwargs,
        )

        rounds += 1
        total_messages += round_metrics["messages_sent"]
        total_successful_deliveries += round_metrics["successful_deliveries"]
        total_redundant += round_metrics["redundant_messages"]
        total_dropped += round_metrics["dropped_messages"]
        total_congestion_drops += round_metrics.get("congestion_drops", 0)
        total_bandwidth_used += round_metrics.get("bandwidth_used", 0)
        total_latency += round_metrics.get("latency_sum", 0.0)
        total_delayed_scheduled += round_metrics.get("delayed_messages_scheduled", 0)

        avg_latency = (
            round_metrics.get("latency_sum", 0.0) / round_metrics["successful_deliveries"]
            if round_metrics["successful_deliveries"]
            else 0.0
        )

        history_row = {
            "round": rounds,
            "informed_count": network.informed_count(),
            "messages_sent": round_metrics["messages_sent"],
            "newly_informed": round_metrics["newly_informed"],
            "newly_informed_immediate": round_metrics.get("newly_informed_immediate", 0),
            "newly_informed_delayed": round_metrics.get("newly_informed_delayed", 0),
            "successful_deliveries": round_metrics["successful_deliveries"],
            "redundant_messages": round_metrics["redundant_messages"],
            "dropped_messages": round_metrics["dropped_messages"],
            "congestion_drops": round_metrics.get("congestion_drops", 0),
            "bandwidth_used": round_metrics.get("bandwidth_used", 0),
            "fanout_used": round_metrics.get("fanout_used", 0),
            "interval_used": round_metrics.get("interval_used", 1),
            "avg_latency": avg_latency,
            "idle_round": round_metrics.get("idle_round", False),
        }
        history.append(history_row)
        network.recent_stats.append(history_row)

    avg_latency_overall = total_latency / total_successful_deliveries if total_successful_deliveries else 0.0
    bandwidth_utilization = total_bandwidth_used / rounds if rounds else 0.0
    redundancy_factor = total_redundant / total_successful_deliveries if total_successful_deliveries else 0.0

    return {
        "converged": network.all_informed(),
        "rounds": rounds,
        "total_messages": total_messages,
        "successful_deliveries": total_successful_deliveries,
        "redundant_messages": total_redundant,
        "dropped_messages": total_dropped,
        "congestion_drops": total_congestion_drops,
        "final_informed": network.informed_count(),
        "avg_latency": avg_latency_overall,
        "bandwidth_utilization": bandwidth_utilization,
        "redundancy_factor": redundancy_factor,
        "delayed_messages_scheduled": total_delayed_scheduled,
        "history": history,
    }
