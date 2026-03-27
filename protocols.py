from __future__ import annotations

import random
from typing import Callable


MAX_FANOUT_CAP = 8
MAX_INTERVAL_CAP = 3


def _empty_metrics(fanout_used=0, interval_used=1, delayed_from_queue=0):
    return {
        "messages_sent": 0,
        "successful_deliveries": 0,
        "redundant_messages": 0,
        "dropped_messages": 0,
        "congestion_drops": 0,
        "newly_informed": 0,
        "newly_informed_immediate": 0,
        "newly_informed_delayed": delayed_from_queue,
        "latency_sum": 0.0,
        "delayed_messages_scheduled": 0,
        "bandwidth_used": 0,
        "fanout_used": fanout_used,
        "interval_used": interval_used,
        "idle_round": True,
    }


def _choose_peer(network, node):
    peer = random.choice(network.nodes)
    if network.size() > 1:
        while peer.node_id == node.node_id:
            peer = random.choice(network.nodes)
    return peer


def _sample_delay_rounds(latency_mean=0.0, latency_jitter=0.0):
    if latency_mean <= 0 and latency_jitter <= 0:
        return 0

    sampled = random.gauss(latency_mean, latency_jitter)
    sampled = max(0.0, sampled)
    return int(round(sampled))


def _attempt_send(
    *,
    sender,
    receiver,
    network,
    packet_loss,
    latency_mean,
    latency_jitter,
    bandwidth_limit,
    used_bandwidth,
    newly_informed_now,
):
    metrics = {
        "messages_sent": 1,
        "successful_deliveries": 0,
        "redundant_messages": 0,
        "dropped_messages": 0,
        "congestion_drops": 0,
        "newly_informed_immediate": 0,
        "delayed_messages_scheduled": 0,
        "latency_sum": 0.0,
        "bandwidth_used": 0,
    }

    if bandwidth_limit is not None and used_bandwidth >= bandwidth_limit:
        metrics["dropped_messages"] += 1
        metrics["congestion_drops"] += 1
        return metrics

    metrics["bandwidth_used"] = 1

    if random.random() < packet_loss:
        metrics["dropped_messages"] += 1
        return metrics

    metrics["successful_deliveries"] += 1

    if receiver.has_message or receiver.node_id in newly_informed_now or network.is_pending_for(receiver.node_id):
        metrics["redundant_messages"] += 1
        return metrics

    delay_rounds = _sample_delay_rounds(latency_mean=latency_mean, latency_jitter=latency_jitter)
    metrics["latency_sum"] += delay_rounds

    if delay_rounds == 0:
        newly_informed_now.add(receiver.node_id)
        metrics["newly_informed_immediate"] += 1
    else:
        network.schedule_delivery(receiver.node_id, delay_rounds)
        metrics["delayed_messages_scheduled"] += 1

    return metrics


def _finalize_round(metrics, network, newly_informed_now, fanout_used, interval_used, delayed_from_queue):
    for node_id in newly_informed_now:
        network.nodes[node_id].has_message = True

    metrics["newly_informed_immediate"] = len(newly_informed_now)
    metrics["newly_informed_delayed"] = delayed_from_queue
    metrics["newly_informed"] = len(newly_informed_now) + delayed_from_queue
    metrics["fanout_used"] = fanout_used
    metrics["interval_used"] = interval_used
    metrics["idle_round"] = metrics.get("messages_sent", 0) == 0
    return metrics


def push_round(
    network,
    fanout=1,
    packet_loss=0.0,
    latency_mean=0.0,
    latency_jitter=0.0,
    bandwidth_limit=None,
    delayed_from_queue=0,
):
    newly_informed_now = set()
    metrics = _empty_metrics(fanout_used=fanout, interval_used=1, delayed_from_queue=delayed_from_queue)
    metrics["idle_round"] = False
    used_bandwidth = 0

    for node in network.nodes:
        if not node.has_message:
            continue

        for _ in range(fanout):
            peer = _choose_peer(network, node)
            attempt = _attempt_send(
                sender=node,
                receiver=peer,
                network=network,
                packet_loss=packet_loss,
                latency_mean=latency_mean,
                latency_jitter=latency_jitter,
                bandwidth_limit=bandwidth_limit,
                used_bandwidth=used_bandwidth,
                newly_informed_now=newly_informed_now,
            )
            used_bandwidth += attempt["bandwidth_used"]
            for key, value in attempt.items():
                metrics[key] += value

    return _finalize_round(metrics, network, newly_informed_now, fanout, 1, delayed_from_queue)


def push_pull_round(
    network,
    fanout=1,
    packet_loss=0.0,
    latency_mean=0.0,
    latency_jitter=0.0,
    bandwidth_limit=None,
    delayed_from_queue=0,
):
    newly_informed_now = set()
    metrics = _empty_metrics(fanout_used=fanout, interval_used=1, delayed_from_queue=delayed_from_queue)
    metrics["idle_round"] = False
    used_bandwidth = 0

    for node in network.nodes:
        for _ in range(fanout):
            peer = _choose_peer(network, node)

            if not node.has_message and not peer.has_message:
                continue

            # Count push-pull as two message exchanges worth of overhead.
            metrics["messages_sent"] += 1  # second direction; first comes from _attempt_send

            if node.has_message and not peer.has_message:
                receiver = peer
            elif peer.has_message and not node.has_message:
                receiver = node
            else:
                receiver = None

            if receiver is None:
                # Both sides already know the rumor.
                if bandwidth_limit is not None and used_bandwidth >= bandwidth_limit:
                    metrics["dropped_messages"] += 2
                    metrics["congestion_drops"] += 2
                    continue

                consume = 2 if bandwidth_limit is None else min(2, max(0, bandwidth_limit - used_bandwidth))
                used_bandwidth += consume
                metrics["bandwidth_used"] += consume
                metrics["successful_deliveries"] += 2 - max(0, 2 - consume)
                metrics["redundant_messages"] += 2 - max(0, 2 - consume)
                continue

            attempt = _attempt_send(
                sender=node,
                receiver=receiver,
                network=network,
                packet_loss=packet_loss,
                latency_mean=latency_mean,
                latency_jitter=latency_jitter,
                bandwidth_limit=bandwidth_limit,
                used_bandwidth=used_bandwidth,
                newly_informed_now=newly_informed_now,
            )
            used_bandwidth += attempt["bandwidth_used"]
            for key, value in attempt.items():
                metrics[key] += value

            # Add overhead for the response direction whenever bandwidth allows.
            if bandwidth_limit is not None and used_bandwidth >= bandwidth_limit:
                metrics["dropped_messages"] += 1
                metrics["congestion_drops"] += 1
            else:
                metrics["bandwidth_used"] += 1
                used_bandwidth += 1
                if random.random() < packet_loss:
                    metrics["dropped_messages"] += 1
                else:
                    metrics["successful_deliveries"] += 1
                    metrics["redundant_messages"] += 1

    return _finalize_round(metrics, network, newly_informed_now, fanout, 1, delayed_from_queue)


def _adaptive_state(network, key, base_fanout):
    if key not in network.protocol_state:
        network.protocol_state[key] = {
            "fanout": base_fanout,
            "interval": 1,
            "cooldown": 0,
        }
    return network.protocol_state[key]


def _adapt_parameters(network, state, base_fanout, success_threshold, redundancy_threshold):
    recent = list(network.recent_stats)
    if not recent:
        return state

    total_messages = sum(r["messages_sent"] for r in recent)
    total_success = sum(r["successful_deliveries"] for r in recent)
    total_redundant = sum(r["redundant_messages"] for r in recent)
    total_congestion = sum(r.get("congestion_drops", 0) for r in recent)
    total_new = sum(r["newly_informed"] for r in recent)
    avg_latency = sum(r.get("avg_latency", 0.0) for r in recent) / len(recent)

    success_rate = (total_success / total_messages) if total_messages else 1.0
    redundancy_rate = (total_redundant / total_success) if total_success else 0.0
    congestion_rate = (total_congestion / total_messages) if total_messages else 0.0
    informed_ratio = network.informed_ratio()

    if success_rate < success_threshold or (total_new == 0 and informed_ratio < 0.95):
        state["fanout"] = min(MAX_FANOUT_CAP, state["fanout"] + 1)
        state["interval"] = 1
    elif redundancy_rate > redundancy_threshold or congestion_rate > 0.10 or (avg_latency > 1.5 and informed_ratio > 0.7):
        state["fanout"] = max(base_fanout, state["fanout"] - 1)
        state["interval"] = min(MAX_INTERVAL_CAP, state["interval"] + 1)
    else:
        if state["fanout"] > base_fanout:
            state["fanout"] -= 1
        elif state["fanout"] < base_fanout:
            state["fanout"] += 1

        if state["interval"] > 1:
            state["interval"] -= 1

    return state


def _adaptive_round(
    network,
    *,
    protocol: Callable,
    state_key: str,
    base_fanout: int,
    packet_loss: float,
    latency_mean: float,
    latency_jitter: float,
    bandwidth_limit,
    success_threshold: float,
    redundancy_threshold: float,
    delayed_from_queue: int,
):
    state = _adaptive_state(network, state_key, base_fanout)
    state = _adapt_parameters(network, state, base_fanout, success_threshold, redundancy_threshold)

    if state["cooldown"] < state["interval"] - 1:
        state["cooldown"] += 1
        return _empty_metrics(
            fanout_used=state["fanout"],
            interval_used=state["interval"],
            delayed_from_queue=delayed_from_queue,
        )

    state["cooldown"] = 0
    result = protocol(
        network,
        fanout=state["fanout"],
        packet_loss=packet_loss,
        latency_mean=latency_mean,
        latency_jitter=latency_jitter,
        bandwidth_limit=bandwidth_limit,
        delayed_from_queue=delayed_from_queue,
    )
    result["interval_used"] = state["interval"]
    return result


def adaptive_push_round(
    network,
    base_fanout=1,
    packet_loss=0.0,
    latency_mean=0.0,
    latency_jitter=0.0,
    bandwidth_limit=None,
    success_threshold=0.75,
    redundancy_threshold=0.65,
    delayed_from_queue=0,
):
    return _adaptive_round(
        network,
        protocol=push_round,
        state_key="adaptive_push",
        base_fanout=base_fanout,
        packet_loss=packet_loss,
        latency_mean=latency_mean,
        latency_jitter=latency_jitter,
        bandwidth_limit=bandwidth_limit,
        success_threshold=success_threshold,
        redundancy_threshold=redundancy_threshold,
        delayed_from_queue=delayed_from_queue,
    )


def adaptive_push_pull_round(
    network,
    base_fanout=1,
    packet_loss=0.0,
    latency_mean=0.0,
    latency_jitter=0.0,
    bandwidth_limit=None,
    success_threshold=0.75,
    redundancy_threshold=0.70,
    delayed_from_queue=0,
):
    return _adaptive_round(
        network,
        protocol=push_pull_round,
        state_key="adaptive_push_pull",
        base_fanout=base_fanout,
        packet_loss=packet_loss,
        latency_mean=latency_mean,
        latency_jitter=latency_jitter,
        bandwidth_limit=bandwidth_limit,
        success_threshold=success_threshold,
        redundancy_threshold=redundancy_threshold,
        delayed_from_queue=delayed_from_queue,
    )
