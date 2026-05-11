from __future__ import annotations
from collections import deque
from node import Node


class Network:
    def __init__(self, num_nodes: int):
        if num_nodes <= 0:
            raise ValueError("num_nodes must be greater than 0")

        self.nodes = [Node(i) for i in range(num_nodes)]
        self.round_number = 0
        self.pending_deliveries: list[tuple[int, int]] = []
        self.recent_stats = deque(maxlen=5)
        self.protocol_state: dict[str, dict] = {}
        self.nodes[0].has_message = True  

    def reset(self):
        for node in self.nodes:
            node.reset()
        self.round_number = 0
        self.pending_deliveries.clear()
        self.recent_stats.clear()
        self.protocol_state.clear()
        self.nodes[0].has_message = True

    def start_round(self) -> int:
        self.round_number += 1
        delivered_now = 0
        remaining = []

        for arrival_round, node_id in self.pending_deliveries:
            if arrival_round <= self.round_number:
                node = self.nodes[node_id]
                if not node.has_message:
                    node.has_message = True
                    delivered_now += 1
            else:
                remaining.append((arrival_round, node_id))

        self.pending_deliveries = remaining
        return delivered_now

    def schedule_delivery(self, node_id: int, delay_rounds: int):
        arrival_round = self.round_number + max(1, delay_rounds)
        self.pending_deliveries.append((arrival_round, node_id))

    def is_pending_for(self, node_id: int) -> bool:
        return any(pending_id == node_id for _, pending_id in self.pending_deliveries)

    def all_informed(self) -> bool:
        return all(node.has_message for node in self.nodes)

    def informed_count(self) -> int:
        return sum(1 for node in self.nodes if node.has_message)

    def informed_ratio(self) -> float:
        return self.informed_count() / len(self.nodes)

    def size(self) -> int:
        return len(self.nodes)
