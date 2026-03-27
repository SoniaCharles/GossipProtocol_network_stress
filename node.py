class Node:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.has_message = False

    def reset(self):
        self.has_message = False