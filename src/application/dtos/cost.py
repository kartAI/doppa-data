from dataclasses import dataclass


@dataclass
class Cost:
    total_cost: float
    compute_cost: float = 0.0
    memory_cost: float = 0.0
    storage_cost: float = 0.0
    operation_cost: float = 0.0
    network_cost: float = 0.0
