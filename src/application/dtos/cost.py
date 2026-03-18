from dataclasses import dataclass


@dataclass(frozen=True)
class Cost:
    compute_cost: float
    storage_cost: float
    network_cost: float
    operations_cost: float
    total_cost: float

    @staticmethod
    def from_components(
            compute: float = 0.0,
            storage: float = 0.0,
            network: float = 0.0,
            operations: float = 0.0,
    ) -> "Cost":
        return Cost(
            compute_cost=compute,
            storage_cost=storage,
            network_cost=network,
            operations_cost=operations,
            total_cost=compute + storage + network + operations,
        )
