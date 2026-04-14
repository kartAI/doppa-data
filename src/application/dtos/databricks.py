import json
from dataclasses import asdict, dataclass


@dataclass
class DatabricksUsage:
    duration_seconds: float
    num_workers: int
    bytes_egress: float

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(frozen=True)
class DatabricksPricing:
    dbu_per_node_per_hour: float
    dbu_price_per_hour: float
    vm_cost_per_node_per_hour: float
    network_egress_per_gb: float = 0.0

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
