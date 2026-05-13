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


@dataclass(frozen=True)
class DatabricksRunResult:
    execution_duration_s: float
    cardinality: int
    executor_input_bytes_read: int
    executor_run_time_ms: int
    shuffle_read_bytes: int
    shuffle_write_bytes: int
    driver_collection_time_ms: int
    stage_durations_ms: str

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())
