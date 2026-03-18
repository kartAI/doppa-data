import json
from dataclasses import asdict, dataclass


@dataclass
class DatabaseUsage:
    duration_seconds: float
    avg_cpu_percent: float
    avg_memory_percent: float
    network_ingress_bytes: float
    network_egress_bytes: float
    storage_used_bytes: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(frozen=True)
class DatabasePricing:
    compute_per_second: float
    storage_gb_per_month: float
    network_egress_per_gb: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

