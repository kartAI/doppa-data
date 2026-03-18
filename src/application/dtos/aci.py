import json
from dataclasses import asdict, dataclass


@dataclass
class AciUsage:
    duration_seconds: float
    vcpu_count: float
    memory_gb: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass(frozen=True)
class AciPricing:
    vcpu_per_second: float
    memory_gb_per_second: float
    network_egress_per_gb: float = 0.0

    def to_dict(self) -> dict[str, float]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

