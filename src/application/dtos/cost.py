import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class Cost:
    compute_cost: float
    storage_cost: float
    network_cost: float
    operations_cost: float
    total_cost: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())


@dataclass
class CostConfiguration(frozen=True):
    include_aci: bool
    include_blob_storage: bool
    include_postgres: bool
