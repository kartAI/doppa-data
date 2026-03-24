import json
from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class BenchmarkConfiguration:
    id: str
    image: str
    cpu: float
    memory_gb: float

    def to_dict(self) -> dict[str, str | float]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

