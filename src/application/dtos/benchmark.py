from dataclasses import dataclass


@dataclass(frozen=True)
class BenchmarkConfiguration:
    id: str
    image: str
    cpu: float
    memory_gb: float
