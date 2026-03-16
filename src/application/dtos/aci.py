from dataclasses import dataclass


@dataclass
class AciUsage:
    duration_seconds: float
    vcpu_count: float
    memory_gb: float


@dataclass(frozen=True)
class AciPricing:
    vcpu_per_second: float
    memory_gb_per_second: float
    network_egress_per_gb: float = 0.0
