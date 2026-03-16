from dataclasses import dataclass


@dataclass
class DatabaseUsage:
    duration_seconds: float
    compute_units: float
    storage_gb_avg: float


@dataclass(frozen=True)
class DatabasePricing:
    compute_per_second: float
    storage_gb_per_month: float
    network_egress_per_gb: float = 0.0
