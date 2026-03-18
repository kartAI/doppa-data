from dataclasses import dataclass


@dataclass
class DatabaseUsage:
    duration_seconds: float
    compute_units: float
    storage_gb_avg: float
    network_bytes_ingress: int = 0
    network_bytes_egress: int = 0
    read_iops_avg: float = 0.0
    read_throughput_bytes: int = 0


@dataclass(frozen=True)
class DatabasePricing:
    compute_per_second: float
    storage_gb_per_month: float
    network_egress_per_gb: float = 0.0
