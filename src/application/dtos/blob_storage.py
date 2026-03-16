from dataclasses import dataclass


@dataclass
class BlobStorageUsage:
    read_operations: int
    write_operations: int
    list_operations: int
    bytes_stored_avg: int
    bytes_ingress: int
    bytes_egress: int


@dataclass(frozen=True)
class BlobStoragePricing:
    read_operation_cost: float
    write_operation_cost: float
    list_operation_cost: float
    storage_gb_per_month: float
    ingress_per_gb: float = 0.0
    egress_per_gb: float = 0.0
