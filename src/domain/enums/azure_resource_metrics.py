from enum import Enum


class AzureResourceMetrics(Enum):
    BLOB = [
        "UsedCapacity", "Transactions", "Ingress", "Egress", "SuccessServerLatency", "SuccessE2ELatency", "Availability"
    ]
    ACI = ["CpuUsage", "MemoryUsage", "NetworkBytesReceivedPerSecond", "NetworkBytesTransmittedPerSecond"]
    POSTGRES = [
        "cpu_percent", "memory_percent", "storage_used", "network_bytes_ingress", "network_bytes_egress",
        "iops"
    ]
