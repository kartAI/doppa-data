import datetime
from abc import ABC, abstractmethod
from typing import Literal

from azure.monitor.querymetrics import MetricAggregationType, MetricsQueryResult

from src.application.dtos import AciUsage, BlobStorageUsage, DatabaseUsage
from src.domain.enums import AzureMetricNamespace

BlobMetric = Literal[
    "Transactions", "Ingress", "Egress", "BlobCapacity", "BlobCount"
]
AciMetric = Literal[
    "CpuUsage", "MemoryUsage",
    "NetworkBytesReceivedPerSecond", "NetworkBytesTransmittedPerSecond"
]
PostgresMetric = Literal[
    "cpu_percent", "memory_percent", "storage_used",
    "network_bytes_ingress", "network_bytes_egress", "iops"
]

AzureMetric = BlobMetric | AciMetric | PostgresMetric


class IAzureMetricService(ABC):
    @abstractmethod
    def query_metrics(
            self,
            resource_name: str,
            metric_namespace: AzureMetricNamespace,
            metric_names: list[AzureMetric],
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            aggregations: list[MetricAggregationType]
    ) -> list[MetricsQueryResult]:
        raise NotImplementedError

    @abstractmethod
    def get_aci_usage(
            self,
            experiment_id: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> AciUsage:
        raise NotImplementedError

    @abstractmethod
    def get_blob_storage_usage(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> BlobStorageUsage:
        raise NotImplementedError

    @abstractmethod
    def get_database_usage(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> DatabaseUsage:
        raise NotImplementedError
