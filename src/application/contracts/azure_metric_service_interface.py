import datetime
from abc import ABC, abstractmethod

from azure.monitor.querymetrics import MetricAggregationType, MetricsQueryResult

from src.application.dtos import AciUsage, BlobStorageUsage, DatabaseUsage
from src.domain.enums import AzureMetricNamespace, AzureResourceMetrics, BlobOperationType


class IAzureMetricService(ABC):
    @abstractmethod
    def query_metrics(
            self,
            resource_name: str,
            metric_namespace: AzureMetricNamespace,
            metric_names: AzureResourceMetrics,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            aggregations: list[MetricAggregationType],
            granularity: datetime.timedelta = datetime.timedelta(minutes=1)
    ) -> list[MetricsQueryResult]:
        raise NotImplementedError

    @abstractmethod
    def get_aci_usage(
            self,
            script_id: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> AciUsage:
        raise NotImplementedError

    @abstractmethod
    def get_blob_storage_usage(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            bytes_ingress: float,
            bytes_egress: float,
            operation_type: BlobOperationType
    ) -> BlobStorageUsage:
        raise NotImplementedError

    @abstractmethod
    def get_database_usage(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> DatabaseUsage:
        raise NotImplementedError
