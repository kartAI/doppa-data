import datetime
from typing import Literal

from azure.identity import DefaultAzureCredential
from azure.monitor.querymetrics import MetricsClient, MetricAggregationType, MetricsQueryResult

from src import Config
from src.application.contracts import IAzureMetricService, AzureMetric
from src.application.dtos import DatabaseUsage, BlobStorageUsage, AciUsage
from src.domain.enums import AzureMetricNamespace


class AzureMetricService(IAzureMetricService):
    __metrics_client: MetricsClient

    def __init__(self):
        credential = DefaultAzureCredential()
        self.__metrics_client = MetricsClient(Config.AZURE_METRICS_REGIONAL_ENDPOINT, credential)

    def query_metrics(
            self,
            resource_name: str,
            metric_namespace: AzureMetricNamespace,
            metric_names: list[AzureMetric],
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            aggregations: list[MetricAggregationType]
    ) -> list[MetricsQueryResult]:
        return self.__metrics_client.query_resources(
            resource_ids=[self.__create_resource_ids(metric_namespace, resource_name)],
            metric_namespace=metric_namespace.value,
            metric_names=metric_names,
            timespan=(start_time, end_time),
            granularity=datetime.timedelta(minutes=1),
            aggregations=aggregations,
        )

    def get_aci_usage(self, experiment_id: str, start_time: datetime.datetime, end_time: datetime.datetime) -> AciUsage:
        pass

    def get_blob_storage_usage(self, start_time: datetime.datetime, end_time: datetime.datetime) -> BlobStorageUsage:
        pass

    def get_database_usage(self, start_time: datetime.datetime, end_time: datetime.datetime) -> DatabaseUsage:
        pass

    @staticmethod
    def __create_resource_ids(azure_metric_namespace: AzureMetricNamespace, resource_name: str) -> str:
        return f"/subscriptions/{Config.AZURE_SUBSCRIPTION_ID}/resourceGroups/{Config.AZURE_RESOURCE_GROUP}/providers/{azure_metric_namespace.value}/{resource_name}"
