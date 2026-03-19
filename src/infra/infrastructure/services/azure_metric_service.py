import datetime
import time

from azure.identity import DefaultAzureCredential
from azure.monitor.querymetrics import MetricsClient, MetricAggregationType, MetricsQueryResult

from src import Config
from src.application.common import logger
from src.application.contracts import IAzureMetricService, IBenchmarkConfigurationService
from src.application.dtos import DatabaseUsage, BlobStorageUsage, AciUsage
from src.domain.enums import AzureMetricNamespace, AzureResourceMetrics


class AzureMetricService(IAzureMetricService):
    __metrics_client: MetricsClient
    __benchmark_configuration_service: IBenchmarkConfigurationService

    def __init__(self, benchmark_configuration_service: IBenchmarkConfigurationService):
        credential = DefaultAzureCredential()
        self.__metrics_client = MetricsClient(Config.AZURE_METRICS_REGIONAL_ENDPOINT, credential)
        self.__benchmark_configuration_service = benchmark_configuration_service

    def query_metrics(
            self,
            resource_name: str,
            metric_namespace: AzureMetricNamespace,
            metric_names: AzureResourceMetrics,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            aggregations: list[MetricAggregationType]
    ) -> list[MetricsQueryResult]:
        self.__wait_for_ingestion(end_time=end_time)
        return self.__metrics_client.query_resources(
            resource_ids=[self.__create_resource_ids(metric_namespace, resource_name)],
            metric_namespace=metric_namespace.value,
            metric_names=metric_names.value,
            timespan=(start_time, end_time),
            granularity=datetime.timedelta(minutes=1),
            aggregations=aggregations,
        )

    def get_aci_usage(self, script_id: str, start_time: datetime.datetime, end_time: datetime.datetime) -> AciUsage:
        duration_seconds = (end_time - start_time).total_seconds()
        benchmark_configuration = self.__benchmark_configuration_service.get_experiment_configuration(
            script_id=script_id
        )

        return AciUsage(
            duration_seconds=duration_seconds,
            vcpu_count=benchmark_configuration.cpu,
            memory_gb=benchmark_configuration.memory_gb
        )

    def get_blob_storage_usage(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> BlobStorageUsage:
        results = self.query_metrics(
            resource_name=Config.AZURE_BLOB_STORAGE_ACCOUNT_NAME,
            metric_namespace=AzureMetricNamespace.BLOB_STORAGE,
            metric_names=AzureResourceMetrics.BLOB,
            start_time=start_time,
            end_time=end_time,
            aggregations=[MetricAggregationType.TOTAL],
        )

        return BlobStorageUsage(
            transactions=self.__extract_metric_total(results, "Transactions"),
            bytes_ingress=self.__extract_metric_total(results, "Ingress"),
            bytes_egress=self.__extract_metric_total(results, "Egress"),
        )

    def get_database_usage(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime
    ) -> DatabaseUsage:
        results = self.query_metrics(
            resource_name=Config.POSTGRES_SERVER_NAME,
            metric_namespace=AzureMetricNamespace.POSTGRESQL_FLEXIBLE,
            metric_names=AzureResourceMetrics.POSTGRES,
            start_time=start_time,
            end_time=end_time,
            aggregations=[
                MetricAggregationType.AVERAGE,
                MetricAggregationType.TOTAL,
            ],
        )

        return DatabaseUsage(
            duration_seconds=(end_time - start_time).total_seconds(),
            avg_cpu_percent=self.__extract_metric_average(results, "cpu_percent"),
            avg_memory_percent=self.__extract_metric_average(results, "memory_percent"),
            network_ingress_bytes=self.__extract_metric_total(results, "network_bytes_ingress"),
            network_egress_bytes=self.__extract_metric_total(results, "network_bytes_egress"),
            storage_used_bytes=self.__extract_metric_last(results, "storage_used"),
        )

    @staticmethod
    def __create_resource_ids(azure_metric_namespace: AzureMetricNamespace, resource_name: str) -> str:
        return f"/subscriptions/{Config.AZURE_SUBSCRIPTION_ID}/resourceGroups/{Config.AZURE_RESOURCE_GROUP}/providers/{azure_metric_namespace.value}/{resource_name}"

    @staticmethod
    def __wait_for_ingestion(end_time: datetime.datetime) -> None:
        seconds_since_end = (datetime.datetime.now(datetime.timezone.utc) - end_time).total_seconds()
        if seconds_since_end < Config.INGESTION_DELAY_SECONDS:
            wait = Config.INGESTION_DELAY_SECONDS - int(seconds_since_end)
            logger.info(f"Waiting {wait} seconds for metrics ingestion...")
            time.sleep(wait)

    @staticmethod
    def __extract_metric_total(
            results: list[MetricsQueryResult],
            metric_name: str,
    ) -> float:
        total = 0.0
        for result in results:
            for metric in result.metrics:
                if metric.name != metric_name:
                    continue
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        if data_point.total is not None:
                            total += data_point.total
                        elif data_point.average is not None:
                            total += data_point.average
        return total

    @staticmethod
    def __extract_metric_average(
            results: list[MetricsQueryResult],
            metric_name: str,
    ) -> float:
        """Compute the average across all non-null time buckets."""
        values = []
        for result in results:
            for metric in result.metrics:
                if metric.name != metric_name:
                    continue
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        if data_point.average is not None:
                            values.append(data_point.average)

        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def __extract_metric_last(
            results: list[MetricsQueryResult],
            metric_name: str,
    ) -> float:
        """Get the last non-null value (useful for gauges like storage_used)."""
        last_value = 0.0
        for result in results:
            for metric in result.metrics:
                if metric.name != metric_name:
                    continue
                for timeseries in metric.timeseries:
                    for data_point in timeseries.data:
                        if data_point.average is not None:
                            last_value = data_point.average

        return last_value
