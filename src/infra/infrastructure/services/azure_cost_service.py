import datetime

from src.application.contracts import IAzureCostService, IAzureMetricService, IAzurePricingService
from src.application.dtos.cost import Cost

_BYTES_PER_GB = 1024 ** 3
_SECONDS_PER_MONTH = 30 * 24 * 3600


class AzureCostService(IAzureCostService):
    def compute_blob_storage_cost(self, start_time: datetime.datetime, end_time: datetime.datetime) -> Cost:
        pass

    def compute_aci_cost(self, experiment_id: str, start_time: datetime.datetime, end_time: datetime.datetime) -> Cost:
        pass

    def compute_database_cost(self, start_time: datetime.datetime, end_time: datetime.datetime) -> Cost:
        pass