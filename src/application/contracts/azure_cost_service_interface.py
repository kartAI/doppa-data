import datetime
from abc import ABC, abstractmethod

from src.application.dtos.cost import Cost


class IAzureCostService(ABC):

    @abstractmethod
    def compute_blob_storage_cost(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime
    ) -> Cost:
        raise NotImplementedError

    @abstractmethod
    def compute_aci_cost(
            self,
            experiment_id: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime
    ) -> Cost:
        raise NotImplementedError

    @abstractmethod
    def compute_database_cost(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime
    ) -> Cost:
        raise NotImplementedError
