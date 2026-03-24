import datetime
from abc import abstractmethod, ABC

from src.application.dtos import Cost
from src.domain.enums import BlobOperationType


class IAzureCostService(ABC):

    @abstractmethod
    def compute_aci_cost(
            self,
            experiment_id: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> Cost:
        raise NotImplementedError

    @abstractmethod
    def compute_blob_storage_cost(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            bytes_ingress: float,
            bytes_egress: float,
            operation_type: BlobOperationType,
    ) -> Cost:
        raise NotImplementedError

    @abstractmethod
    def compute_database_cost(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
    ) -> Cost:
        raise NotImplementedError
