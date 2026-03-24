from abc import abstractmethod, ABC

from src.application.dtos import AciPricing, BlobStoragePricing, DatabasePricing


class IAzurePricingService(ABC):

    @abstractmethod
    def get_aci_pricing(self) -> AciPricing:
        raise NotImplementedError

    @abstractmethod
    def get_blob_storage_pricing(self) -> BlobStoragePricing:
        raise NotImplementedError

    @abstractmethod
    def get_database_pricing(self) -> DatabasePricing:
        raise NotImplementedError
