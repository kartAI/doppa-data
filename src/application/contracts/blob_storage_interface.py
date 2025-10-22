from abc import ABC, abstractmethod

import geopandas as gpd

from src.domain.enums import StorageContainer


class IBlobStorageService(ABC):
    @abstractmethod
    def get_container(self, container_name: StorageContainer) -> any:
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, container_name: StorageContainer, blob_name: str, data: bytes) -> str:
        raise NotImplementedError

    @abstractmethod
    def delete_file(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def download_file(self) -> gpd.GeoDataFrame:
        raise NotImplementedError
