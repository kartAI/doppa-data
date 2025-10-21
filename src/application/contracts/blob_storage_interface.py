from abc import ABC, abstractmethod
import geopandas as gpd


class IBlobStorageService(ABC):
    @abstractmethod
    def upload_file(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def delete_file(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def download_file(self) -> gpd.GeoDataFrame:
        raise NotImplementedError
