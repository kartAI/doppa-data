from abc import ABC, abstractmethod
import geopandas as gpd


class IOpenStreetMapService(ABC):
    @abstractmethod
    def create_osm_parquet_file(self, release: str) -> None:
        """Extract OSM building features and write them to Parquet batches."""
        raise NotImplementedError

    @abstractmethod
    def upload(self, release: str, region: str, partitions: list[gpd.GeoDataFrame]) -> None:
        raise NotImplementedError
