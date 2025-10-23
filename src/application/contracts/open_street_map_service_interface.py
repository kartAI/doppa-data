from abc import ABC, abstractmethod

from duckdb import DuckDBPyConnection


class IOpenStreetMapService(ABC):
    @abstractmethod
    def create_osm_parquet_file(self) -> None:
        """Extract OSM building features and write them to Parquet batches."""
        raise NotImplementedError
