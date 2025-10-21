from abc import ABC, abstractmethod

from duckdb import DuckDBPyConnection


class IOpenStreetMapService(ABC):
    @property
    @abstractmethod
    def db_context(self) -> DuckDBPyConnection:
        raise NotImplementedError

    @property
    @abstractmethod
    def building_handler(self):
        """Return the handler used to extract OSM buildings."""
        raise NotImplementedError

    @abstractmethod
    def create_osm_parquet_file(self) -> None:
        """Extract OSM building features and write them to Parquet batches."""
        raise NotImplementedError
