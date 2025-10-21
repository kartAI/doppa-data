from abc import ABC, abstractmethod

from osmium import SimpleHandler
from osmium.geom import WKBFactory
from osmium.osm import Area


class IOpenStreetMapFileService(ABC, SimpleHandler):
    @staticmethod
    @abstractmethod
    def download_pbf() -> None:
        """Download OSM PBF file from configured source if not already present."""
        raise NotImplementedError

    @property
    @abstractmethod
    def geom_factory(self) -> WKBFactory:
        raise NotImplementedError

    @property
    @abstractmethod
    def buildings(self) -> list[dict]:
        raise NotImplementedError

    @buildings.setter
    @abstractmethod
    def buildings(self, buildings: list[dict]) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def batches(self) -> list[list[dict]]:
        raise NotImplementedError

    @batches.setter
    @abstractmethod
    def batches(self, batches: list[list[dict]]) -> None:
        raise NotImplementedError

    @abstractmethod
    def area(self, area: Area) -> None:
        """Handle a single OSM area element."""
        raise NotImplementedError

    @abstractmethod
    def post_apply_file_cleanup(self) -> None:
        """Handle any remaining data after parsing is complete."""
        raise NotImplementedError

    @abstractmethod
    def pop_batch_by_index(self, index: int) -> None:
        """Remove a batch from memory by its index."""
        raise NotImplementedError
