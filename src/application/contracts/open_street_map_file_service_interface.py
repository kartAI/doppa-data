from abc import ABC, abstractmethod

from osmium import SimpleHandler
from osmium.osm import Area

import geopandas as gpd


class IOpenStreetMapFileService(ABC, SimpleHandler):
    @staticmethod
    @abstractmethod
    def download_pbf() -> None:
        """Download OSM PBF file from configured source if not already present."""
        raise NotImplementedError

    @property
    @abstractmethod
    def batches(self) -> list[gpd.GeoDataFrame]:
        raise NotImplementedError

    @batches.setter
    @abstractmethod
    def batches(self, batches: list[gpd.GeoDataFrame]) -> None:
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
