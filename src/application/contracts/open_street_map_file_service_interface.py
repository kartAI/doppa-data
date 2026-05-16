from abc import ABC, abstractmethod

from osmium import SimpleHandler
from osmium.osm import Area

import geopandas as gpd


class IOpenStreetMapFileService(ABC, SimpleHandler):
    @staticmethod
    @abstractmethod
    def download_pbf() -> None:
        """
        Downloads the OpenStreetMap PBF file from the configured source URL to the configured local path.
        Skips the download if the file already exists locally. The file is streamed to disk in chunks to
        avoid loading it fully into memory.
        :return: None
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def batches(self) -> list[gpd.GeoDataFrame]:
        """
        Accumulated building batches produced while parsing the PBF file. Each batch is a GeoDataFrame
        of up to `BUILDINGS_BATCH_SIZE` building features.
        :return: List of building batches as GeoDataFrames.
        :rtype: list[gpd.GeoDataFrame]
        """
        raise NotImplementedError

    @batches.setter
    @abstractmethod
    def batches(self, batches: list[gpd.GeoDataFrame]) -> None:
        """
        Overwrites the accumulated building batches with the provided list.
        :param batches: List of building batches as GeoDataFrames.
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def area(self, area: Area) -> None:
        """
        Handler invoked by Osmium for each OSM area element. When the area is tagged as a building, a
        feature is created and appended to the internal buffer. Once the buffer reaches
        `BUILDINGS_BATCH_SIZE`, it is converted into a GeoDataFrame and appended to `batches`. Areas
        whose geometry cannot be constructed are skipped with a warning.
        :param area: OSM area element provided by Osmium.
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def post_apply_file_cleanup(self) -> None:
        """
        Flushes any buildings still buffered after the PBF file has been fully parsed into a final
        batch. Called once after Osmium has finished applying the file.
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def pop_batch_by_index(self, index: int) -> None:
        """
        Removes the batch at the given index from `batches`, freeing the memory it held.
        :param index: Index of the batch to remove from `batches`.
        :return: None
        """
        raise NotImplementedError
