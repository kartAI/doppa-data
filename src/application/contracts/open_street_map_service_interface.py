from abc import ABC, abstractmethod

import geopandas as gpd
from pystac import Catalog, Collection


class IOpenStreetMapService(ABC):
    @abstractmethod
    def process_osm_dataset(self, catalog: Catalog, release: str) -> None:
        """
        Processes the OSM dataset by extracting building features, clipping them to county polygons, partitioning them, and uploading the partitions to blob storage.
        :param catalog: Release STAC catalog for the current release. Not to be confused with the root catalog.
        :param release: Release identifier on the format "YYYY-MM-DD.x"
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def process_buildings_in_region(
            self,
            release_catalog: Catalog,
            theme_collection: Collection,
            region: str,
            release: str,
            buildings: list[gpd.GeoDataFrame]
    ) -> None:
        """
        Clips building batches to county polygon and partition the clipped polygon. Uploads each partition to blob storage.
        :param release_catalog: STAC catalog for the current release
        :param region: Two digit county identifier (e.g. "03" for Oslo)
        :param release: Release identifier on the format "YYYY-MM-DD.x"
        :param buildings: List of building GeoDataFrames to process
        :return: None
        """
        raise NotImplementedError

    @abstractmethod
    def upload(self, release: str, region: str, partitions: list[gpd.GeoDataFrame]) -> list[str]:
        """
        Uploads each partitioned GeoDataFrame to blob storage as Parquet files.
        :param release: Release identifier on the format "YYYY-MM-DD.x"
        :param region: Two digit county identifier (e.g. "03" for Oslo)
        :param partitions: List of partitioned GeoDataFrames to upload
        :return: Paths where the partitions were uploaded
        :rtype: list[str]
        """
        raise NotImplementedError
