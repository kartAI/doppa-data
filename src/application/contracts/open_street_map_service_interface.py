from abc import ABC, abstractmethod

import geopandas as gpd


class IOpenStreetMapService(ABC):
    @abstractmethod
    def create_building_batches(self) -> list[gpd.GeoDataFrame]:
        """
        Create batches of building features from the OSM dataset. The dataset is read from the
        contribution container as a single Parquet blob and returned as a list with one batch. The
        list shape is kept to mirror `IOpenStreetMapFileService.batches` which produces many batches
        when parsing PBF files directly.
        :return: List of building GeoDataFrames as batches.
        :rtype: list[gpd.GeoDataFrame]
        """
        raise NotImplementedError
