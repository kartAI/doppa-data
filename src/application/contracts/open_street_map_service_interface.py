from abc import ABC, abstractmethod

import geopandas as gpd


class IOpenStreetMapService(ABC):
    @abstractmethod
    def create_building_batches(self) -> list[gpd.GeoDataFrame]:
        """
        Create batches of building features from the OSM dataset. This batches the dataset into manageable chunks for processing, but not based on geographic regions.
        :return: List of building GeoDataFrames as batches
        """
