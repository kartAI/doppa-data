from abc import ABC, abstractmethod
import geopandas as gpd
from pystac import Catalog

from src.domain.enums import EPSGCode


class IFKBService(ABC):
    @abstractmethod
    def extract_fkb_data(self) -> gpd.GeoDataFrame:
        """
        Extracts FKB data and returns it as a GeoDataFrame.
        :return: GeoDataFrame containing the extracted FKB data.
        """
        raise NotImplementedError

    @abstractmethod
    def create_building_polygons(self, gdf: gpd.GeoDataFrame, crs: EPSGCode) -> gpd.GeoDataFrame:
        """
        Create polygons for buildings from the given GeoDataFrame. Takes the FKB dataset as input and merges all building parts into single building polygons. Filters out buildings that does not have a point representation. Flattens the geometries to 2D.
        :param gdf: FKB GeoDataFrame
        :param crs: EPSG code for the coordinate reference system
        :return: Dataframe with building polygons
        """
        raise NotImplementedError
