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
        raise NotImplementedError

    @abstractmethod
    def process_fkb_dataset(self, catalog: Catalog, release: str) -> None:
        raise NotImplementedError
