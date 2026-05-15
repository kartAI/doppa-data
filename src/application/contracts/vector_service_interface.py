from abc import ABC, abstractmethod
import geopandas as gpd

from src.domain.enums import EPSGCode


class IVectorService(ABC):
    @abstractmethod
    def compute_partition_key(self, dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        """
        Compute the `partition_key` column for each row in the given GeoDataFrame using a precision-3
        geohash over the LAEA Europe centroid of the geometry. Returns a copy of the input frame with
        the `partition_key` column added (or overwritten).

        :param dataframe: GeoDataFrame with a valid geometry column in WGS84.
        :return: Copy of the input frame with a `partition_key` column populated.
        """
        raise NotImplementedError

    @abstractmethod
    def partition_dataframe(self, dataframe: gpd.GeoDataFrame) -> list[gpd.GeoDataFrame]:
        raise NotImplementedError

    @abstractmethod
    def clip_dataframes_to_wkb(
            self,
            dataframes: list[gpd.GeoDataFrame],
            wkb: bytes,
            epsg_code: EPSGCode
    ) -> gpd.GeoDataFrame:
        raise NotImplementedError
