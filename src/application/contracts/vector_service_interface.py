from abc import ABC, abstractmethod
import geopandas as gpd

from src.domain.enums import EPSGCode


class IVectorService(ABC):
    @abstractmethod
    def partition_dataframe(self, dataframe: gpd.GeoDataFrame, batch_size: int) -> list[gpd.GeoDataFrame]:
        raise NotImplementedError

    @abstractmethod
    def clip_dataframes_to_wkb(
            self,
            dataframes: list[gpd.GeoDataFrame],
            wkb: bytes,
            epsg_code: EPSGCode
    ) -> gpd.GeoDataFrame:
        raise NotImplementedError
