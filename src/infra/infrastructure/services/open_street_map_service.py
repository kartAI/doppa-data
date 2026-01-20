import geopandas as gpd

from application.contracts import IBytesService
from domain.enums import StorageContainer, EPSGCode
from src.application.contracts import (
    IOpenStreetMapService, IBlobStorageService
)


class OpenStreetMapService(IOpenStreetMapService):
    __blob_storage_service: IBlobStorageService
    __bytes_service: IBytesService

    def __init__(
            self,
            blob_storage_service: IBlobStorageService,
            bytes_service: IBytesService
    ):
        self.__blob_storage_service = blob_storage_service
        self.__bytes_service = bytes_service

    def create_building_batches(self) -> list[gpd.GeoDataFrame]:
        osm_bytes = self.__blob_storage_service.download_file(
            container_name=StorageContainer.CONTRIBUTION,
            blob_name="osm.parquet"
        )

        df = self.__bytes_service.convert_parquet_bytes_to_gdf(osm_bytes, EPSGCode.WGS84)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=EPSGCode.WGS84.value)
        return [gdf]
