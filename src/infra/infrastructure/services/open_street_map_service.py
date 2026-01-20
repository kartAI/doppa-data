from warnings import deprecated

import geopandas as gpd

from application.contracts import IBytesService
from domain.enums import StorageContainer, EPSGCode
from src import Config
from src.application.common import logger
from src.application.contracts import (
    IOpenStreetMapService, IOpenStreetMapFileService, IBlobStorageService
)


class OpenStreetMapService(IOpenStreetMapService):
    __osm_file_service: IOpenStreetMapFileService
    __blob_storage_service: IBlobStorageService
    __bytes_service: IBytesService

    def __init__(
            self,
            osm_file_service: IOpenStreetMapFileService,
            blob_storage_service: IBlobStorageService,
            bytes_service: IBytesService
    ):
        self.__osm_file_service = osm_file_service
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

    # def create_building_batches(self) -> list[gpd.GeoDataFrame]:
    #     if not Config.OSM_FILE_PATH.is_file():
    #         raise FileNotFoundError(
    #             "Failed to find OSM-dataset. Ensure that it has been installed to the correct location"
    #         )
    #
    #     logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")
    #     self.__osm_file_service.apply_file(str(Config.OSM_FILE_PATH), locations=True)
    #     self.__osm_file_service.post_apply_file_cleanup()
    #     logger.info(f"Batched OSM-dataset into {len(self.__osm_file_service.batches)} batches.")
    #
    #     return self.__osm_file_service.batches
