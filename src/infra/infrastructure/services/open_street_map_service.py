import geopandas as gpd
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IOpenStreetMapService, IOpenStreetMapFileService, IFilePathService, IBlobStorageService, ICountyService,
    IVectorService, IStacService
)


class OpenStreetMapService(IOpenStreetMapService):
    __osm_file_service: IOpenStreetMapFileService

    def __init__(
            self,
            osm_file_service: IOpenStreetMapFileService,
    ):
        self.__osm_file_service = osm_file_service

    def create_building_batches(self) -> list[gpd.GeoDataFrame]:
        if not Config.OSM_FILE_PATH.is_file():
            raise FileNotFoundError(
                "Failed to find OSM-dataset. Ensure that it has been installed to the correct location"
            )

        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")
        self.__osm_file_service.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        self.__osm_file_service.post_apply_file_cleanup()
        logger.info(f"Batched OSM-dataset into {len(self.__osm_file_service.batches)} batches.")

        return self.__osm_file_service.batches
