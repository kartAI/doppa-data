from io import BytesIO

import geopandas as gpd
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IOpenStreetMapService, IOpenStreetMapFileService, IFilePathService, IBlobStorageService, ICountyService,
    IVectorService
)
from src.domain.enums import Theme, StorageContainer, EPSGCode


class OpenStreetMapService(IOpenStreetMapService):
    __db_context: DuckDBPyConnection
    __osm_file_service: IOpenStreetMapFileService
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService
    __county_service: ICountyService
    __vector_service: IVectorService

    def __init__(
            self,
            db_context: DuckDBPyConnection,
            osm_file_service: IOpenStreetMapFileService,
            file_path_service: IFilePathService,
            blob_storage_service: IBlobStorageService,
            county_service: ICountyService,
            vector_service: IVectorService
    ):
        self.__db_context = db_context
        self.__osm_file_service = osm_file_service
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service
        self.__county_service = county_service
        self.__vector_service = vector_service

    def create_osm_parquet_file(self, release: str) -> None:
        if not Config.OSM_FILE_PATH.is_file():
            raise FileNotFoundError(
                "Failed to find OSM-dataset. Ensure that it has been installed to the correct location"
            )

        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")

        self.__osm_file_service.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        self.__osm_file_service.post_apply_file_cleanup()

        logger.info(
            f"Features extracted from the OSM-dataset. This resulted in {len(self.__osm_file_service.batches)} batches.")
        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")

        county_ids = self.__county_service.get_county_ids()
        for county_id in county_ids:
            logger.info(f"Processing county '{county_id}'")
            county_wkb = self.__county_service.get_county_wkb_by_id(county_id=county_id, epsg_code=EPSGCode.WGS84)
            county_gdf = self.__vector_service.clip_dataframes_to_wkb(
                self.__osm_file_service.batches,
                county_wkb,
                epsg_code=EPSGCode.WGS84
            )

            logger.info(f"County '{county_id}' has {len(county_gdf)} features after clipping.")

            county_partitions = self.__vector_service.partition_dataframe(
                dataframe=county_gdf,
                batch_size=Config.OSM_FEATURE_BATCH_SIZE
            )

            self.upload(release=release, region=county_id, partitions=county_partitions)

        logger.info(f"Extraction completed")

    def upload(self, release: str, region: str, partitions: list[gpd.GeoDataFrame]) -> None:
        """
        Writes a partition to a temporary Parquet file using DuckDB.
        Each partition becomes its own file to avoid overwriting.
        """
        for index, partition in enumerate(partitions):
            storage_path = self.__file_path_service.create_storage_account_file_path(
                release=release,
                theme=Theme.BUILDINGS,
                region=region,
                file_name=f"part_{index:05d}.parquet",
                prefix="osm",
            )

            with BytesIO() as buffer:
                partition.to_parquet(
                    buffer,
                    index=False,
                    compression="snappy",
                    geometry_encoding="WKB",
                    schema_version="1.1.0",
                    write_covering_bbox=True
                )

                buffer.seek(0)
                self.__blob_storage_service.upload_file(
                    container_name=StorageContainer.RAW,
                    blob_name=storage_path,
                    data=buffer.getvalue()
                )
