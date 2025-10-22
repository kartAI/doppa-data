from io import BytesIO

import geopandas as gpd
import pandas as pd
from duckdb import DuckDBPyConnection

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IOpenStreetMapService, IOpenStreetMapFileService, IFilePathService, IBlobStorageService
)
from src.domain.enums import Theme, StorageContainer


class OpenStreetMapService(IOpenStreetMapService):
    __db_context: DuckDBPyConnection
    __osm_file_service: IOpenStreetMapFileService
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService

    def __init__(
            self,
            db_context: DuckDBPyConnection,
            osm_file_service: IOpenStreetMapFileService,
            file_path_service: IFilePathService,
            blob_storage_service: IBlobStorageService
    ):
        self.__db_context = db_context
        self.__osm_file_service = osm_file_service
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service

    @property
    def db_context(self) -> DuckDBPyConnection:
        return self.__db_context

    @property
    def building_handler(self) -> IOpenStreetMapFileService:
        return self.__osm_file_service

    def create_osm_parquet_file(self) -> None:
        if not Config.OSM_FILE_PATH.is_file():
            raise FileNotFoundError(
                "Failed to find OSM-dataset. Ensure that it has been installed to the correct location"
            )

        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")

        self.building_handler.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        self.building_handler.post_apply_file_cleanup()

        logger.info(
            f"Features extracted from the OSM-dataset. This resulted in {len(self.building_handler.batches)} batches.")
        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")

        total_batches = len(self.building_handler.batches)
        batch_index = 0
        while self.building_handler.batches:
            building_batch = self.building_handler.batches.pop(0)
            logger.info(f"Processing batch {batch_index + 1}/{total_batches}")
            self.__stream_batch_to_storage_account(index=batch_index, batch=building_batch)
            batch_index += 1

        # self.__merge_temp_parquet_files() TODO: Remove this
        logger.info(f"Extraction completed")

    def __stream_batch_to_storage_account(self, index: int, batch: list[dict]) -> None:
        """
        Writes a batch to a temporary Parquet file using DuckDB.
        Each batch becomes its own file to avoid overwriting.
        """
        file_path = f"part_{index:05d}.parquet"
        gdf = OpenStreetMapService.__create_geodataframe_from_batch(batch)

        storage_path = self.__file_path_service.create_storage_account_file_path(
            release="2025-10-22.0",
            theme=Theme.BUILDINGS,
            region="03",
            file_name=f"part_{index:05d}.parquet",
            prefix="osm",
        )

        buffer = BytesIO()
        buffer.seek(0)

        gdf.to_parquet(
            buffer,
            index=False,
            compression="zstd",
            schema_version="1.1.0"
        )

        self.__blob_storage_service.upload_file(
            container_name=StorageContainer.RAW,
            blob_name=storage_path,
            data=buffer
        )

    def __merge_temp_parquet_files(self) -> None:
        """
        Merges all batch parquet files into a single Parquet dataset.
        """
        logger.info("Merging temp-parquet files")

        self.__db_context.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{Config.OSM_TEMP_PARQUET_DIR}/*.parquet', union_by_name=true)
            WHERE geometry IS NOT NULL
        )
        TO '{Config.OSM_BUILDINGS_PARQUET_PATH}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """)

    @staticmethod
    def __create_geodataframe_from_batch(batch: list[dict]) -> gpd.GeoDataFrame:
        dataframe = pd.DataFrame(batch)

        if "geometry" in dataframe.columns:
            dataframe = dataframe.rename(columns={"geometry": "geom_wkb"})

            dataframe["geom_wkb"] = dataframe["geom_wkb"].apply(
                lambda x: bytes.fromhex(x) if isinstance(x, str) and x[:4] == "0106" else x
            )

        geometries = gpd.GeoSeries.from_wkb(dataframe["geom_wkb"])
        gdf = gpd.GeoDataFrame(
            dataframe.drop(columns=["geom_wkb"]),
            geometry=geometries,
            crs="EPSG:4326"
        )

        return gdf
