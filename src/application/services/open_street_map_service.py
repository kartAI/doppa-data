import pandas as pd
import requests
from duckdb import DuckDBPyConnection
import geopandas as gpd

from src import Config
from src.application.common import BuildingHandler, logger


class OpenStreetMapService:
    __building_handler: BuildingHandler
    __db_context: DuckDBPyConnection

    def __init__(self, db_context: DuckDBPyConnection, building_handler: BuildingHandler):
        self.__db_context = db_context
        self.__building_handler = building_handler

    @property
    def db_context(self) -> DuckDBPyConnection:
        return self.__db_context

    @property
    def building_handler(self) -> BuildingHandler:
        return self.__building_handler

    @staticmethod
    def download_pbf() -> None:
        if Config.OSM_FILE_PATH.is_file():
            logger.info("OSM-data have already been downloaded. Skipping download...")
            return

        logger.info(f"Downloading OSM-data from '{Config.OSM_PBF_URL}'")
        response = requests.get(Config.OSM_PBF_URL, stream=True)
        response.raise_for_status()

        with open(Config.OSM_FILE_PATH, "wb") as f:
            chunks = response.iter_content(chunk_size=Config.OSM_STREAMING_CHUNK_SIZE)
            for chunk in chunks:
                f.write(chunk)

        logger.info("Download completed")

    def create_osm_parquet_file(self) -> None:
        if not Config.OSM_FILE_PATH.is_file():
            raise FileNotFoundError(
                "Failed to find OSM-dataset. Ensure that it has been installed to the correct location"
            )

        if Config.OSM_BUILDINGS_PARQUET_PATH.is_file():
            logger.info(f"'{Config.OSM_BUILDINGS_PARQUET_PATH.name}' already exists. Skipped creation step...")
            return

        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")

        self.building_handler.apply_file(str(Config.OSM_FILE_PATH), locations=True)
        self.building_handler.post_apply_file_cleanup()

        logger.info(
            f"Features extracted from the OSM-dataset. This resulted in {len(self.building_handler.batches)} batches.")
        logger.info(f"Extracting features from OSM-dataset in batches of {Config.OSM_FEATURE_BATCH_SIZE} geometries.")

        for i, building_batch in enumerate(self.building_handler.batches):
            logger.info(f"Processing batch {i + 1}/{len(self.building_handler.batches)}")
            OpenStreetMapService.__stream_batch_to_parquet(index=i, batch=building_batch)
            # self.building_handler.pop_batch_by_index(index=i)

        self.__merge_temp_parquet_files()
        logger.info(f"Extraction completed")

    @staticmethod
    def __stream_batch_to_parquet(index: int, batch: list[dict]) -> None:
        """
        Writes a batch to a temporary Parquet file using DuckDB.
        Each batch becomes its own file to avoid overwriting.
        """
        file_path = Config.OSM_TEMP_PARQUET_DIR / f"part_{index:05d}.parquet"
        batch_df = OpenStreetMapService.__create_dataframe_from_batch(batch)
        batch_df.to_parquet(
            file_path,
            index=False,
            compression="zstd",
            schema_version="1.1.0"
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
    def __create_dataframe_from_batch(batch: list[dict]) -> gpd.GeoDataFrame:
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
