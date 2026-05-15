import math

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from duckdb import DuckDBPyConnection
from shapely.affinity import rotate, translate

from src import Config
from src.application.common import logger
from src.application.contracts import (
    IBlobStorageService,
    ICountyService,
    IDatasetSynthesisService,
    IFilePathService,
    IVectorService,
)
from src.domain.enums import DatasetSize, EPSGCode, StorageContainer, Theme


class DatasetSynthesisService(IDatasetSynthesisService):
    __db_context: DuckDBPyConnection
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService
    __county_service: ICountyService
    __vector_service: IVectorService

    def __init__(
        self,
        db_context: DuckDBPyConnection,
        file_path_service: IFilePathService,
        blob_storage_service: IBlobStorageService,
        county_service: ICountyService,
        vector_service: IVectorService,
    ):
        self.__db_context = db_context
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service
        self.__county_service = county_service
        self.__vector_service = vector_service

    def run_pipeline(self, release: str, target_size: DatasetSize) -> None:
        clones_per_polygon = target_size.clones_per_polygon
        if clones_per_polygon <= 0:
            raise ValueError(
                f"DatasetSynthesisService only supports sizes with a positive clones_per_polygon. Got '{target_size.value}' ({clones_per_polygon})."
            )

        regions = self.__county_service.get_county_ids()

        logger.info(
            f"Synthesizing '{target_size.value}' dataset for release '{release}' with {clones_per_polygon} clones per source polygon across {len(regions)} regions."
        )

        for region in regions:
            logger.info(
                f"Synthesizing region '{region}' for size '{target_size.value}'..."
            )
            self.__synthesize_region(
                release=release,
                region=region,
                target_size=target_size,
                clones_per_polygon=clones_per_polygon,
            )

        logger.info(
            f"Synthesis of '{target_size.value}' dataset complete for release '{release}'."
        )

    def __synthesize_region(
        self,
        release: str,
        region: str,
        target_size: DatasetSize,
        clones_per_polygon: int,
    ) -> None:
        county_wkb, _ = self.__county_service.get_county_polygons_by_id(
            county_id=region, epsg_code=EPSGCode.WGS84
        )
        county_polygon = shapely.from_wkb(county_wkb)
        county_minx, county_miny, county_maxx, county_maxy = county_polygon.bounds

        source_path = self.__file_path_service.create_release_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.DATA,
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            file_name="*.parquet",
            dataset_size=DatasetSize.SMALL,
        )

        originals_geodataframe = self.__read_source_polygons(source_path=source_path)
        if originals_geodataframe is None or originals_geodataframe.empty:
            logger.warning(
                f"No source rows for region '{region}' at '{source_path}'. Skipping."
            )
            return

        clones_geodataframe = self.__generate_clones(
            originals_geodataframe=originals_geodataframe,
            region=region,
            clones_per_polygon=clones_per_polygon,
            county_bounds=(county_minx, county_miny, county_maxx, county_maxy),
        )

        combined_geodataframe = self.__combine_and_finalize(
            originals_geodataframe=originals_geodataframe,
            clones_geodataframe=clones_geodataframe,
        )

        logger.info(
            f"Region '{region}': uploading {len(combined_geodataframe)} rows as size '{target_size.value}'"
        )
        self.__blob_storage_service.upload_blobs_as_parquet(
            container=StorageContainer.DATA,
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            partitions=[combined_geodataframe],
            dataset_size=target_size,
            row_group_size=Config.GEOPARQUET_ROW_GROUP_SIZE,
        )

    def __read_source_polygons(self, source_path: str) -> gpd.GeoDataFrame | None:
        dataframe = self.__db_context.execute(
            f"""
            SELECT
                ST_AsWKB(geometry) AS geometry,
                * EXCLUDE (geometry)
            FROM read_parquet('{source_path}', union_by_name = true)
            """
        ).fetchdf()

        if dataframe.empty:
            return None

        dataframe = dataframe.drop(columns=["bbox"], errors="ignore")

        dataframe["geometry"] = dataframe["geometry"].apply(
            lambda geometry: bytes(geometry) if isinstance(geometry, (bytearray, memoryview)) else geometry
        )
        dataframe["geometry"] = shapely.from_wkb(dataframe["geometry"].to_numpy())

        geodataframe = gpd.GeoDataFrame(
            dataframe, geometry="geometry", crs=f"EPSG:{EPSGCode.WGS84.value}"
        )

        valid_mask = shapely.is_valid(geodataframe.geometry.to_numpy())
        invalid_count = int((~valid_mask).sum())
        if invalid_count:
            logger.info(
                f"Dropping {invalid_count} invalid source polygons before cloning"
            )
            geodataframe = geodataframe.loc[valid_mask].reset_index(drop=True)

        return geodataframe

    def __generate_clones(
        self,
        originals_geodataframe: gpd.GeoDataFrame,
        region: str,
        clones_per_polygon: int,
        county_bounds: tuple[float, float, float, float],
    ) -> gpd.GeoDataFrame:
        county_minx, county_miny, county_maxx, county_maxy = county_bounds
        source_geometries = originals_geodataframe.geometry.to_numpy()
        source_count = len(source_geometries)
        clone_count = source_count * clones_per_polygon

        logger.info(
            f"Region '{region}': generating {clone_count} candidate clones from {source_count} source polygons"
        )

        centroids = shapely.centroid(source_geometries)
        source_centroid_x = shapely.get_x(centroids)
        source_centroid_y = shapely.get_y(centroids)

        random_generator = np.random.default_rng()
        target_centroid_x = random_generator.uniform(county_minx, county_maxx, clone_count)
        target_centroid_y = random_generator.uniform(county_miny, county_maxy, clone_count)
        rotation_radians = random_generator.uniform(0.0, 2.0 * math.pi, clone_count)
        jitter_x = random_generator.uniform(-Config.SYNTHESIS_JITTER_DEGREES, Config.SYNTHESIS_JITTER_DEGREES, clone_count)
        jitter_y = random_generator.uniform(-Config.SYNTHESIS_JITTER_DEGREES, Config.SYNTHESIS_JITTER_DEGREES, clone_count)

        repeated_geometries = np.repeat(source_geometries, clones_per_polygon)
        repeated_centroid_x = np.repeat(source_centroid_x, clones_per_polygon)
        repeated_centroid_y = np.repeat(source_centroid_y, clones_per_polygon)

        clone_geometries = np.empty(clone_count, dtype=object)
        for clone_index in range(clone_count):
            rotated_geometry = rotate(
                repeated_geometries[clone_index],
                rotation_radians[clone_index],
                origin=(repeated_centroid_x[clone_index], repeated_centroid_y[clone_index]),
                use_radians=True,
            )
            clone_geometries[clone_index] = translate(
                rotated_geometry,
                xoff=target_centroid_x[clone_index] + jitter_x[clone_index] - repeated_centroid_x[clone_index],
                yoff=target_centroid_y[clone_index] + jitter_y[clone_index] - repeated_centroid_y[clone_index],
            )

        valid_mask = shapely.is_valid(clone_geometries)
        dropped_count = int((~valid_mask).sum())
        if dropped_count:
            logger.info(
                f"Region '{region}': dropped {dropped_count} invalid clones after transform"
            )
        clone_geometries = clone_geometries[valid_mask]

        if len(clone_geometries) == 0:
            return gpd.GeoDataFrame(
                {
                    "region": pd.Series([], dtype="object"),
                    "partition_key": pd.Series([], dtype="object"),
                },
                geometry=gpd.GeoSeries([], crs=f"EPSG:{EPSGCode.WGS84.value}"),
            )

        clones_geodataframe = gpd.GeoDataFrame(
            {"region": [region] * len(clone_geometries)},
            geometry=gpd.GeoSeries(clone_geometries),
            crs=f"EPSG:{EPSGCode.WGS84.value}",
        )

        return self.__vector_service.compute_partition_key(clones_geodataframe)

    @staticmethod
    def __combine_and_finalize(
        originals_geodataframe: gpd.GeoDataFrame,
        clones_geodataframe: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        combined = pd.concat([originals_geodataframe, clones_geodataframe], ignore_index=True)
        combined_geodataframe = gpd.GeoDataFrame(
            combined, geometry="geometry", crs=originals_geodataframe.crs
        )
        combined_geodataframe = combined_geodataframe.sort_values("partition_key").reset_index(drop=True)
        return combined_geodataframe
