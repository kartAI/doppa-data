from typing import Dict

import pandas as pd
import geopandas as gpd
from duckdb import DuckDBPyConnection
from shapely import from_wkb

from src.application.common import logger
from src.application.contracts import IConflationService, IFilePathService
from src.domain.enums import Theme, StorageContainer, DataSource, EPSGCode


class ConflationService(IConflationService):
    __db_context: DuckDBPyConnection
    __file_path_service: IFilePathService

    def __init__(self, db_context: DuckDBPyConnection, file_path_service: IFilePathService):
        self.__db_context = db_context
        self.__file_path_service = file_path_service

    def get_fkb_osm_id_relations(self, release: str, theme: Theme) -> pd.DataFrame:
        logger.info(f"Finding diff between the raw OSM- and FKB-datasets.")

        osm_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region="*",
            file_name="*.parquet",
            dataset=DataSource.OSM.value
        )

        fkb_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region="*",
            file_name="*.parquet",
            dataset=DataSource.FKB.value
        )

        ids_df = self.__db_context.execute(
            f'''
            WITH fkb AS (
                SELECT
                    lokalId AS fkb_id,
                    TRY_CAST("bygningsnummer" AS INTEGER) AS building_id,
                    CAST(FLOOR(ST_X(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_x,
                    CAST(FLOOR(ST_Y(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_y,
                    ST_Force2D(geometry) AS geom,
                FROM '{fkb_release}'
            ),

            osm AS (
                SELECT
                    id AS osm_id,
                    TRY_CAST("ref:bygningsnr" AS INTEGER) AS building_id,
                    CAST(FLOOR(ST_X(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_x,
                    CAST(FLOOR(ST_Y(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_y,
                    ST_Force2D(geometry) AS geom,
                FROM '{osm_release}'
            ),

            candidate_fkb_only_buildings AS (
                SELECT
                    f.fkb_id AS fkb_id,
                    o.osm_id AS osm_id,
                    CAST(MAX (
                        ST_Area(ST_Intersection(f.geom, o.geom)) / NULLIF(ST_Area(ST_Union(f.geom, o.geom)), 0)
                    ) AS DECIMAL) AS max_iou
                FROM fkb f
                LEFT JOIN osm o
                    ON f.grid_x = o.grid_x
                    AND f.grid_y = o.grid_y
                    AND ST_Intersects(f.geom, o.geom)
                GROUP BY f.fkb_id, o.osm_id
            ),

            candidate_osm_only_buildings AS (
                SELECT
                    f.fkb_id AS fkb_id,
                    o.osm_id AS osm_id,
                    CAST(MAX (
                        ST_Area(ST_Intersection(f.geom, o.geom)) / NULLIF(ST_Area(ST_Union(f.geom, o.geom)), 0)
                    ) AS DECIMAL) AS max_iou
                FROM osm o
                LEFT JOIN fkb f
                    ON f.grid_x = o.grid_x
                    AND f.grid_y = o.grid_y
                    AND ST_Intersects(f.geom, o.geom)
                GROUP BY f.fkb_id, o.osm_id
            ),

            fkb_only AS (
                SELECT
                    fkb_id,
                    osm_id,
                FROM candidate_fkb_only_buildings
                WHERE max_iou IS NULL
            ),

            osm_only AS (
                SELECT
                    fkb_id,
                    osm_id,
                FROM candidate_osm_only_buildings
                WHERE max_iou IS NULL
            ),

            fkb_osm_overlap AS (
                SELECT
                    f.fkb_id AS fkb_id,
                    o.osm_id AS osm_id,
                    ST_Area(ST_Intersection(f.geom, o.geom)) / ST_Area(ST_Union(f.geom, o.geom)) AS iou,
                FROM fkb f
                JOIN osm o
                    ON f.grid_x = o.grid_x
                    AND f.grid_y = o.grid_y
                WHERE ST_Intersects(f.geom, o.geom)
            ),

            merged AS (
                SELECT
                    fkb_id,
                    osm_id,
                FROM fkb_only
                UNION
                SELECT
                    fkb_id,
                    osm_id,
                FROM osm_only
                UNION
                SELECT
                    fkb_id,
                    osm_id,
                FROM fkb_osm_overlap
                WHERE iou > 0.70
            )

            SELECT * FROM merged
            '''
        ).fetchdf()

        osm_only_count = ids_df[ids_df["fkb_id"].isna()].shape[0]
        fkb_only_count = ids_df[ids_df["osm_id"].isna()].shape[0]
        overlapping_count = ids_df[ids_df["fkb_id"].notna() & ids_df["osm_id"].notna()].shape[0]

        logger.info(
            f"Conflation step completed. Result: {osm_only_count} OSM-only buildings, {fkb_only_count} FKB-only buildings, and {overlapping_count} overlapping buildings."
        )

        return ids_df

    def merge_fkb_osm(self, release: str, theme: Theme, ids: pd.DataFrame) -> Dict[str, list[gpd.GeoDataFrame]]:
        logger.info(f"Merging OSM- and FKB-datasets")
        osm_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region="*",
            file_name="*.parquet",
            dataset=DataSource.OSM.value
        )

        fkb_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region="*",
            file_name="*.parquet",
            dataset=DataSource.FKB.value
        )

        ids_fkb = ids[ids["fkb_id"].notna()]["fkb_id"].to_list()
        ids_osm = ids[ids["fkb_id"].isna() & ids["osm_id"].notna()]["osm_id"].to_list()

        fkb_filter = (
            "FALSE"
            if not ids_fkb
            else f'external_id IN ({", ".join(f"'{v}'" for v in ids_fkb)})'
        )

        osm_filter = (
            "FALSE"
            if not ids_osm
            else f'external_id IN ({", ".join(f"'{v}'" for v in ids_osm)})'
        )

        df = self.__db_context.execute(
            f'''
            WITH fkb AS (
                SELECT 
                    lokalId AS external_id,
                    ST_AsWKB(geometry) AS geometry,
                    bbox,
                    region,
                    partition_key,
                    TRY_CAST(bygningstype AS VARCHAR) AS type, 
                    TRY_CAST(bygningsnummer AS INTEGER) AS building_id,
                    'fkb' AS source
                FROM '{fkb_release}'
                WHERE {fkb_filter}
            ),
            osm AS (
                SELECT 
                    id AS external_id,
                    ST_AsWKB(geometry) as geometry,
                    bbox,
                    region,
                    partition_key,
                    type,
                    TRY_CAST("ref:bygningsnr" AS INTEGER) AS building_id,
                    'osm' AS source
                FROM '{osm_release}'
                WHERE {osm_filter}
            ),
            
            conflated AS (
                SELECT * FROM fkb
                UNION
                SELECT * FROM osm
            )
            
            SELECT * FROM conflated
            '''
        ).fetchdf()

        df["geometry"] = df["geometry"].apply(lambda g: bytes(g) if isinstance(g, (bytearray, memoryview)) else g)
        df["geometry"] = df["geometry"].apply(from_wkb)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=f"EPSG:{EPSGCode.WGS84.value}")

        return {
            str(region): [
                gpd.GeoDataFrame(hash_partition, geometry="geometry", crs=gdf.crs)
                for _, hash_partition in region_partition.groupby("partition_key")
            ] for region, region_partition in gdf.groupby("region")
        }
