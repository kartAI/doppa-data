import geopandas as gpd
import pandas as pd
from duckdb import DuckDBPyConnection
from shapely import from_wkb

from src.application.common import logger
from src.application.contracts import IConflationService, IFilePathService, IBlobStorageService
from src.domain.enums import Theme, StorageContainer, DataSource, EPSGCode


class ConflationService(IConflationService):
    __db_context: DuckDBPyConnection
    __file_path_service: IFilePathService
    __blob_storage_service: IBlobStorageService

    def __init__(
            self,
            db_context: DuckDBPyConnection,
            file_path_service: IFilePathService,
            blob_storage_service: IBlobStorageService
    ):
        self.__db_context = db_context
        self.__file_path_service = file_path_service
        self.__blob_storage_service = blob_storage_service

    def get_fkb_osm_id_relations(self, release: str, theme: Theme, region: str) -> pd.DataFrame:
        logger.info(f"Finding diff between the raw OSM- and FKB-datasets for region '{region}'.")

        osm_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            file_name="*.parquet",
            dataset=DataSource.OSM.value
        )

        fkb_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            file_name="*.parquet",
            dataset=DataSource.FKB.value
        )

        osm_path_base = self.__file_path_service.remove_blob_file_name_from_path(
            file_path=osm_release,
            file_name="*.parquet",
            prefix="az://raw/"
        )

        fkb_path_base = self.__file_path_service.remove_blob_file_name_from_path(
            file_path=fkb_release,
            file_name="*.parquet",
            prefix="az://raw/"
        )

        has_osm_files = self.__blob_storage_service.has_files_under_blob_path_base(
            container=StorageContainer.RAW,
            path=osm_path_base
        )

        has_fkb_files = self.__blob_storage_service.has_files_under_blob_path_base(
            container=StorageContainer.RAW,
            path=fkb_path_base
        )

        osm_count = self.__db_context.execute(
            f"SELECT COUNT(*) AS count FROM '{osm_release}'"
        ).fetchone()[0] if has_osm_files else 0

        fkb_count = self.__db_context.execute(
            f"SELECT COUNT(*) AS count FROM '{fkb_release}'"
        ).fetchone()[0] if has_fkb_files else 0

        logger.info(f"Total number of buildings: {osm_count} in OSM and {fkb_count} FKB")

        osm_cte, fkb_cte = ConflationService.__create_cte(
            has_osm_files=has_osm_files,
            has_fkb_files=has_fkb_files,
            osm_release=osm_release,
            fkb_release=fkb_release
        )

        query = f'''
            WITH {fkb_cte}, {osm_cte},

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
                UNION ALL
                SELECT
                    fkb_id,
                    osm_id,
                FROM osm_only
                UNION ALL
                SELECT
                    fkb_id,
                    osm_id,
                FROM fkb_osm_overlap
                WHERE iou > 0.70
            )

            SELECT * FROM merged
            '''

        ids_df = self.__db_context.execute(query).fetchdf()

        osm_only_count = ids_df[ids_df["fkb_id"].isna()].shape[0]
        fkb_only_count = ids_df[ids_df["osm_id"].isna()].shape[0]
        overlapping_count = ids_df[ids_df["fkb_id"].notna() & ids_df["osm_id"].notna()].shape[0]

        logger.info(
            f"Conflation step for region '{region}' completed. Result: {osm_only_count} OSM-only buildings, {fkb_only_count} FKB-only buildings, and {overlapping_count} overlapping buildings."
        )

        return ids_df

    def merge_fkb_osm(
            self,
            release: str,
            region: str,
            theme: Theme,
            ids: pd.DataFrame
    ) -> list[gpd.GeoDataFrame]:
        logger.info(f"Merging OSM- and FKB-datasets for region '{region}'")
        osm_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            file_name="*.parquet",
            dataset=DataSource.OSM.value
        )

        fkb_release = self.__file_path_service.create_virtual_filesystem_path(
            storage_scheme="az",
            container=StorageContainer.RAW,
            release=release,
            theme=Theme.BUILDINGS,
            region=region,
            file_name="*.parquet",
            dataset=DataSource.FKB.value
        )

        ids_fkb = ids[ids["fkb_id"].notna()]["fkb_id"].to_list()
        ids_osm = ids[ids["fkb_id"].isna() & ids["osm_id"].notna()]["osm_id"].to_list()

        ids_fkb_str = ", ".join(f"'{v}'" for v in ids_fkb)
        ids_osm_str = ", ".join(f"'{v}'" for v in ids_osm)

        fkb_filter = "FALSE" if not ids_fkb else f"external_id IN ({ids_fkb_str})"
        osm_filter = "FALSE" if not ids_osm else f"external_id IN ({ids_osm_str})"

        osm_path_base = self.__file_path_service.remove_blob_file_name_from_path(
            file_path=osm_release,
            file_name="*.parquet",
            prefix="az://raw/"
        )

        fkb_path_base = self.__file_path_service.remove_blob_file_name_from_path(
            file_path=fkb_release,
            file_name="*.parquet",
            prefix="az://raw/"
        )

        has_osm_files = self.__blob_storage_service.has_files_under_blob_path_base(
            container=StorageContainer.RAW,
            path=osm_path_base
        )

        has_fkb_files = self.__blob_storage_service.has_files_under_blob_path_base(
            container=StorageContainer.RAW,
            path=fkb_path_base
        )

        osm_cte, fkb_cte = ConflationService.__create_cte_2(
            has_osm_files=has_osm_files,
            osm_release=osm_release,
            osm_filter=osm_filter,
            has_fkb_files=has_fkb_files,
            fkb_release=fkb_release,
            fkb_filter=fkb_filter
        )

        query = f'''
                WITH {fkb_cte}, {osm_cte},
                
                conflated AS (
                    SELECT * FROM fkb
                    UNION ALL
                    SELECT * FROM osm
                )
                
                SELECT * FROM conflated
                '''

        df = self.__db_context.execute(query).fetchdf()

        df["geometry"] = df["geometry"].apply(lambda g: bytes(g) if isinstance(g, (bytearray, memoryview)) else g)
        df["geometry"] = df["geometry"].apply(from_wkb)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=f"EPSG:{EPSGCode.WGS84.value}")

        merged_count = gdf.shape[0]
        logger.info(f"Merged dataset for region '{region}' contains {merged_count} entries")

        return [
            gpd.GeoDataFrame(hash_partition, geometry="geometry", crs=gdf.crs)
            for _, hash_partition in gdf.groupby("partition_key")
        ]

    @staticmethod
    def __create_cte(has_osm_files: bool, has_fkb_files: bool, osm_release: str, fkb_release: str) -> tuple[str, str]:
        if has_osm_files:
            osm_cte = f"""osm AS 
            (
                SELECT
                    external_id AS osm_id,
                    TRY_CAST(building_id AS INTEGER) AS building_id,
                    CAST(FLOOR(ST_X(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_x,
                    CAST(FLOOR(ST_Y(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_y,
                    ST_Force2D(geometry) AS geom
                FROM read_parquet('{osm_release}', union_by_name = true)
            )
            """
        else:
            osm_cte = """osm AS 
            (
                SELECT
                    CAST(NULL AS INTEGER) AS osm_id,
                    CAST(NULL AS INTEGER) AS building_id,
                    CAST(NULL AS INTEGER) AS grid_x,
                    CAST(NULL AS INTEGER) AS grid_y,
                    CAST(NULL AS GEOMETRY) AS geom
                WHERE FALSE
            )
            """

        if has_fkb_files:
            fkb_cte = f"""fkb AS 
            (
                SELECT
                    external_id AS fkb_id,
                    TRY_CAST(building_id AS INTEGER) AS building_id,
                    CAST(FLOOR(ST_X(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_x,
                    CAST(FLOOR(ST_Y(ST_Centroid(geometry)) * 100) AS INTEGER) AS grid_y,
                    ST_Force2D(geometry) AS geom
                FROM read_parquet('{fkb_release}', union_by_name = true)
            )
            """
        else:
            fkb_cte = """fkb AS 
            (
                SELECT
                    CAST(NULL AS VARCHAR) AS fkb_id,
                    CAST(NULL AS INTEGER) AS building_id,
                    CAST(NULL AS INTEGER) AS grid_x,
                    CAST(NULL AS INTEGER) AS grid_y,
                    CAST(NULL AS GEOMETRY) AS geom
                WHERE FALSE
            )
            """

        return osm_cte, fkb_cte

    @staticmethod
    def __create_cte_2(
            has_osm_files: bool,
            has_fkb_files: bool,
            osm_release: str,
            fkb_release: str,
            osm_filter,
            fkb_filter
    ) -> tuple[str, str]:
        if has_osm_files:
            osm_cte = f"""osm AS 
            (
                SELECT 
                    external_id,
                    ST_AsWKB(geometry) as geometry,
                    bbox,
                    region,
                    partition_key,
                    building_type,
                    TRY_CAST(building_id AS INTEGER) AS building_id,
                    feature_update_time,
                    feature_capture_time,
                    'osm' AS source
                FROM read_parquet('{osm_release}', union_by_name = true)
                WHERE {osm_filter}
            )
            """
        else:
            osm_cte = """
            osm AS (
                SELECT
                    CAST(NULL AS INTEGER) AS external_id,
                    CAST(NULL AS BLOB) AS geometry,
                    CAST(NULL AS STRUCT(minx DOUBLE, miny DOUBLE, maxx DOUBLE, maxy DOUBLE)) AS bbox,
                    CAST(NULL AS VARCHAR) AS region,
                    CAST(NULL AS VARCHAR) AS partition_key,
                    CAST(NULL AS VARCHAR) AS building_type,
                    CAST(NULL AS INTEGER) AS building_id,
                    CAST(NULL AS TIMESTAMP) AS feature_update_time,
                    CAST(NULL AS TIMESTAMP) AS feature_capture_time,
                    'osm' AS source
                WHERE FALSE
            )
            """

        if has_fkb_files:
            fkb_cte = f"""fkb AS 
            (
                SELECT 
                    external_id,
                    ST_AsWKB(geometry) AS geometry,
                    bbox,
                    region,
                    partition_key,
                    TRY_CAST(building_type AS VARCHAR) AS building_type,
                    TRY_CAST(building_id AS INTEGER) AS building_id,
                    NULL AS feature_update_time,
                    NULL AS feature_capture_time,
                    'fkb' AS source
                FROM read_parquet('{fkb_release}', union_by_name = true)
                WHERE {fkb_filter}
            )
            """
        else:
            fkb_cte = """
            fkb AS (
                SELECT
                    CAST(NULL AS VARCHAR) AS external_id,
                    CAST(NULL AS BLOB) AS geometry,
                    CAST(NULL AS STRUCT(minx DOUBLE, miny DOUBLE, maxx DOUBLE, maxy DOUBLE)) AS bbox,
                    CAST(NULL AS VARCHAR) AS region,
                    CAST(NULL AS VARCHAR) AS partition_key,
                    CAST(NULL AS VARCHAR) AS building_type,
                    CAST(NULL AS INTEGER) AS building_id,
                    CAST(NULL AS TIMESTAMP) AS feature_update_time,
                    CAST(NULL AS TIMESTAMP) AS feature_capture_time,
                    'fkb' AS source
                WHERE FALSE
            )
            """

        return osm_cte, fkb_cte
