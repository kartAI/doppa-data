import pandas as pd
from duckdb import DuckDBPyConnection

from src.application.common import logger
from src.application.contracts import IConflationService, IFilePathService
from src.domain.enums import Theme


class ConflationService(IConflationService):
    __db_context: DuckDBPyConnection
    __file_path_service: IFilePathService

    def __init__(self, db_context: DuckDBPyConnection, file_path_service: IFilePathService):
        self.__db_context = db_context
        self.__file_path_service = file_path_service

    def find_fkb_osm_diff(self, release: str, theme: Theme) -> pd.DataFrame:
        logger.info(f"Finding diff between the raw OSM- and FKB-datasets.")

        osm_release = f"az://raw/release/{release}/dataset=osm/theme={theme.value}/region=*/*.parquet"
        fkb_release = f"az://raw/release/{release}/dataset=fkb/theme={theme.value}/region=*/*.parquet"

        df = self.__db_context.execute(
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
                    ) AS DECIMAl) AS max_iou
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
                    ) AS DECIMAl) AS max_iou
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

        osm_only_count = df[df["fkb_id"].isna()].shape[0]
        fkb_only_count = df[df["osm_id"].isna()].shape[0]
        overlapping_count = df[df["fkb_id"].notna() & df["osm_id"].notna()].shape[0]

        logger.info(
            f"Conflation step completed. Result: {osm_only_count} OSM-only buildings, {fkb_only_count} FKB-only buildings, and {overlapping_count} overlapping buildings."
        )

        return df
