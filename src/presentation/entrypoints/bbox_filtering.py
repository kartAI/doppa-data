from dependency_injector.wiring import Provide, inject
from duckdb import DuckDBPyConnection

from src.application.common.monitor import monitor_cpu_and_ram
from src.application.contracts import IFilePathService
from src.domain.enums import StorageContainer, Theme
from src.infra.infrastructure import Containers


@inject
@monitor_cpu_and_ram(query_id="duckdb-bbox-filtering")
def duckdb_bbox_filtering(
        db_context: DuckDBPyConnection = Provide[Containers.duckdb_context],
        path_service: IFilePathService = Provide[Containers.file_path_service],
) -> None:
    path = path_service.create_release_virtual_filesystem_path(
        storage_scheme="az",
        release="2026-02-16.3",
        container=StorageContainer.DATA,
        theme=Theme.BUILDINGS,
        region="*",
        file_name="*.parquet",
    )

    # Example bbox in Norway (Oslo-ish, WGS84 lon/lat)
    min_lon = 10.40
    max_lon = 10.95
    min_lat = 59.70
    max_lat = 60.10

    query = f"""
        WITH src AS (
            SELECT
                *,
                -- geometry is already GEOMETRY; assume EPSG:4326 (lon/lat)
                geometry AS geom_4326
            FROM read_parquet('{path}')
        ),
        bbox AS (
            -- Axis-aligned bbox in 4326 coordinates (lon/lat)
            SELECT ST_MakeEnvelope(?, ?, ?, ?) AS bbox_4326
        ),
        filtered AS (
            SELECT
                s.*,
                -- Precompute projected geometry once for advanced metrics
                ST_Transform(
                    s.geom_4326,
                    'EPSG:4326',
                    'EPSG:25833'
                ) AS geom_25833
            FROM src s
            CROSS JOIN bbox b
            WHERE
                -- Only buildings intersecting the bbox
                ST_Intersects(s.geom_4326, b.bbox_4326)
                -- Valid geometries only
                AND ST_IsValid(s.geom_4326)
                -- Advanced: filter by realistic building area in EPSG:25833 (ETRS89 / UTM 33N)
                AND ST_Area(geom_25833) BETWEEN 50 AND 5000
        )
        SELECT
            COUNT(*) AS building_count,
            AVG(ST_Area(geom_25833)) AS avg_area_m2,
            MIN(ST_Perimeter(geom_25833)) AS min_perimeter_m,
            MAX(ST_Perimeter(geom_25833)) AS max_perimeter_m
        FROM filtered;
    """

    db_context.execute(
        query,
        [min_lon, min_lat, max_lon, max_lat],
    )
