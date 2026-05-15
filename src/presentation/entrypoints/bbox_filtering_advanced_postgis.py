from dependency_injector.wiring import Provide, inject
from sqlalchemy import Engine, text

from src.application.common.monitor import monitor
from src.application.dtos import CostConfiguration
from src.domain.enums import BenchmarkIteration
from src.infra.infrastructure import Containers


@inject
@monitor(
    query_id="bbox-filtering-advanced-postgis",
    benchmark_iteration=BenchmarkIteration.BBOX_FILTERING_ADVANCED,
    cost_configuration=CostConfiguration(include_aci=True, include_postgres=True)
)
def bbox_filtering_advanced_postgis(
        db_context: Engine = Provide[Containers.postgres_context],
) -> list:
    # Same bbox as in the DuckDB example (Oslo-ish, WGS84 lon/lat)
    min_lon = 10.40
    max_lon = 10.95
    min_lat = 59.70
    max_lat = 60.10

    sql = text(
        """
        WITH src AS (SELECT *, geometry AS geom_4326 FROM buildings_small),
             bbox AS (
                 -- Axis-aligned bbox in EPSG:4326 (lon/lat)
                 SELECT ST_MakeEnvelope(
                                :min_lon,
                                :min_lat,
                                :max_lon,
                                :max_lat,
                                4326
                        ) AS bbox_4326),
             filtered AS (
                 -- Subselect so we can reference geom_25833 in the outer WHERE
                 SELECT *
                 FROM (SELECT s.*,
                              ST_Transform(s.geom_4326, 25833) AS geom_25833
                       FROM src s
                                CROSS JOIN bbox b
                       WHERE ST_Intersects(s.geom_4326, b.bbox_4326)
                         AND ST_IsValid(s.geom_4326)) t
                 WHERE
                     -- Advanced: realistic building area in EPSG:25833 (ETRS89 / UTM 33N)
                     ST_Area(geom_25833) BETWEEN 50 AND 5000)
        SELECT COUNT(*)                      AS building_count,
               AVG(ST_Area(geom_25833))      AS avg_area_m2,
               MIN(ST_Perimeter(geom_25833)) AS min_perimeter_m,
               MAX(ST_Perimeter(geom_25833)) AS max_perimeter_m
        FROM filtered;
        """
    )

    with db_context.connect() as conn:
        return conn.execute(
            sql,
            {
                "min_lon": min_lon,
                "min_lat": min_lat,
                "max_lon": max_lon,
                "max_lat": max_lat,
            },
        ).fetchall()
